# Cluster Mode

## Goal

Run multiple Meilisearch nodes as a single logical cluster with automatic leader election
and task replication. Writes go to the leader (or are forwarded there), get replicated
through Raft consensus, and applied to all nodes. Reads can hit any node for horizontal
read scaling.

This is additive — standalone Meilisearch is unchanged. Cluster mode activates only when
`--cluster-create` or `--cluster-join` flags are used, behind the `cluster` Cargo feature.

## Usage

### Create a cluster (first node)

```bash
meilisearch --cluster-create \
  --master-key "your-master-key" \
  --cluster-bind 0.0.0.0:7700 \
  --cluster-node-id 0
```

This prints the cluster key (derived from the master key) and a join command. The first
node becomes the initial Raft leader. When `--master-key` is set, the cluster secret is
deterministically derived — nodes sharing the same master key auto-authenticate.

### Join an existing cluster

```bash
# With same master key (recommended — no cluster-secret needed):
meilisearch --cluster-join <leader-host>:7700 \
  --master-key "your-master-key" \
  --cluster-node-id 1 \
  --cluster-bind 0.0.0.0:7701

# Or with explicit cluster secret:
meilisearch --cluster-join <leader-host>:7700 \
  --cluster-secret <cluster-key> \
  --cluster-node-id 1 \
  --cluster-bind 0.0.0.0:7701
```

The joining node contacts the leader, gets added to Raft membership, and begins receiving
replicated entries. It starts as a follower.

### Manual mode (no Raft)

Phase 2's manual leader/follower mode still works without the `cluster` feature:

```bash
# Leader
meilisearch --cluster-role leader --node-id node-1

# Follower (forwards writes to leader)
meilisearch --cluster-role follower --node-id node-2 \
  --cluster-peers http://leader:7700
```

## Architecture

### Phases

The cluster implementation was built in phases:

1. **Barrier** — `X-Meili-Barrier` header for read-after-write consistency. A client that
   writes to the leader can pass the returned `taskUid` as a barrier to a follower read,
   which waits until that task is locally visible.

2. **Write Forwarding** — Followers detect write requests and transparently forward them
   to the leader via HTTP, returning the leader's response to the client.

3. **Raft Integration** — Automatic leader election and task replication via openraft.
   This is the core cluster functionality.

### Docker / Kubernetes

In container environments, nodes typically bind to `0.0.0.0` but need to advertise a
routable hostname. Use `--cluster-addr` to set the advertise address:

```bash
meilisearch --cluster-create \
  --cluster-bind 0.0.0.0:7701 \
  --cluster-addr node1 \
  --master-key "your-master-key"
```

The `--cluster-addr` value is combined with the QUIC and HTTP ports to form the addresses
other nodes use to connect. If `--cluster-bind` is `0.0.0.0` and `--cluster-addr` is not
set, the node will refuse to start with a clear error message.

### Transport

**QUIC** (via quinn) + **HMAC-SHA256** per-message signing, with optional **TLS encryption**.

By default, the transport uses quinn-plaintext (QUIC without TLS) — HMAC provides integrity
and authentication on every message. For deployments on untrusted networks, pass
`--cluster-tls` to enable encrypted QUIC transport. TLS mode derives a deterministic
self-signed Ed25519 certificate from the cluster secret — zero configuration, no PKI needed.
All nodes must use the same `--cluster-tls` setting. HMAC signing remains active in both
modes as defense-in-depth.

See [cluster-performance.md](cluster-performance.md) for data transfer efficiency analysis
and [cluster-sharding-assessment.md](cluster-sharding-assessment.md) for future sharding
compatibility.

Each peer connection opens 3 bidirectional QUIC streams (channels):

| Channel | Purpose |
|---------|---------|
| `raft` | openraft RPCs (vote, append-entries, install-snapshot) |
| `dml` | Out-of-band file transfers (document upload files sent to followers before Raft entry) |
| `snapshot` | Full database transfer for bootstrapping new nodes |

The `raft` and `dml` channels carry active traffic. The `snapshot` channel transfers a
point-in-time database snapshot to new nodes joining a cluster with existing data.

### Serialization

**bincode** throughout. openraft types derive `Serialize`/`Deserialize`, and bincode is
fast binary with minimal overhead. Used for both the raft channel (RPCs) and the dml
channel (file transfer headers/ACKs).

### Consensus

**openraft 0.9** with **LMDB-backed** log and state machine storage (`LmdbRaftStore`).

The Raft log, vote, and snapshot metadata are all persisted to an LMDB environment at
`{db_path}/cluster/`. This means a crashed node retains its Raft state and can restart
without re-joining the cluster — see `ClusterNode::restart()`.

The Raft log stores `RaftRequest` entries:
- `TaskEnqueued` — bincode-encoded `KindWithContent` (document/index operations)
- `ApiKeyPut` / `ApiKeyDelete` — API key CRUD replicated through Raft
- `SetRuntimeFeatures` — runtime feature toggles replicated cluster-wide
- `SetLogLevel` — log level changes replicated to all nodes
- `ClusterProtocolUpgrade` — protocol version bumps
- `Noop` — leader confirmation after election

When an entry is committed (replicated to a quorum), the state machine calls
`IndexScheduler::register_from_raft()` on each node, which inserts the task into
the local LMDB.

Task IDs are **not pre-assigned** by the leader. Each node auto-assigns from its own LMDB
via `queue.register(None)`. Because Raft guarantees all nodes apply entries in the same
deterministic order from the same starting state, they all assign the same IDs.

### Wiring

The cluster integration uses trait-based dependency injection to avoid circular
dependencies:

```
ClusterNode ──→ LmdbRaftStore ──→ TaskApplier (trait)
     ↑               │           → AuthApplier (trait)
     │               │           → FeatureApplier (trait)
     │               │           → LogLevelApplier (trait)
     │               ↓
TaskProposer (trait) ←── IndexScheduler ──┘
KeyProposer  (trait) ←── AuthController ──┘
```

All applier/proposer traits use `Arc<OnceLock<Arc<dyn Trait>>>` for post-construction
wiring — the objects are created first, then connected.

**Leader watcher**: A background task monitors openraft's `watch::Receiver<RaftMetrics>`
for leadership changes and updates the `is_leader` AtomicBool + signals `wake_up` to
start or stop the scheduler's batch processing loop.

## Guiding Principles

### Fail fast, never diverge silently

A committed Raft entry that fails to apply on a node **panics the process** rather than
logging and continuing. A crashed node can replay the Raft log on restart. A diverged
node — where one node has a task and another doesn't — is irrecoverable without manual
intervention.

This applies everywhere in the state machine's `apply()` path:
- `apply_task()` failure → panic
- Buffered entry replay failure → panic
- Mutex operations use poison-safe locking to avoid cascading panics

Errors that are NOT fatal (network issues, peer disconnects, transient failures in the
accept loop) are logged and retried.

### Standalone must never break

The `cluster` feature is additive. When compiled without it (or when no `--cluster-*`
flags are used), every code path must behave identically to upstream Meilisearch:
- `task_proposer` is `None` → `register()` writes directly to LMDB
- `is_leader` defaults to `true` → scheduler processes batches normally
- No new dependencies are pulled in

### Leader-only batch processing

Only the Raft leader runs the scheduler's `tick()` loop. Followers wait on `wake_up`.
This prevents duplicate batch processing. When leadership changes (detected by the
metrics watcher), the new leader's scheduler wakes up and the old leader's stops.

### No pre-assigned IDs

Task IDs are auto-assigned by each node from its own LMDB. Pre-assignment (via counters
or reads) creates race conditions under concurrent writes. Deterministic Raft log ordering
makes auto-assignment safe — all nodes see the same sequence.

## Conscious Tradeoffs

### LMDB-backed Raft storage

The Raft log, vote, and state machine are persisted to LMDB at `{db_path}/cluster/`.
A crashed node restarts from its persisted state without re-joining the cluster. The
`last_applied_log_id` is tracked in LMDB alongside the state, preventing duplicate
application on restart.

### Content file replication via DML

Document upload files (content files) are replicated to followers via the DML channel
**before** the corresponding Raft entry is proposed. This ensures followers have the
file on disk when the Raft entry arrives for processing. Files are transferred in
parallel to all followers with retry and exponential backoff. Followers that
consistently fail file transfers are evicted from the cluster.

### Follower writes use 307 redirects

Followers detect write requests and return a `307 Temporary Redirect` to the leader's
HTTP address. The client follows the redirect and sends the request directly to the
leader.

**Why**: Eliminates proxy overhead entirely. The follower doesn't buffer, parse, or
forward the request body — the client talks directly to the leader. This also means
the client sees the leader's response unmodified (correct `Location` headers, task IDs,
etc.).

**Consequence**: Clients must support HTTP redirects (all standard HTTP clients do).
Write latency from a follower is one extra round-trip for the redirect, then the same
as writing to the leader directly. Use `/cluster/health/writer` in your load balancer
to route write-heavy clients directly to the leader.

### Transport encryption options

By default, inter-node traffic uses HMAC-SHA256 signing (integrity + authentication) but
no encryption. This is the fastest option for trusted LAN deployments.

**Option 1: `--cluster-tls` (application-level encryption)**

Pass `--cluster-tls` on all nodes to enable QUIC TLS encryption. A deterministic
self-signed Ed25519 certificate is derived from the cluster secret — no CA, no cert
distribution, no expiry management. The custom certificate verifier accepts only peers
with the same derived cert, which proves they possess the cluster secret. HMAC remains
active for defense-in-depth. CPU overhead is ~1-5% on hardware with AES-NI.

**Option 2: WireGuard (network-level encryption)**

For WAN / multi-datacenter deployments, WireGuard provides:
- Kernel-level encryption with minimal overhead (~3% throughput impact)
- Simple key management (one static key per node, no expiry)
- Transparent to the application — QUIC traffic flows unchanged over the tunnel
- Selective encryption: only cross-datacenter links pay the cost

Both options can be combined (TLS + WireGuard) for defense-in-depth, though either
alone is sufficient for most deployments.

## Limitations

- **Full replication only** — every node stores all data. No sharding. Dataset must fit
  on each node.
- **Write scaling is vertical** — all writes go through the single Raft leader. For
  write-heavy workloads, use the fastest machine as the likely leader.
- **Follower writes redirect** — followers return 307 to the leader. Clients must
  support HTTP redirects. Use `/cluster/health/writer` in your load balancer to route
  writes directly to the leader and avoid the redirect round-trip.

## Consistency Testing

### What we test today

- **Leader election and failover**: Python integration tests verify that when the leader
  is killed, a new leader is elected and the cluster continues operating (write + read).
- **Write replication**: 36 integration tests verify documents, settings, API keys, and
  runtime features are replicated to all nodes.
- **Read-after-write via barrier**: The `X-Meili-Barrier` header test verifies a follower
  waits until the specified task is locally applied before responding.
- **Batch checksum divergence detection**: Unit tests verify that batch checksums are
  compared across nodes and mismatches are flagged.
- **Dead node eviction**: Integration test verifies a crashed node is eventually evicted
  from cluster membership.
- **Committed write survives leader crash**: Test verifies that documents committed via
  Raft quorum survive leader failure and are present on surviving nodes after re-election.
- **Concurrent writes to same index**: 10 concurrent writers to the same index across
  all nodes, verifying all documents are applied and replicated.
- **Leader crash mid-batch**: Leader is killed during batch processing; verifies the new
  leader completes work and surviving nodes agree on document count.

- **Network partition (simulated)**: Tests verify that killing minority nodes leaves the
  majority operational, and that restarted nodes catch up. Tested with 3-node (kill 1)
  and 5-node (kill 2) clusters, including leader isolation with re-election.
- **Network partition (fault injection)**: Application-level fault injection blocks
  peer communication while all nodes remain running. Tests verify follower isolation
  (majority writes, follower catches up after heal) and leader isolation (new election,
  writes via new leader, old leader catches up). Uses built-in `/cluster/test/block-peer`
  endpoints (gated behind `--cluster-enable-test-endpoints`) — no external tools required.

### What we don't test (and why it matters)

- **True network partition via isolation** — Partition behavior is tested via node
  kill/restart and via application-level fault injection (blocked peers). The fault
  injection tests verify true partition behavior where the minority is alive but
  unreachable. External tools like Toxiproxy are not needed since the transport is
  QUIC (UDP) and fault injection operates at the application layer.
- **High-contention concurrent writes** — Basic concurrent writes are tested (10 writers),
  but not stress-tested with hundreds of concurrent writers or sustained high throughput.
- **Leader crash before Raft commit** — We test leader crash after commit. The case where
  the leader crashes before committing (client gets no response, must retry) is untested.

### Recommended next steps (not in this PR)

- **Phase 1: Extended fault injection** — The built-in fault injection endpoints
  (`/cluster/test/block-peer`, `/cluster/test/unblock-peer`) enable partition testing
  without external tools. Next steps: add latency injection, partial message loss, and
  long-duration partition tests with sustained writes.
- **Phase 2: DIY Jepsen harness (4–6 weeks)** — If Phase 1 reveals issues, build a
  custom consistency checker that records write history and verifies linearizability of
  committed operations.
- **Phase 3: Antithesis / deterministic simulation** — For ongoing regression testing,
  consider deterministic simulation tools that can explore rare interleavings.

### Key invariants to verify

- Committed writes are never lost (Raft quorum guarantee)
- No split-brain (Raft single-leader guarantee)
- Eventual convergence after partition heals
- Search index matches committed document state

## Frequently Asked Questions

This section addresses concerns from community discussions about Meilisearch clustering:
[Issue #1383](https://github.com/meilisearch/meilisearch/issues/1383),
[Discussion #617](https://github.com/orgs/meilisearch/discussions/617), and
[PR #3593](https://github.com/meilisearch/meilisearch/pull/3593) (prior prototype).

### Architecture

**Why single-leader, not multi-leader?**

Raft consensus gives strong consistency with a single leader. Multi-leader architectures
add conflict resolution complexity (last-write-wins? CRDTs? manual merge?) that search
engines don't need — there's no meaningful concurrent-write-to-same-document scenario
where you'd want both writes to survive. All reads scale horizontally since every node
has a full copy; writes go through one leader for ordering.

**Why Raft (openraft) instead of the custom sync from PR #3593?**

PR #3593 implemented a custom leader-follower sync protocol with configurable consistency
levels (ONE/QUORUM/ALL). It had several issues: TCP keepalive bugs caused silent
disconnects, follower divergence behavior was undefined (writes could be accepted on
followers without propagating back), and the entire index had to be serialized in-memory
for initial sync. Raft provides proven consensus with well-understood failure modes,
automatic leader election, and a large body of literature on correctness.

**Is there automatic leader election?**

Yes. Unlike the PR #3593 prototype (which had a pre-selected leader with no failover),
this branch uses Raft election. If the leader dies, a new leader is elected within
1.5–3 seconds (configurable via election timeout). No manual intervention required.

### Data Consistency

**What consistency model?**

Writes are linearizable — a write is acknowledged only after the Raft log entry is
committed to a quorum of nodes. Reads are eventually consistent by default; followers
apply committed entries asynchronously, so a read immediately after a write may hit a
follower that hasn't applied it yet. Use the `X-Meili-Barrier: <taskUid>` header to get
read-after-write consistency: the follower will wait until the specified task is locally
applied before responding.

**Can writes be lost?**

No, once acknowledged. A write returns success to the client only after Raft commits it
to a quorum (majority of nodes). If the leader crashes after commit, the write survives
on the majority and the new leader will have it. If the leader crashes *before* commit,
the write was never acknowledged to the client, so it's the client's responsibility to
retry.

**What about concurrent writes to the same index?**

Raft serializes all writes through the leader. Concurrent writes from different clients
are ordered deterministically in the Raft log. There is no conflict — the second write
simply applies after the first. This is not yet stress-tested under heavy contention
(see Consistency Testing above).

### Scaling

**How does read scaling work?**

All nodes maintain a full copy of all data (documents, indexes, settings, API keys).
Any node can serve search queries with identical results. Put a load balancer (Caddy,
HAProxy, nginx) in front and use `/cluster/health/reader` for health checks. Reads
are local — no inter-node communication required.

**Write scaling?**

Not horizontally scalable. All writes go through the single Raft leader. For write-heavy
workloads, use the fastest machine as the likely leader. Horizontal write scaling would
require sharding, which is not in scope for this branch.

**How many nodes are supported?**

Tested with 1–5 nodes. Raft works best with 3 or 5 nodes (odd numbers for clean quorum
majorities). More nodes increase read capacity but add replication overhead for writes.
There is no hard limit, but beyond 7 voters the Raft replication latency becomes
noticeable.

### Operations

**Rolling upgrades?**

Supported. Two operational patterns:

**Restart-based (bare metal / VM):**
Best for physical servers, VMs — any deployment where the binary is replaced in place
on disk.
1. Stop a follower node
2. Replace the binary
3. Restart with the same `--db-path` and `--experimental-dumpless-upgrade`
4. The node resumes from persisted Raft state and catches up on missed log entries
5. Repeat for each follower, upgrading the leader last (triggers a brief re-election ~2s)

This is the fastest path — no snapshot transfer needed, just Raft log catch-up.

**Add-new/retire-old (containers / images):**
Best for Docker, Kubernetes — any image-based deployment where containers are immutable.
1. Start new nodes running the new image with `--cluster-join`
2. Verify they're healthy via `GET /cluster/status` (check `nodeVersions`)
3. Gracefully leave or stop the old nodes one by one

This requires snapshot transfer per new node but is natural for immutable infrastructure.

**Version compatibility rules:**
- Major version must match
- Minor versions may differ by at most 1 (allows an upgrade window)
- Patch versions may differ freely

**Protocol auto-upgrade:**
Version info is exchanged automatically via the RPC peer handshake. Each node reports
its binary version and supported protocol versions. When all nodes in the cluster
support a higher protocol version, the leader automatically proposes a
`ClusterProtocolUpgrade` through Raft consensus. Monitor progress via
`GET /cluster/status` → `nodeVersions`.

**Recommended order:** followers first (one at a time, verify health), leader last.

**Recovery from incompatible upgrades:**
If the Raft log format changes between versions (new `RaftRequest` enum variants), the
node detects this on startup and prints a clear error. Use `--cluster-reset` to wipe
the persisted cluster state, then re-create or re-join the cluster.

**Endpoints for monitoring:**
- `GET /cluster/version-info` — unauthenticated, returns this node's binary version
  and supported protocols
- `GET /cluster/status` → `nodeVersions` — shows per-node version info for all
  known cluster members

**Monitoring?**

Prometheus metrics are exposed for: leader status, replication lag, peer connection
state, eviction events, and snapshot transfer progress. See
[cluster-operations.md](cluster-operations.md) for details.

**Backup?**

Same as standalone Meilisearch: snapshot or copy the `--db-path` directory. Since every
node has a full copy of all data, any node's data directory is a valid backup.

### Failure Modes

**Split-brain?**

Prevented by Raft. A leader requires votes from a majority of nodes. In a network
partition, only the partition containing a majority can elect a leader. The minority
side has no leader — it serves stale reads from local data but rejects writes with 503.
When the partition heals, the minority catches up from the Raft log.

**Network partition?**

The majority side continues normal operation (reads and writes). The minority side
cannot write (no leader) and serves stale reads. When the partition heals, minority
nodes receive the missed Raft log entries and converge. No data is lost. This behavior
is tested via application-level fault injection (see Consistency Testing above).

**Follower divergence?**

Unlike PR #3593 (which had "undefined behavior" when followers accepted writes
independently), divergence is impossible under Raft. All nodes apply the same log
entries in the same order. Additionally, batch checksums are compared across nodes for
defense-in-depth — if a checksum mismatch is detected, it indicates a bug and the node
panics rather than silently diverging.

### Security

**Inter-node authentication?**

HMAC-SHA256 on every message. The cluster secret is deterministically derived from
`--master-key` using HKDF (or set explicitly via `--cluster-secret`). Every QUIC
message includes an HMAC tag — unauthenticated messages are rejected. A node without
the correct key cannot join the cluster or inject messages.

**Encryption?**

Two options, depending on your deployment:

- **`--cluster-tls`**: Enables QUIC TLS encryption using a deterministic self-signed
  certificate derived from the cluster secret. Zero configuration — no CA, no cert
  distribution. All nodes must use the same setting. Best for untrusted networks.
- **WireGuard**: Network-level encryption for WAN/multi-datacenter deployments.
  Selective — only cross-datacenter links pay the cost.

Without `--cluster-tls`, traffic is signed (HMAC-SHA256) but not encrypted. This is
the fastest option for trusted LANs. See "Transport encryption options" in Conscious
Tradeoffs above.

### Performance

**Write latency impact?**

Writes to the leader: approximately the same as standalone. Raft commit adds a few
milliseconds for quorum replication (network round-trip to followers). Writes to a
follower: the follower returns a 307 redirect to the leader's HTTP address — the client
follows the redirect and writes directly to the leader. No proxy overhead; the extra
cost is one redirect round-trip.

**Read latency?**

Identical to standalone. Reads are served from local data with no inter-node
communication. Every node has the full dataset.

### Prior Work

**How does this differ from PR #3593?**

This is a complete rewrite. PR #3593 was a draft prototype using a custom sync protocol.
Key differences:

| Aspect | PR #3593 (prototype) | This branch (Raft) |
|--------|---------------------|-------------------|
| Consensus | Custom ack-based sync | openraft (Raft) |
| Leader election | Pre-selected, no failover | Automatic Raft election |
| Consistency | Configurable (ONE/QUORUM/ALL) | Quorum commit (linearizable writes) |
| Transport | TCP + ChaCha20 | QUIC + HMAC-SHA256 (optional TLS) |
| Divergence handling | Undefined | Impossible (Raft log ordering) |
| Follower writes | Accepted (undefined behavior) | 307 redirect to leader |
| State transfer | Full dump from leader | Chunked snapshot with xxhash64 |
| Node restart | Rejoin required | Auto-restart from LMDB |

# Cluster Operations Guide

Operational guide for running Meilisearch in Raft cluster mode.

## Quick Start

### Create a new cluster

Start the first node as cluster creator. It bootstraps a single-node Raft cluster
and prints the cluster key to stderr:

```bash
# Bare metal / VM (bind to a specific IP):
meilisearch \
  --db-path ./data/node1 \
  --http-addr 0.0.0.0:7700 \
  --master-key "your-master-key" \
  --cluster-create \
  --cluster-bind 10.0.0.1:7701 \
  --cluster-node-id 1

# Docker / Kubernetes (bind 0.0.0.0, advertise a routable hostname):
meilisearch \
  --db-path ./data/node1 \
  --http-addr 0.0.0.0:7700 \
  --master-key "your-master-key" \
  --cluster-create \
  --cluster-bind 0.0.0.0:7701 \
  --cluster-addr node1 \
  --cluster-node-id 1
```

When `--cluster-bind` is `0.0.0.0` (wildcard), you **must** set `--cluster-addr` to the
hostname or IP that other nodes can reach. Without it, the node would advertise `0.0.0.0`
which is not routable and the cluster will fail to form.

When `--master-key` is set, the cluster secret is automatically derived from it.
Nodes sharing the same master key can authenticate to each other without managing
a separate cluster key.

### Join an existing cluster

Start additional nodes with `--cluster-join` pointing to the creator's QUIC address.
If all nodes share the same `--master-key`, no explicit `--cluster-secret` is needed:

```bash
meilisearch \
  --db-path ./data/node2 \
  --http-addr 0.0.0.0:7710 \
  --master-key "your-master-key" \
  --cluster-join 10.0.0.1:7701 \
  --cluster-bind 0.0.0.0:7702 \
  --cluster-node-id 2
```

Alternatively, you can use an explicit `--cluster-secret` (e.g., if you don't want
to derive from the master key):

```bash
meilisearch \
  --db-path ./data/node2 \
  --http-addr 0.0.0.0:7710 \
  --master-key "your-master-key" \
  --cluster-join 10.0.0.1:7701 \
  --cluster-secret "<cluster-key>" \
  --cluster-bind 0.0.0.0:7702 \
  --cluster-node-id 2
```

### Verify cluster health

```bash
curl -s http://localhost:7700/cluster/status \
  -H "Authorization: Bearer your-master-key" | jq
```

Response:

```json
{
  "role": "leader",
  "nodeId": "...",
  "lifecycle": "leader",
  "raftNodeId": 1,
  "raftLeaderId": 1,
  "leaderUrl": "http://10.0.0.1:7700",
  "peers": [],
  "voters": [1, 2, 3],
  "clusterProtocolVersion": 1
}
```

## Configuration Reference

### Core flags

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--cluster-create` | `MEILI_CLUSTER_CREATE` | false | Bootstrap a new cluster (first node only) |
| `--cluster-join <addr>` | `MEILI_CLUSTER_JOIN` | — | Join existing cluster at `host:port` (QUIC) |
| `--cluster-secret <key>` | `MEILI_CLUSTER_SECRET` | derived from master key | Cluster key (auto-derived from `--master-key` if not set) |
| `--cluster-bind <addr>` | `MEILI_CLUSTER_BIND` | `0.0.0.0:7701` | QUIC bind address for intra-cluster traffic |
| `--cluster-addr <host>` | `MEILI_CLUSTER_ADDR` | — | Advertise hostname/IP. Required when `--cluster-bind` is `0.0.0.0` (Docker/K8s) |
| `--cluster-node-id <id>` | `MEILI_CLUSTER_NODE_ID` | 0 | Unique numeric node ID |

### Timing and tuning

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--cluster-heartbeat-ms` | `MEILI_CLUSTER_HEARTBEAT_MS` | 500 | Raft heartbeat interval |
| `--cluster-election-timeout-min-ms` | `MEILI_CLUSTER_ELECTION_TIMEOUT_MIN_MS` | 1500 | Minimum election timeout |
| `--cluster-election-timeout-max-ms` | `MEILI_CLUSTER_ELECTION_TIMEOUT_MAX_MS` | 3000 | Maximum election timeout |
| `--cluster-accept-timeout-ms` | `MEILI_CLUSTER_ACCEPT_TIMEOUT_MS` | 10000 | Timeout for accepting peer QUIC streams |
| `--cluster-write-timeout-secs` | `MEILI_CLUSTER_WRITE_TIMEOUT_SECS` | 10 | Timeout for Raft write proposals |

### Resource limits

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--cluster-max-message-size-mb` | `MEILI_CLUSTER_MAX_MESSAGE_SIZE_MB` | 512 | Max DML message size (MB) |
| `--cluster-raft-db-size-mb` | `MEILI_CLUSTER_RAFT_DB_SIZE_MB` | 256 | Raft LMDB map size (MB) |
| `--cluster-max-transfer-failures` | `MEILI_CLUSTER_MAX_TRANSFER_FAILURES` | 3 | Consecutive file transfer failures before eviction |
| `--cluster-max-replication-lag` | `MEILI_CLUSTER_MAX_REPLICATION_LAG` | 10000 | Max log-entry lag before eviction (0 to disable) |

### Security and transport

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--cluster-tls` | `MEILI_CLUSTER_TLS` | false | Enable TLS encryption on QUIC transport (derives cert from cluster secret) |

### Utility flags

| Flag | Description |
|------|-------------|
| `--cluster-status-url <url>` | Query a node's status and exit (e.g., `http://localhost:7700`) |
| `--cluster-leave-url <url>` | Tell a node to gracefully leave and exit |
| `--cluster-show-secret` | Print the cluster secret (derived from `--master-key`) and exit |
| `--cluster-reset` | Wipe persisted cluster state and exit (use after incompatible upgrades) |

## Monitoring

### Prometheus metrics

All metrics have the `meilisearch_` prefix.

| Metric | Type | Description |
|--------|------|-------------|
| `meilisearch_cluster_is_leader` | Gauge | 1 if this node is the Raft leader |
| `meilisearch_cluster_current_term` | Gauge | Current Raft term |
| `meilisearch_cluster_last_applied_log` | Gauge | Index of last applied Raft log entry |
| `meilisearch_cluster_members_total` | Gauge | Total cluster members (voters + learners) |
| `meilisearch_cluster_failed_applies_total` | Gauge | Cumulative failed state machine applies |
| `meilisearch_cluster_file_transfer_failures_total` | Gauge | Cumulative file transfer failures |
| `meilisearch_cluster_nodes_evicted_total` | Gauge | Cumulative evicted nodes |
| `meilisearch_cluster_node_lifecycle` | Gauge | Current lifecycle state (numeric) |
| `meilisearch_cluster_snapshot_transfer_bytes` | Gauge | Bytes transferred during snapshot bootstrap |

### Lifecycle states

The `lifecycle` field in `/cluster/status` and the `node_lifecycle` metric report:

| Value | Numeric | Description |
|-------|---------|-------------|
| `bootstrapping` | 0 | Node is creating a new cluster |
| `joining` | 1 | Node is joining an existing cluster |
| `learner` | 2 | Joined but not yet promoted to voter |
| `follower` | 3 | Voter, following the leader |
| `leader` | 4 | Voter, elected leader |
| `evicted` | 5 | Removed from cluster membership |
| `shutting_down` | 6 | Shutdown initiated |

### Alerting rules (Prometheus example)

```yaml
groups:
  - name: meilisearch-cluster
    rules:
      # No leader elected
      - alert: MeilisearchClusterNoLeader
        expr: max(meilisearch_cluster_is_leader) == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "No leader in Meilisearch cluster"

      # Replication lag
      - alert: MeilisearchReplicationLag
        expr: >
          max(meilisearch_cluster_last_applied_log) -
          min(meilisearch_cluster_last_applied_log) > 1000
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Meilisearch cluster replication lag > 1000 entries"

      # Node evicted
      - alert: MeilisearchNodeEvicted
        expr: increase(meilisearch_cluster_nodes_evicted_total[5m]) > 0
        labels:
          severity: warning
        annotations:
          summary: "A node was evicted from the Meilisearch cluster"
```

## Operations

### Scaling up (adding nodes)

1. Choose a unique `--cluster-node-id` not used by any current member.
2. Start the new node with `--cluster-join` and the same `--master-key` (or explicit `--cluster-secret`).
3. The new node joins as a learner, gets promoted to voter automatically.
4. Verify via `/cluster/status` that the `voters` list includes the new ID.

### Graceful leave (removing a node)

Remove a node cleanly so the cluster shrinks without waiting for eviction:

```bash
# From the node itself:
curl -X POST http://localhost:7700/cluster/status/leave \
  -H "Authorization: Bearer your-master-key"

# Or from another machine:
meilisearch --cluster-leave-url http://node3:7700 --master-key "your-master-key"
```

The node removes itself from Raft membership, then shuts down. On Ctrl+C, cluster
nodes attempt a graceful leave with a 5-second timeout before hard shutdown.

### Node restart

A node that was part of a cluster can restart without `--cluster-create` or
`--cluster-join` — just start with the same `--db-path`:

```bash
meilisearch \
  --db-path ./data/node2 \
  --http-addr 0.0.0.0:7710 \
  --master-key "your-master-key"
```

The persisted Raft state (membership, log) is restored from the LMDB store in
`db_path/cluster/`. The node rejoins automatically and catches up via Raft log
replay.

### Disaster recovery

If a majority of nodes are lost (e.g., 2 of 3), the cluster cannot elect a leader
and is unavailable. Recovery options:

1. **Restore from backup**: Restore `db_path/` from a snapshot on a surviving node
   and bootstrap a new single-node cluster with `--cluster-create`.

2. **Force bootstrap**: If one node still has data, start it as a new single-node
   cluster. The Raft log will be lost but the data (indexes, documents) persists.

## Failure Scenarios

### Leader death

When the leader dies, the remaining nodes detect the missing heartbeat and trigger
a Raft election. A new leader is elected within the election timeout
(default: 1.5–3 seconds). Writes fail during the election window but succeed once
a new leader is elected.

### Network partition

If a node is partitioned from the majority, it cannot receive heartbeats and steps
down. The majority side elects a new leader and continues operating. When the
partition heals, the isolated node catches up via Raft log replay.

### Follower lagging

If a follower falls behind by more than `--cluster-max-replication-lag` log entries,
the leader evicts it from the cluster. The evicted node's lifecycle transitions to
`Evicted`. After fixing the issue, the node can rejoin with `--cluster-join`.

### File transfer failures

If a follower fails to receive document files more than
`--cluster-max-transfer-failures` consecutive times, it is evicted. This prevents
a node with disk or network issues from blocking cluster progress.

### Disk full

If a node's disk fills up, LMDB writes fail and the state machine apply errors.
The `meilisearch_cluster_failed_applies_total` metric increments. The node may
fall behind and get evicted. Free disk space and rejoin.

## Read-After-Write Consistency (Barrier)

By default, reads on followers are eventually consistent. If you need read-after-write
consistency (e.g., you wrote via the leader and want to immediately read from a follower),
use the `X-Meili-Barrier` header:

```bash
# Write a document via leader
TASK_UID=$(curl -s -X POST http://leader:7700/indexes/movies/documents \
  -H "Authorization: Bearer your-master-key" \
  -H "Content-Type: application/json" \
  -d '[{"id": 1, "title": "Inception"}]' | jq -r '.taskUid')

# Read from follower with barrier — waits until the follower has processed this task
curl -s http://follower:7710/indexes/movies/search \
  -H "Authorization: Bearer your-master-key" \
  -H "X-Meili-Barrier: $TASK_UID" \
  -H "Content-Type: application/json" \
  -d '{"q": "inception"}'
```

The barrier causes the follower to wait (up to `--barrier-timeout-ms`, default 5000ms)
until the specified task is locally visible before responding. This is useful for flows
that write-then-read and need the read to reflect the write.

For most search workloads, eventual consistency is acceptable and no barrier is needed.

## Limitations

- **Manual node IDs**: Each node requires a unique `--cluster-node-id`.
  There is no auto-assignment. This is a conscious tradeoff — Raft needs stable
  numeric IDs, and auto-assignment requires a coordination protocol.
- **Same master key**: All nodes in a cluster share the same `--master-key`,
  ensuring uniform API access. The cluster secret is derived from it by default.
- **Full data replication**: All nodes maintain a full copy of the data, providing
  linear read scaling without sharding complexity. Maximum dataset size is limited
  to what fits on a single node.
- **Write latency on followers (~2x)**: Follower writes are forwarded to the leader
  via HTTP, adding one extra hop. For lowest write latency, send writes directly to
  the leader. Use `/cluster/status` to discover the leader URL.
- **Single-region deployment**: QUIC-plaintext transport and tight heartbeat defaults
  are designed for LAN/VLAN environments. WAN replication would require TLS, higher
  election timeouts, and possibly witness nodes.

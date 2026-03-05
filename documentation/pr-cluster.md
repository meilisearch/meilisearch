# PR: Add Raft-based cluster mode for horizontal read scaling

## Summary

Adds optional cluster mode (`--features cluster`) with Raft-based consensus
for multi-node Meilisearch deployments. Enables horizontal read scaling
while maintaining write consistency.

**Context**: PR #3593 (custom sync prototype) was closed on 2026-03-05. This
is a complete rewrite using Raft consensus instead of custom sync. It is
complementary to the v1.37.0 enterprise "replicated sharding" feature:

| | This PR | Enterprise replicated sharding |
|---|---------|-------------------------------|
| **Approach** | Full replication via Raft | Partitioned sharding via network config |
| **Scaling** | Horizontal read scaling | Horizontal write scaling |
| **License** | Open source (community) | Enterprise only |
| **Architecture** | Single-leader Raft consensus | Remote index shards |
| **Activation** | `--features cluster` compile flag | `network` configuration |

### Key capabilities

- Automatic leader election and failover (openraft)
- Full data replication: documents, settings, API keys, runtime features
- QUIC transport with HMAC-SHA256 integrity signing and optional TLS encryption (`--cluster-tls`)
- Snapshot bootstrap for new nodes joining populated clusters
- 307 redirect for follower writes (zero proxy overhead)
- Health endpoints for load balancer routing (`/cluster/health/writer`, `/cluster/health/reader`)
- Rolling upgrades with two patterns: restart-based (bare metal) and add-new/retire-old (containers)
- Automatic protocol version negotiation and upgrade across mixed-version clusters
- Barrier header (`X-Meili-Barrier`) for read-after-write consistency
- Prometheus metrics for cluster observability
- Graceful leave and automatic dead-node eviction

### Architecture

Single-leader Raft cluster. Writes go through the leader and are replicated
to all followers via the Raft log. Reads can be served by any node (local
data, no inter-node communication). See `documentation/cluster.md` for full
details.

### What's covered

- 36 Python integration tests covering: cluster lifecycle, document/settings/
  API key replication, leader failover, graceful leave, node eviction, bulk
  indexing, concurrent writes, large documents, barrier consistency, scale
  up/down, 307 redirect, health endpoints, leader-crash-mid-write, concurrent
  writes to same index, leader-crash-mid-batch, network partition (3-node and
  5-node), leader isolation and re-election, fault-injection partition testing
  (follower isolation and leader isolation with live nodes), rolling upgrade
  infrastructure (version-info endpoint, node version tracking, restart
  persistence, leave/rejoin version updates)
- 24 Rust unit tests covering: snapshot protocol, HMAC framing,
  replay protection, LMDB persistence, auth replication
- 95 files changed, ~17,500 lines added
- CI: cluster feature added to test-linux and clippy matrices; schedule-only
  Python integration test job
- Standalone mode completely unaffected (behind feature flag)

### What's NOT covered (known limitations)

- No external network-level partition testing (partitions are tested via
  application-level fault injection; external tools like Toxiproxy (tcp) are
  not relevant since the transport is QUIC/UDP)
- No concurrent writes to same index stress testing (basic contention tested,
  but not high-load stress-tested)
- No sharding (full replication only)
- No cross-major-version rolling upgrades (major version must match; minor may
  differ by 1)
- No Jepsen/formal verification (assessed; see FAQ in `documentation/cluster.md`)

### Community context

This addresses long-standing community requests for clustering:

- https://github.com/meilisearch/meilisearch/issues/1383
- https://github.com/orgs/meilisearch/discussions/617
- PR #3593 was closed on 2026-03-05 — this is a complete rewrite using Raft
  consensus instead of the custom sync approach

## Test plan

- [x] `cargo check -p meilisearch` (non-cluster build unchanged)
- [x] `cargo check -p meilisearch --features cluster`
- [x] `cargo test -p meilisearch-cluster --lib` (24 tests)
- [ ] Python cluster tests: 36/36 pass
- [ ] Manual: create 3-node cluster, add docs, failover, verify

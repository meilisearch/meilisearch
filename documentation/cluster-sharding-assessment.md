# Sharding Compatibility Assessment

> Enterprise-gated feature. This document assesses the current architecture's
> compatibility with sharding and identifies what would need to change.

## Executive Summary

Meilisearch's architecture is **well-suited for index-level sharding** (each node
owns a subset of indexes) because each `Index` is already an isolated LMDB
environment, `IndexScheduler` already batches tasks per-index, and federated
search already fans out across indexes with per-index read transactions and
score-based result merging. The existing `shard_docids` database provides
document-level shard metadata within an index (used by the enterprise network
export/balancing feature) but is not consulted by search.

**Key insight:** Index-level sharding is architecturally simpler and more natural
than document-level sharding, because the isolation boundary already exists at
the index level. Federated search already provides the cross-index result-merging
layer; extending it to query remote nodes for indexes they own is the natural
path.

**Estimated effort to MVP:** ~6-8 weeks for index-level sharding with 2+ nodes,
leveraging existing federated search for query fan-out. Document-level sharding
(splitting a single large index across nodes) is significantly harder and not
recommended as a first step.

## Current Architecture

### Index isolation

Each `Index` (`crates/milli/src/index.rs`) is a self-contained LMDB environment
with `26 + Cellulite::nb_dbs()` named databases:

| Category | Databases | Purpose |
|----------|-----------|---------|
| Core | `main`, `documents`, `external-documents-ids` | Document storage, metadata, ID mapping |
| Full-text | `word-docids`, `exact-word-docids`, `word-prefix-docids`, `exact-word-prefix-docids`, `word-pair-proximity-docids`, `word-position-docids`, `word-field-id-docids`, `word-prefix-position-docids`, `word-prefix-field-id-docids`, `field-id-word-count-docids`, `docid-word-positions` | Inverted index for text search |
| Faceting | `facet-id-f64-docids`, `facet-id-string-docids`, `facet-id-normalized-string-strings`, `facet-id-string-fst`, `facet-id-exists-docids`, `facet-id-is-null-docids`, `facet-id-is-empty-docids`, `field-id-docid-facet-f64s`, `field-id-docid-facet-strings` | Facet filtering and distribution |
| Vector | `vector-embedder-category-id`, `vector-arroy` (hannoy) | Embedding storage and ANN search |
| Geo | `cellulite` (4 sub-databases) | Geo-spatial indexing |
| Sharding | `shard-docids` | Maps shard name to RoaringBitmap of docids |

Each index lives in its own directory (`{base_path}/{uuid}/`) with its own
LMDB `data.mdb` file, write transactions, and map size. There is zero shared
state between indexes at the storage level.

### IndexMapper and LRU cache

`IndexMapper` (`crates/index-scheduler/src/index_mapper/mod.rs`) manages the
mapping from user-facing index names to UUIDs to open `Index` instances:

```
index name (String) --> UUID --> Index (LMDB env)
     index_mapping DB         IndexMap (LRU cache)
```

- `index_mapping`: persistent `Database<Str, UuidCodec>` in the scheduler's LMDB env
- `index_stats`: persistent `Database<UuidCodec, SerdeJson<IndexStats>>` for cached stats
- `IndexMap`: in-memory LRU cache (`LruMap<Uuid, Index>`) that evicts least-recently-used
  indexes by closing their LMDB environments
- `currently_updating_index`: fast-path `Arc<RwLock<Option<(String, Index)>>>` so search
  can read the index being updated without hitting the LRU

Index states: `Missing` (not opened), `Available` (in LRU, ready for queries),
`Closing` (being resized or evicted), `BeingDeleted`.

### Per-index task batching

The autobatcher (`crates/index-scheduler/src/scheduler/autobatcher.rs`) groups
enqueued tasks into batches. The critical constraint: **each batch operates on
exactly one index**. The scheduler's `create_batch()` method:

1. Picks the first enqueued task and identifies its target index
2. Collects `index_tasks` = all enqueued tasks for that index
3. Feeds them through `autobatch()` which merges compatible operations
   (e.g. multiple document imports, settings + clear)
4. Returns a single `BatchKind` that operates on one index

This per-index batching means the scheduler already treats indexes as
independent units of work. No batch ever spans multiple indexes (except
`IndexSwap`, which is non-batched).

### Federated search fan-out

`perform_federated_search()` (`crates/meilisearch/src/search/federated/perform.rs`)
already implements cross-index and cross-node query fan-out:

1. **Partition queries** by destination: `PartitionedQueries` splits incoming
   queries into `local_queries_by_index` (grouped by index name) and
   `remote_queries_by_host` (grouped by remote node name)
2. **Start remote queries**: `RemoteSearch::start()` spawns one tokio task per
   remote host, sending queries via HTTP proxy (`proxy_search`)
3. **Execute local queries**: `SearchByIndex::execute()` opens each local index
   with its own `read_txn()`, runs all queries for that index, and collects
   `SearchResultByQuery` results
4. **Merge results**: `merge_index_global_results()` uses `itertools::kmerge_by`
   to merge local and remote results by weighted score, producing a single
   ranked result list

The per-index read transaction guarantee is important: all queries targeting
the same index see a consistent snapshot. The k-way merge by weighted score
handles heterogeneous result sets from different indexes and remote nodes.

### Existing shard_docids database

The `shard_docids` database (`Database<Str, CboRoaringBitmapCodec>`) in each
index maps shard names to `RoaringBitmap` of internal document IDs.
`DbShardDocids` (`crates/milli/src/sharding/mod.rs`) provides the API.

This is **document-level shard metadata within an index**, used by the
enterprise network export/balancing feature to track which documents belong
to which shard (node). The enterprise edition uses rendezvous hashing
(`crates/milli/src/sharding/enterprise_edition.rs`) with `XxHash3_64` to
assign documents to shards by hashing `(shard_name, document_id)` pairs.

What this database does **not** do:
- Search does not consult `shard_docids` to filter results
- There is no query-time shard routing based on this data
- It is purely write-path metadata for the export/rebalancing system

## Sharding Approaches

### Index-level sharding (recommended first step)

Each node owns a subset of indexes. A query for an index not present on
the receiving node is forwarded to the owning node.

| Aspect | Current state | Required changes |
|--------|--------------|------------------|
| Index isolation | Each index is a separate LMDB env | None -- already independent |
| Query fan-out | Federated search fans out to remote nodes via HTTP | Extend to auto-route single-index queries to owning node |
| Write routing | Single Raft leader, 307 redirect from followers | Per-index leader via multi-Raft, or index placement map |
| Task scheduling | Per-index batching in autobatcher | Add shard-awareness: skip tasks for indexes not owned locally |
| Index placement | Not tracked | New: consistent hash or explicit mapping of index name to node(s) |
| Snapshots | Whole-node snapshot | Per-index snapshots already natural (each is a separate dir) |

#### Document placement

Consistent hashing by index name assigns each index to a set of replica nodes.
The existing `IndexMapper.index_mapping` database already provides the
name-to-UUID mapping; an additional `index_placement` map would track which
node(s) own each index UUID.

#### Query fan-out

For single-index queries, the receiving node checks the placement map:
- If local: execute directly (current path)
- If remote: forward to owning node(s), return result

For federated search, the existing `PartitionedQueries` logic already
separates local vs. remote queries by index. The only addition is automatic
partition based on the placement map rather than explicit `remote` annotations
in the query.

The QUIC transport (already used for Raft and DML channels) is a natural
candidate for a query channel, avoiding HTTP overhead for inter-node queries.

#### Write routing

Two options, in order of complexity:

1. **Redirect model** (simpler): Any node can receive writes. If the target
   index is not local, 307 redirect to the owning node's leader. Same pattern
   as current follower-to-leader redirect.

2. **Multi-Raft** (true horizontal write scaling): One Raft group per index
   (or per shard group). Each group has its own leader. `openraft` supports
   multiple instances per process. This eliminates the single-leader bottleneck
   but adds significant complexity.

### Document-level sharding (future, harder)

Splitting a single index across multiple nodes. Each node holds a partition
of the documents and a corresponding partition of all inverted-index databases.

| Challenge | Why it is hard |
|-----------|---------------|
| Inverted index partitioning | `word_docids`, `facet_id_f64_docids`, etc. use `RoaringBitmap` keyed by internal `DocumentId`. Splitting requires either disjoint docid spaces or bitmap remapping at merge time. |
| Query execution | Every search must fan out to all shards of the index and merge results. The k-way merge exists in federated search but assumes independent scoring; document-level sharding would need global IDF or two-phase scoring. |
| Facet aggregation | `FacetDistribution` and `facet_stats` would need cross-shard aggregation beyond simple merge. |
| Vector search | `hannoy` (ANN) search produces approximate results per shard; merging approximate results degrades recall. |
| Geo search | The R-tree (`cellulite`) is per-index; partitioning spatial indexes across nodes requires a spatial partitioning scheme. |
| Atomic writes | A document addition must update the correct shard. The existing `shard_docids` + rendezvous hashing provides the placement logic, but cross-shard transactions are not supported. |

The existing `shard_docids` database and enterprise rendezvous hashing provide
the document-placement foundation for this approach, but search-time fan-out
and result merging across document-level shards are not implemented.

## Key Source Files

| File | Role |
|------|------|
| `crates/milli/src/index.rs` | `Index` struct: 26+ LMDB databases per index, `CreateOrOpen`, version management |
| `crates/index-scheduler/src/index_mapper/mod.rs` | `IndexMapper`: name-to-UUID mapping, LRU cache of open indexes |
| `crates/index-scheduler/src/index_mapper/index_map.rs` | `IndexMap`: LRU eviction, closing/reopening lifecycle |
| `crates/index-scheduler/src/scheduler/autobatcher.rs` | Per-index task batching: `autobatch()`, `BatchKind`, accumulation rules |
| `crates/index-scheduler/src/scheduler/create_batch.rs` | `create_batch()`: selects index, collects `index_tasks`, calls autobatcher |
| `crates/meilisearch/src/search/federated/perform.rs` | `perform_federated_search()`: query partitioning, local/remote fan-out, k-way score merge |
| `crates/milli/src/sharding/mod.rs` | `DbShardDocids`: shard-name-to-docid-bitmap API, `rebalance_shards()` |
| `crates/milli/src/sharding/enterprise_edition.rs` | Rendezvous hashing: `processing_shard()`, `hash_rendezvous()`, `reshard()` |
| `crates/index-scheduler/src/scheduler/enterprise_edition/network.rs` | Network export/balancing: shard rebalancing, document export to remote nodes |

## Compatibility Assessment

| Layer | Index-level sharding | Document-level sharding |
|-------|---------------------|------------------------|
| LMDB storage | **High** -- each index is already isolated | **Low** -- would need docid-space partitioning |
| IndexMapper | **High** -- add placement metadata alongside UUID | **Medium** -- need shard-aware index resolution |
| Autobatcher | **High** -- already per-index, just filter by ownership | **Medium** -- need cross-shard task coordination |
| Federated search | **High** -- already fans out across indexes and remotes | **Medium** -- need per-shard fan-out within a single index |
| Raft replication | **Medium** -- multi-Raft for per-index groups | **Medium** -- same, but more groups |
| Snapshot/restore | **High** -- per-index directories already independent | **Low** -- need shard-consistent snapshots |
| `shard_docids` | Not needed (index-level placement) | **High** -- already tracks doc-to-shard mapping |

## Recommendations

1. **Pursue index-level sharding first.** The architecture is naturally aligned:
   isolated LMDB environments, per-index batching, and federated search with
   k-way merge. The implementation is mostly routing and placement metadata.

2. **Extend federated search for automatic routing.** The `PartitionedQueries`
   mechanism already separates local vs. remote. Adding an `index_placement`
   lookup to automatically route queries to owning nodes is a small extension
   of existing code.

3. **Use QUIC for inter-node queries.** The cluster transport already has QUIC
   channels for Raft, DML, and snapshots. A query channel avoids HTTP overhead
   for the latency-sensitive search path.

4. **Defer document-level sharding.** It requires solving hard problems
   (global IDF, approximate ANN merge, spatial index partitioning, cross-shard
   facet aggregation) that provide diminishing returns for most use cases.
   Index-level sharding handles the common case of many medium-sized indexes.

5. **Continue hardening single-leader clustering first.** Rolling upgrades,
   partition tolerance, and operational tooling are prerequisites for any
   sharding mode.

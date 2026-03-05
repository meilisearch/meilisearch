# Cluster Performance: Data Transfer Efficiency

## Design Philosophy

The cluster is designed to be **as fast as standalone Meilisearch** for the
operations that matter most:

- **Writes to the leader**: identical to standalone. Raft commit adds only
  the network round-trip for quorum replication (milliseconds on a LAN).
- **Writes via followers**: 307 redirect, then direct to leader. After the
  client discovers the leader (one redirect), subsequent writes go direct.
- **Reads**: fully local, zero inter-node communication. Identical to standalone.

The default transport layer (QUIC with HMAC-only signing) minimizes overhead
on trusted LANs. For untrusted networks, `--cluster-tls` enables QUIC TLS
encryption (~1-5% CPU overhead with AES-NI). Network-level encryption
(WireGuard) is also supported and can be combined with or used instead of TLS.

## Data Transfer Paths

### 1. Raft Log Replication (frequent, latency-sensitive)

**Path**: Leader LMDB → bincode deserialize → RaftRpc enum → bincode serialize
→ HMAC sign → QUIC frame → follower

**Copy count**: 4 per entry (deserialize from LMDB, serialize into RPC, frame
building, QUIC send buffer).

**Why this is acceptable**: Raft AppendEntries RPCs are small (task metadata,
not document content). The actual document data is transferred via DML (see
below) before the Raft entry is proposed. The RPC carries only a
bincode-encoded `KindWithContent` — typically a few hundred bytes.

**Optimization opportunity**: The enum wrapping (`RaftRpc::AppendEntries(req)`)
adds one serialization layer. Sending entries with a discriminant byte directly
would save one serialize/deserialize cycle. Impact: ~5-15% CPU reduction for
high-frequency replication.

### 2. DML Content File Transfer (infrequent, throughput-sensitive)

**Path**: Leader filesystem → 64KB chunked reads → HMAC sign per chunk → QUIC
stream → follower filesystem

**Copy count**: 2 per 64KB chunk (disk read buffer, frame building).

**Streaming**: Files are streamed in 64KB chunks — never loaded fully into
memory. Large document uploads (100MB+) transfer without proportional memory
usage.

**Bottleneck**: Disk I/O dominates. The 44-byte framing overhead per 64KB
chunk is 0.07% — negligible.

### 3. Snapshot Transfer (rare, throughput-sensitive)

**Path**: LMDB `copy_to_path()` → temp directory → 4MB chunked reads →
xxhash64 per chunk → HMAC sign → QUIC stream → follower filesystem

**Copy count**: 3 per 4MB chunk (LMDB copy to temp, disk read, frame building).

**Note**: The LMDB `copy_to_path()` step creates a consistent point-in-time
copy of the database. This is an inherent cost of consistent snapshots — the
alternative (reading from live LMDB while it's being modified) would require
complex coordination.

**Chunk integrity**: Each 4MB chunk is verified with xxhash64 on the receiver.
Corrupted chunks are re-requested. This adds negligible CPU cost (~2-5µs per
4MB chunk).

### 4. LMDB Read Path

**Status**: Partial zero-copy. LMDB provides memory-mapped zero-copy access
to raw bytes via `RoTxn` (read transactions). However, the heed `SerdeBincode`
codec deserializes values into owned Rust types (`Entry<TypeConfig>`), which
allocates heap memory.

**Why this is acceptable**: Raft log entries are small (metadata, not document
content). The deserialization cost is dominated by the network round-trip time
for replication. Switching to a zero-copy codec (Cap'n Proto, FlatBuffers)
would require a protocol-breaking change with no proportional benefit.

## QUIC Transport Performance

QUIC (via quinn) provides:
- **Multiplexed streams**: 3 independent channels (raft, dml, snapshot) over
  one UDP connection. No head-of-line blocking.
- **Flow control**: Built-in per-stream and per-connection flow control.
- **Connection migration**: Handles IP changes without reconnection.

**Default mode (plaintext + HMAC)**: The only per-message CPU cost is
HMAC-SHA256 (a few microseconds per message). No TLS handshake overhead.

**TLS mode (`--cluster-tls`)**: Adds AES-GCM encryption/decryption per
message. On modern hardware with AES-NI, this costs ~1-5% CPU. The TLS
handshake uses a deterministic Ed25519 certificate derived from the cluster
secret — no external CA or cert distribution needed.

## Summary

| Transfer | Frequency | Bottleneck | Zero-copy? | Speed |
|----------|-----------|-----------|-----------|-------|
| Raft RPCs | Every write | Network RTT | No (4 copies, but small payloads) | Fast |
| DML files | Per document upload | Disk I/O | Partial (streaming, 2 copies/chunk) | Fast |
| Snapshots | Node join only | Disk I/O + network | Partial (3 copies/chunk) | Adequate |
| LMDB reads | Every Raft operation | Memory alloc | Partial (mmap'd, then deserialized) | Fast |
| QUIC framing | Every message | HMAC computation | No (frame copy) | Fast |

The design prioritizes **correctness and simplicity** over eliminating every
copy. The real-world bottleneck for cluster performance is standalone
Meilisearch's indexing speed — the cluster transport adds negligible overhead
for typical workloads (document sizes >> Raft metadata sizes).

## Future Optimization Opportunities

If profiling reveals transport overhead in production:

1. **Vectored QUIC writes**: Provide header/data/sig as separate slices to
   avoid frame-building copy. Requires quinn API support.
2. **Direct LMDB streaming for snapshots**: Read LMDB files directly instead
   of `copy_to_path()` + re-read. Saves one full-database copy but
   complicates consistency guarantees.
3. **Batched entry serialization**: Amortize RPC enum overhead across
   multiple entries in a single AppendEntries call.

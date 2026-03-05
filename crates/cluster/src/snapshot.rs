//! Snapshot bootstrap: full-state transfer for new nodes joining a cluster with existing data.
//!
//! When a new node joins a cluster that already has indexes and documents, the Raft log
//! alone is insufficient (compacted entries are lost). This module transfers a consistent
//! point-in-time snapshot of the leader's database to the joining node.
//!
//! # Protocol (v2 — chunked transfer with xxhash64 integrity)
//!
//! The snapshot channel uses a three-phase protocol:
//!
//! ## Phase 1: Manifest
//! 1. **Joiner sends** a `SnapshotRequest` (1 byte: `0x01`)
//! 2. **Leader prepares** a consistent snapshot via `SnapshotProvider` (LMDB `copy_to_path`)
//! 3. **Leader sends** a JSON `SnapshotManifest` listing all files, sizes, and chunk counts
//!
//! ## Phase 2: Chunked transfer
//! 4. **Leader streams** chunks (4MB each), each containing:
//!    - `ChunkHeader` (file_index, chunk_index, xxhash64, data_len)
//!    - Raw chunk data
//! 5. **Leader sends** an end-of-transfer marker (empty frame)
//!
//! ## Phase 3: Verification + retry
//! 6. **Joiner sends** `ChunkAck` listing any missing/corrupted chunk IDs
//! 7. If non-empty, leader re-sends only those chunks and sends another end-of-transfer
//! 8. Repeat until joiner sends an empty `ChunkAck`
//! 9. **Joiner sends** a final ACK (1 byte: `0x01`)
//!
//! # Consistency
//!
//! The leader creates the snapshot via `SnapshotProvider::prepare_snapshot()`, which uses
//! LMDB's `copy_to_path()` for each environment. This guarantees a consistent point-in-time
//! view even as writes continue. The joiner catches up via Raft log replay for entries
//! committed during the transfer.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{debug, info, warn};
use xxhash_rust::xxh64::xxh64;

use crate::transport::ChannelPair;

/// Trait for preparing a consistent snapshot of the Meilisearch database.
///
/// Implemented by the index-scheduler in `main.rs` to provide LMDB `copy_to_path()`
/// snapshots of all environments (index-scheduler, auth, each index). This avoids
/// reading raw LMDB files which could see partially-written pages.
pub trait SnapshotProvider: Send + Sync + 'static {
    /// Prepare a consistent snapshot of the entire database into a temporary directory.
    ///
    /// The temp directory should contain:
    /// - `VERSION` file
    /// - `tasks/data.mdb` (index-scheduler LMDB)
    /// - `auth/data.mdb` (auth LMDB)
    /// - `indexes/{uuid}/data.mdb` for each index
    /// - `update_files/` directory with pending update files
    /// - `instance-uid` if present
    ///
    /// `max_compaction_age_s`: controls LMDB compaction before snapshot.
    /// - `None` → never compact (raw LMDB copy)
    /// - `Some(0)` → always compact (smallest transfer)
    /// - `Some(N)` → compact only if last compaction was more than N seconds ago
    fn prepare_snapshot(&self, max_compaction_age_s: Option<u64>) -> anyhow::Result<tempfile::TempDir>;
}

// ─── Protocol constants ──────────────────────────────────────────────────────

/// Snapshot request byte sent by joiner.
const SNAPSHOT_REQUEST: u8 = 0x01;
/// Final ACK byte sent by joiner after successful transfer.
const SNAPSHOT_ACK: u8 = 0x01;
/// Chunk size for data transfer (4MB).
const CHUNK_SIZE: usize = 4 * 1024 * 1024;
/// Maximum retry rounds for missing chunks before giving up.
const MAX_RETRY_ROUNDS: usize = 5;

// ─── Manifest types ──────────────────────────────────────────────────────────

/// Manifest describing all files in a snapshot and how they're chunked.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotManifest {
    /// Files in the snapshot, ordered by index.
    pub files: Vec<FileEntry>,
    /// Size of each chunk in bytes (last chunk of each file may be smaller).
    pub chunk_size: u64,
    /// Total size of all files combined.
    pub total_size: u64,
    /// Total number of chunks across all files.
    pub total_chunks: u64,
}

/// A single file in the snapshot manifest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileEntry {
    /// Relative path within the snapshot (e.g., "indexes/abc123/data.mdb").
    pub path: String,
    /// File size in bytes.
    pub size: u64,
    /// Number of chunks this file is split into.
    pub chunk_count: u32,
}

/// Identifies a specific chunk for retry requests.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkId {
    pub file_index: u16,
    pub chunk_index: u32,
}

/// Acknowledgement sent by joiner listing missing/corrupted chunks.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkAck {
    pub missing: Vec<ChunkId>,
}

/// Header prepended to each chunk's data in the signed frame.
/// Wire format: file_index(2) + chunk_index(4) + xxhash64(8) + data_len(4) = 18 bytes header.
const CHUNK_HEADER_SIZE: usize = 2 + 4 + 8 + 4;

// ─── Metrics ─────────────────────────────────────────────────────────────────

/// Metrics for snapshot transfer progress.
pub struct SnapshotMetrics {
    /// Total bytes transferred.
    pub bytes_transferred: Arc<AtomicU64>,
}

impl Default for SnapshotMetrics {
    fn default() -> Self {
        Self {
            bytes_transferred: Arc::new(AtomicU64::new(0)),
        }
    }
}

// ─── Manifest building ───────────────────────────────────────────────────────

/// Build a manifest by walking a snapshot directory.
///
/// Returns the manifest. The `snapshot_dir` must be the temp directory produced
/// by `SnapshotProvider::prepare_snapshot()`.
pub fn build_manifest(snapshot_dir: &Path) -> Result<SnapshotManifest> {
    let mut files = Vec::new();
    let mut total_size: u64 = 0;
    let mut total_chunks: u64 = 0;

    collect_files(snapshot_dir, snapshot_dir, &mut files)?;

    for file in &mut files {
        total_size += file.size;
        let chunks = if file.size == 0 {
            1 // send at least one chunk for empty files to create them
        } else {
            file.size.div_ceil(CHUNK_SIZE as u64) as u32
        };
        file.chunk_count = chunks;
        total_chunks += chunks as u64;
    }

    info!(
        file_count = files.len(),
        total_size_mb = total_size / (1024 * 1024),
        total_chunks,
        "Built snapshot manifest"
    );

    Ok(SnapshotManifest {
        files,
        chunk_size: CHUNK_SIZE as u64,
        total_size,
        total_chunks,
    })
}

/// Recursively collect files from a directory into the file list.
fn collect_files(dir: &Path, base: &Path, files: &mut Vec<FileEntry>) -> Result<()> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory: {}", dir.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("failed to read dir entries")?;

    // Sort for deterministic ordering
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let metadata = entry.metadata().context("failed to read metadata")?;

        if metadata.is_dir() {
            collect_files(&path, base, files)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(base)
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();
            files.push(FileEntry {
                path: relative,
                size: metadata.len(),
                chunk_count: 0, // filled in by build_manifest
            });
        }
    }

    Ok(())
}

// ─── Sender (leader side) ────────────────────────────────────────────────────

/// Encode a chunk header into bytes.
fn encode_chunk_header(file_index: u16, chunk_index: u32, hash: u64, data_len: u32) -> [u8; CHUNK_HEADER_SIZE] {
    let mut buf = [0u8; CHUNK_HEADER_SIZE];
    buf[0..2].copy_from_slice(&file_index.to_le_bytes());
    buf[2..6].copy_from_slice(&chunk_index.to_le_bytes());
    buf[6..14].copy_from_slice(&hash.to_le_bytes());
    buf[14..18].copy_from_slice(&data_len.to_le_bytes());
    buf
}

/// Decode a chunk header from bytes.
fn decode_chunk_header(data: &[u8]) -> Result<(u16, u32, u64, u32)> {
    if data.len() < CHUNK_HEADER_SIZE {
        anyhow::bail!("chunk header too short: {} bytes", data.len());
    }
    let file_index = u16::from_le_bytes(data[0..2].try_into().unwrap());
    let chunk_index = u32::from_le_bytes(data[2..6].try_into().unwrap());
    let hash = u64::from_le_bytes(data[6..14].try_into().unwrap());
    let data_len = u32::from_le_bytes(data[14..18].try_into().unwrap());
    Ok((file_index, chunk_index, hash, data_len))
}

/// Read a specific chunk from a file on disk.
fn read_chunk(file_path: &Path, chunk_index: u32, chunk_size: usize) -> Result<Vec<u8>> {
    let mut file = std::fs::File::open(file_path)
        .with_context(|| format!("failed to open file: {}", file_path.display()))?;

    let offset = chunk_index as u64 * chunk_size as u64;
    file.seek(SeekFrom::Start(offset))?;

    let file_len = file.metadata()?.len();
    let remaining = file_len.saturating_sub(offset);
    let read_size = remaining.min(chunk_size as u64) as usize;

    let mut buf = vec![0u8; read_size];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

/// Send the chunked snapshot over the channel (leader side).
///
/// Sends manifest, then all chunks with xxhash64 verification, then handles retry rounds.
pub async fn send_snapshot_chunked(
    ch: &mut ChannelPair,
    secret: &[u8],
    snapshot_dir: &Path,
    manifest: &SnapshotManifest,
    metrics: &SnapshotMetrics,
) -> Result<()> {
    // Send manifest as JSON
    let manifest_json = serde_json::to_vec(manifest).context("failed to serialize manifest")?;
    let seq = ch.send_seq;
    ch.send_seq += 1;
    crate::framing::send_signed(&mut ch.send, seq, &manifest_json, secret).await?;

    info!(
        total_chunks = manifest.total_chunks,
        total_size_mb = manifest.total_size / (1024 * 1024),
        "Sending chunked snapshot"
    );

    // Send all chunks
    send_chunks(ch, secret, snapshot_dir, manifest, &manifest.files.iter().enumerate().flat_map(|(fi, f)| {
        (0..f.chunk_count).map(move |ci| ChunkId { file_index: fi as u16, chunk_index: ci })
    }).collect::<Vec<_>>(), metrics).await?;

    // Send end-of-transfer marker
    let seq = ch.send_seq;
    ch.send_seq += 1;
    crate::framing::send_signed(&mut ch.send, seq, &[], secret).await?;

    // Retry loop for missing chunks
    for round in 0..MAX_RETRY_ROUNDS {
        let (ack_seq, ack_data) = crate::framing::recv_signed(&mut ch.recv, secret).await?;
        if ack_seq <= ch.recv_seq {
            anyhow::bail!(
                "replay detected on chunk ack: received seq {ack_seq}, expected > {}",
                ch.recv_seq
            );
        }
        ch.recv_seq = ack_seq;

        let ack: ChunkAck = serde_json::from_slice(&ack_data).context("failed to parse ChunkAck")?;

        if ack.missing.is_empty() {
            break;
        }

        info!(
            round = round + 1,
            missing_count = ack.missing.len(),
            "Retrying missing chunks"
        );

        // Re-send only the missing chunks
        send_chunks(ch, secret, snapshot_dir, manifest, &ack.missing, metrics).await?;

        // Send end-of-transfer marker for this retry round
        let seq = ch.send_seq;
        ch.send_seq += 1;
        crate::framing::send_signed(&mut ch.send, seq, &[], secret).await?;
    }

    // Wait for final ACK
    let (ack_seq, ack_data) = crate::framing::recv_signed(&mut ch.recv, secret).await?;
    if ack_seq <= ch.recv_seq {
        anyhow::bail!(
            "replay detected on final ack: received seq {ack_seq}, expected > {}",
            ch.recv_seq
        );
    }
    ch.recv_seq = ack_seq;

    if ack_data.first() != Some(&SNAPSHOT_ACK) {
        anyhow::bail!("unexpected final ACK byte: {:?}", ack_data.first());
    }

    info!(
        total_bytes = manifest.total_size,
        "Chunked snapshot transfer complete"
    );
    Ok(())
}

/// Send a set of chunks over the channel.
async fn send_chunks(
    ch: &mut ChannelPair,
    secret: &[u8],
    snapshot_dir: &Path,
    manifest: &SnapshotManifest,
    chunks: &[ChunkId],
    metrics: &SnapshotMetrics,
) -> Result<()> {
    let mut chunks_sent: u64 = 0;
    let mut last_progress_pct: u64 = 0;

    for chunk_id in chunks {
        let file_entry = manifest.files.get(chunk_id.file_index as usize)
            .context("invalid file_index in chunk list")?;
        let file_path = snapshot_dir.join(&file_entry.path);

        // Read chunk data (may be empty for zero-length files)
        let data = if file_entry.size == 0 {
            Vec::new()
        } else {
            read_chunk(&file_path, chunk_id.chunk_index, CHUNK_SIZE)?
        };

        let hash = xxh64(&data, 0);
        let header = encode_chunk_header(
            chunk_id.file_index,
            chunk_id.chunk_index,
            hash,
            data.len() as u32,
        );

        // Combine header + data into a single frame
        let mut frame = Vec::with_capacity(CHUNK_HEADER_SIZE + data.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&data);

        let seq = ch.send_seq;
        ch.send_seq += 1;
        crate::framing::send_signed(&mut ch.send, seq, &frame, secret).await?;

        metrics.bytes_transferred.fetch_add(data.len() as u64, Ordering::Relaxed);
        chunks_sent += 1;

        // Log progress every 10%
        if manifest.total_chunks > 0 {
            let pct = (chunks_sent * 100) / manifest.total_chunks;
            if pct / 10 > last_progress_pct / 10 {
                info!(
                    progress_pct = pct,
                    chunks_sent,
                    total_chunks = manifest.total_chunks,
                    "Snapshot send progress"
                );
                last_progress_pct = pct;
            }
        }
    }

    Ok(())
}

// ─── Receiver (joiner side) ──────────────────────────────────────────────────

/// Request and receive a chunked snapshot from the leader (joiner side).
///
/// Creates files from the manifest, receives chunks with xxhash64 verification,
/// handles retry for corrupted/missing chunks, then atomically swaps the snapshot
/// into `target_path` (preserving the `cluster/` directory).
pub async fn receive_snapshot_chunked(
    ch: &mut ChannelPair,
    secret: &[u8],
    target_path: &Path,
    metrics: &SnapshotMetrics,
) -> Result<()> {
    // Send snapshot request
    let seq = ch.send_seq;
    ch.send_seq += 1;
    crate::framing::send_signed(&mut ch.send, seq, &[SNAPSHOT_REQUEST], secret).await?;

    // Receive manifest
    let (manifest_seq, manifest_data) = crate::framing::recv_signed(&mut ch.recv, secret).await?;
    if manifest_seq <= ch.recv_seq {
        anyhow::bail!(
            "replay detected on manifest: received seq {manifest_seq}, expected > {}",
            ch.recv_seq
        );
    }
    ch.recv_seq = manifest_seq;

    let manifest: SnapshotManifest =
        serde_json::from_slice(&manifest_data).context("failed to parse SnapshotManifest")?;

    info!(
        files = manifest.files.len(),
        total_size_mb = manifest.total_size / (1024 * 1024),
        total_chunks = manifest.total_chunks,
        "Received snapshot manifest"
    );

    // Create output directory and pre-create all files
    let parent = target_path.parent().unwrap_or_else(|| Path::new("."));
    let temp_dir = tempfile::tempdir_in(parent)
        .context("failed to create temp directory for snapshot")?;

    for file_entry in &manifest.files {
        let file_path = temp_dir.path().join(&file_entry.path);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Pre-create the file at the expected size (sparse file)
        let file = std::fs::File::create(&file_path)
            .with_context(|| format!("failed to create file: {}", file_entry.path))?;
        if file_entry.size > 0 {
            file.set_len(file_entry.size)?;
        }
    }

    // Track which chunks we've received successfully
    let mut received: Vec<Vec<bool>> = manifest
        .files
        .iter()
        .map(|f| vec![false; f.chunk_count as usize])
        .collect();

    // Receive chunks in a loop (initial + retry rounds)
    for round in 0..=MAX_RETRY_ROUNDS {
        receive_chunk_stream(ch, secret, temp_dir.path(), &manifest, &mut received, metrics).await?;

        // Check for missing chunks
        let missing = find_missing_chunks(&received);

        // Send ChunkAck
        let ack = ChunkAck { missing: missing.clone() };
        let ack_data = serde_json::to_vec(&ack).context("failed to serialize ChunkAck")?;
        let seq = ch.send_seq;
        ch.send_seq += 1;
        crate::framing::send_signed(&mut ch.send, seq, &ack_data, secret).await?;

        if missing.is_empty() {
            break;
        }

        if round == MAX_RETRY_ROUNDS {
            anyhow::bail!(
                "snapshot transfer failed: {} chunks still missing after {} retry rounds",
                missing.len(),
                MAX_RETRY_ROUNDS
            );
        }

        info!(
            round = round + 1,
            missing_count = missing.len(),
            "Requesting retry for missing chunks"
        );
    }

    // Send final ACK
    let seq = ch.send_seq;
    ch.send_seq += 1;
    crate::framing::send_signed(&mut ch.send, seq, &[SNAPSHOT_ACK], secret).await?;

    // Install the snapshot: swap into target_path, preserving cluster/ dir
    install_snapshot(temp_dir.path(), target_path)?;

    info!("Snapshot installed at {}", target_path.display());
    Ok(())
}

/// Receive a stream of chunks until an empty frame (end-of-transfer marker).
async fn receive_chunk_stream(
    ch: &mut ChannelPair,
    secret: &[u8],
    output_dir: &Path,
    manifest: &SnapshotManifest,
    received: &mut [Vec<bool>],
    metrics: &SnapshotMetrics,
) -> Result<()> {
    loop {
        let (chunk_seq, frame) = crate::framing::recv_signed(&mut ch.recv, secret).await?;
        if chunk_seq <= ch.recv_seq {
            anyhow::bail!(
                "replay detected on chunk: received seq {chunk_seq}, expected > {}",
                ch.recv_seq
            );
        }
        ch.recv_seq = chunk_seq;

        // Empty frame = end-of-transfer marker
        if frame.is_empty() {
            break;
        }

        // Decode header
        let (file_index, chunk_index, expected_hash, data_len) = decode_chunk_header(&frame)?;

        if frame.len() < CHUNK_HEADER_SIZE + data_len as usize {
            warn!(
                file_index,
                chunk_index,
                expected = CHUNK_HEADER_SIZE + data_len as usize,
                actual = frame.len(),
                "Chunk frame too short, marking as missing"
            );
            continue;
        }

        let data = &frame[CHUNK_HEADER_SIZE..CHUNK_HEADER_SIZE + data_len as usize];

        // Verify xxhash64
        let actual_hash = xxh64(data, 0);
        if actual_hash != expected_hash {
            warn!(
                file_index,
                chunk_index,
                expected_hash,
                actual_hash,
                "Chunk hash mismatch, marking as missing"
            );
            continue;
        }

        // Write to the correct file at the correct offset
        let file_entry = manifest.files.get(file_index as usize)
            .context("invalid file_index in chunk")?;
        let file_path = output_dir.join(&file_entry.path);

        if !data.is_empty() {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&file_path)
                .with_context(|| format!("failed to open file for writing: {}", file_entry.path))?;

            let offset = chunk_index as u64 * manifest.chunk_size;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(data)?;
        }

        // Mark as received
        if let Some(file_received) = received.get_mut(file_index as usize) {
            if let Some(slot) = file_received.get_mut(chunk_index as usize) {
                *slot = true;
            }
        }

        metrics.bytes_transferred.fetch_add(data.len() as u64, Ordering::Relaxed);
    }

    Ok(())
}

/// Find all chunks that haven't been received successfully.
fn find_missing_chunks(received: &[Vec<bool>]) -> Vec<ChunkId> {
    let mut missing = Vec::new();
    for (fi, file_chunks) in received.iter().enumerate() {
        for (ci, &got) in file_chunks.iter().enumerate() {
            if !got {
                missing.push(ChunkId {
                    file_index: fi as u16,
                    chunk_index: ci as u32,
                });
            }
        }
    }
    missing
}

/// Install a snapshot from a temp directory into the target path.
///
/// Preserves the `cluster/` directory in the target (joiner's Raft state).
fn install_snapshot(snapshot_dir: &Path, target_path: &Path) -> Result<()> {
    let parent = target_path.parent().unwrap_or_else(|| Path::new("."));

    // Preserve the cluster/ directory from the target
    let cluster_src = target_path.join("cluster");
    let cluster_temp = snapshot_dir.join("_cluster_preserve");
    if cluster_src.exists() {
        copy_dir_recursive(&cluster_src, &cluster_temp)
            .context("failed to preserve cluster directory")?;
    }

    // Create trash dir for atomic-ish swap
    let trash_dir = tempfile::tempdir_in(parent)
        .context("failed to create trash directory")?;

    // Move existing non-cluster files to trash
    if target_path.exists() {
        for entry in std::fs::read_dir(target_path).context("failed to read target directory")? {
            let entry = entry?;
            let name = entry.file_name();
            if name == "cluster" {
                continue;
            }
            let dest = trash_dir.path().join(&name);
            std::fs::rename(entry.path(), &dest)
                .with_context(|| format!("failed to move {} to trash", name.to_string_lossy()))?;
        }
    }

    // Move snapshot contents into target
    for entry in std::fs::read_dir(snapshot_dir).context("failed to read snapshot directory")? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "_cluster_preserve" {
            continue;
        }
        let dest = target_path.join(&name);
        std::fs::rename(entry.path(), &dest).with_context(|| {
            format!("failed to move snapshot file {} into place", name.to_string_lossy())
        })?;
    }

    // Restore preserved cluster directory
    if cluster_temp.exists() {
        let cluster_dest = target_path.join("cluster");
        if !cluster_dest.exists() {
            std::fs::rename(&cluster_temp, &cluster_dest)
                .context("failed to restore cluster directory")?;
        }
    }

    // Clean up trash dir (best-effort)
    let _ = trash_dir.close();

    Ok(())
}

// ─── Handler (leader side, called from accept loop) ──────────────────────────

/// Handle an incoming snapshot request on the leader side.
///
/// Uses the `SnapshotProvider` to prepare a consistent snapshot, then streams
/// it using the chunked protocol.
pub async fn handle_snapshot_request_chunked(
    ch: &mut ChannelPair,
    secret: &[u8],
    provider: &dyn SnapshotProvider,
    max_compaction_age_s: Option<u64>,
    metrics: &SnapshotMetrics,
) -> Result<()> {
    // Read the request byte
    let (req_seq, req_data) = crate::framing::recv_signed(&mut ch.recv, secret).await?;
    if req_seq <= ch.recv_seq {
        anyhow::bail!(
            "replay detected on snapshot request: received seq {req_seq}, expected > {}",
            ch.recv_seq
        );
    }
    ch.recv_seq = req_seq;

    if req_data.first() != Some(&SNAPSHOT_REQUEST) {
        anyhow::bail!("unexpected snapshot request byte: {:?}", req_data.first());
    }

    info!("Snapshot requested, preparing consistent snapshot via SnapshotProvider");

    // Prepare snapshot using LMDB copy_to_path (blocking I/O)
    let temp_dir = tokio::task::block_in_place(|| provider.prepare_snapshot(max_compaction_age_s))?;

    // Build manifest from the prepared snapshot
    let manifest = tokio::task::block_in_place(|| build_manifest(temp_dir.path()))?;

    // Stream the snapshot
    send_snapshot_chunked(ch, secret, temp_dir.path(), &manifest, metrics).await?;

    // temp_dir is dropped here, cleaning up the snapshot files

    Ok(())
}

// ─── Legacy functions (kept for backward compatibility and tests) ────────────

/// Directories and patterns to exclude from the legacy tar archive.
const EXCLUDED_DIRS: &[&str] = &["cluster"];
const EXCLUDED_FILES: &[&str] = &["data.mdb-lock"];

/// Build a gzip-compressed tar archive of the database directory, excluding cluster state.
/// (Legacy function — kept for tests and fallback.)
pub fn build_snapshot_archive(db_path: &Path) -> Result<Vec<u8>> {
    let mut archive_buf = Vec::new();

    {
        let enc = flate2::write::GzEncoder::new(&mut archive_buf, flate2::Compression::fast());
        let mut builder = tar::Builder::new(enc);
        add_dir_to_tar(&mut builder, db_path, db_path)?;
        let enc = builder.into_inner().context("failed to finalize tar archive")?;
        enc.finish().context("failed to finalize gzip compression")?;
    }

    info!(
        archive_size_bytes = archive_buf.len(),
        archive_size_mb = archive_buf.len() / (1024 * 1024),
        "Built snapshot archive"
    );

    Ok(archive_buf)
}

/// Recursively add directory contents to a tar archive, respecting exclusion rules.
fn add_dir_to_tar<W: Write>(
    builder: &mut tar::Builder<W>,
    dir: &Path,
    base: &Path,
) -> Result<()> {
    let entries = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory: {}", dir.display()))?;

    for entry in entries {
        let entry = entry.context("failed to read dir entry")?;
        let path = entry.path();
        let relative = path.strip_prefix(base).unwrap_or(&path);
        let relative_str = relative.to_string_lossy();

        if let Some(first_component) = relative.components().next() {
            let first = first_component.as_os_str().to_string_lossy();
            if EXCLUDED_DIRS.iter().any(|d| *d == first.as_ref()) {
                debug!(path = %relative_str, "Skipping excluded directory");
                continue;
            }
        }

        let file_name = path.file_name().map(|f| f.to_string_lossy()).unwrap_or_default();
        if EXCLUDED_FILES.iter().any(|f| *f == file_name.as_ref()) {
            debug!(path = %relative_str, "Skipping excluded file");
            continue;
        }

        let metadata = entry.metadata().context("failed to read metadata")?;

        if metadata.is_dir() {
            add_dir_to_tar(builder, &path, base)?;
        } else if metadata.is_file() {
            if relative_str.contains("..") {
                warn!(path = %relative_str, "Skipping path with '..' component");
                continue;
            }

            let mut header = tar::Header::new_gnu();
            header.set_size(metadata.len());
            header.set_mode(0o644);
            header.set_cksum();

            let file = std::fs::File::open(&path)
                .with_context(|| format!("failed to open file: {}", path.display()))?;
            builder
                .append_data(&mut header, relative, file)
                .with_context(|| format!("failed to add file to tar: {}", relative_str))?;
        }
    }

    Ok(())
}

/// Extract a gzip-compressed tar archive to a target directory.
/// (Legacy function — kept for tests.)
pub fn extract_snapshot_archive(archive_data: &[u8], target: &Path) -> Result<()> {
    let decoder = flate2::read::GzDecoder::new(archive_data);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries().context("failed to read tar entries")? {
        let mut entry = entry.context("failed to read tar entry")?;
        let path = entry.path().context("failed to read entry path")?.into_owned();

        if path.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
            anyhow::bail!("path traversal detected in snapshot archive: {}", path.display());
        }

        entry.unpack_in(target).context("failed to extract tar entry")?;
    }

    Ok(())
}

/// Legacy: chunk size for streaming tar data (64KB).
const LEGACY_CHUNK_SIZE: usize = 64 * 1024;

/// Legacy: Send a snapshot over the snapshot QUIC channel (leader side).
///
/// The archive is pre-built in memory and streamed in chunks over the signed channel.
/// Superseded by `send_snapshot_chunked` but kept for backward compatibility.
pub async fn send_snapshot(
    ch: &mut ChannelPair,
    secret: &[u8],
    archive: &[u8],
    metrics: &SnapshotMetrics,
) -> Result<()> {
    info!(archive_size = archive.len(), "Sending snapshot to joining node");

    // Send header: total size as u64 LE
    let size_bytes = (archive.len() as u64).to_le_bytes();
    let seq = ch.send_seq;
    ch.send_seq += 1;
    crate::framing::send_signed(&mut ch.send, seq, &size_bytes, secret).await?;

    // Stream archive in chunks
    let mut offset = 0;
    let mut last_progress_pct = 0u64;

    while offset < archive.len() {
        let end = (offset + LEGACY_CHUNK_SIZE).min(archive.len());
        let chunk = &archive[offset..end];

        let seq = ch.send_seq;
        ch.send_seq += 1;
        crate::framing::send_signed(&mut ch.send, seq, chunk, secret).await?;

        offset = end;
        metrics.bytes_transferred.fetch_add(chunk.len() as u64, Ordering::Relaxed);

        if !archive.is_empty() {
            let pct = (offset as u64 * 100) / archive.len() as u64;
            if pct / 10 > last_progress_pct / 10 {
                info!(progress_pct = pct, bytes_sent = offset, total_bytes = archive.len(), "Snapshot transfer progress");
                last_progress_pct = pct;
            }
        }
    }

    // Send empty frame to signal end-of-stream
    let seq = ch.send_seq;
    ch.send_seq += 1;
    crate::framing::send_signed(&mut ch.send, seq, &[], secret).await?;

    // Wait for ACK
    let (ack_seq, ack_data) = crate::framing::recv_signed(&mut ch.recv, secret).await?;
    if ack_seq <= ch.recv_seq {
        anyhow::bail!("replay detected on snapshot ACK: received seq {ack_seq}, expected > {}", ch.recv_seq);
    }
    ch.recv_seq = ack_seq;

    if ack_data.first() != Some(&SNAPSHOT_ACK) {
        anyhow::bail!("snapshot transfer: unexpected ACK byte: {:?}", ack_data.first());
    }

    info!(total_bytes = archive.len(), "Snapshot transfer complete");
    Ok(())
}

/// Legacy: Request and receive a snapshot from the leader (joiner side).
///
/// Superseded by `receive_snapshot_chunked` but kept for backward compatibility.
pub async fn receive_snapshot(
    ch: &mut ChannelPair,
    secret: &[u8],
    target_path: &Path,
    metrics: &SnapshotMetrics,
) -> Result<()> {
    // Send snapshot request
    let seq = ch.send_seq;
    ch.send_seq += 1;
    crate::framing::send_signed(&mut ch.send, seq, &[SNAPSHOT_REQUEST], secret).await?;

    // Receive header: total size
    let (hdr_seq, hdr_data) = crate::framing::recv_signed(&mut ch.recv, secret).await?;
    if hdr_seq <= ch.recv_seq {
        anyhow::bail!("replay detected on snapshot header: received seq {hdr_seq}, expected > {}", ch.recv_seq);
    }
    ch.recv_seq = hdr_seq;

    let total_size = if hdr_data.len() == 8 {
        u64::from_le_bytes(hdr_data.try_into().unwrap())
    } else {
        0
    };
    info!(total_size, "Receiving snapshot from leader");

    // Receive archive chunks into buffer
    let mut archive = Vec::with_capacity(total_size as usize);
    loop {
        let (chunk_seq, chunk_data) = crate::framing::recv_signed(&mut ch.recv, secret).await?;
        if chunk_seq <= ch.recv_seq {
            anyhow::bail!("replay detected on snapshot chunk: received seq {chunk_seq}, expected > {}", ch.recv_seq);
        }
        ch.recv_seq = chunk_seq;

        if chunk_data.is_empty() {
            break;
        }

        metrics.bytes_transferred.fetch_add(chunk_data.len() as u64, Ordering::Relaxed);
        archive.extend_from_slice(&chunk_data);
    }

    info!(archive_size = archive.len(), "Snapshot received, extracting");

    // Extract to a temporary directory next to target_path
    let parent = target_path.parent().unwrap_or_else(|| Path::new("."));
    let temp_dir = tempfile::tempdir_in(parent)
        .context("failed to create temp directory for snapshot extraction")?;

    extract_snapshot_archive(&archive, temp_dir.path())?;

    // Install snapshot (preserving cluster/ directory)
    install_snapshot(temp_dir.path(), target_path)?;

    info!("Snapshot extracted and installed at {}", target_path.display());

    // Send ACK
    let seq = ch.send_seq;
    ch.send_seq += 1;
    crate::framing::send_signed(&mut ch.send, seq, &[SNAPSHOT_ACK], secret).await?;

    Ok(())
}

/// Legacy: Handle an incoming snapshot request on the leader side.
///
/// Superseded by `handle_snapshot_request_chunked` but kept for backward compatibility.
pub async fn handle_snapshot_request(
    ch: &mut ChannelPair,
    secret: &[u8],
    db_path: &Path,
    metrics: &SnapshotMetrics,
) -> Result<()> {
    let (req_seq, req_data) = crate::framing::recv_signed(&mut ch.recv, secret).await?;
    if req_seq <= ch.recv_seq {
        anyhow::bail!("replay detected on snapshot request: received seq {req_seq}, expected > {}", ch.recv_seq);
    }
    ch.recv_seq = req_seq;

    if req_data.first() != Some(&SNAPSHOT_REQUEST) {
        anyhow::bail!("unexpected snapshot request byte: {:?}", req_data.first());
    }

    info!("Snapshot requested, building archive from {}", db_path.display());
    let archive = tokio::task::block_in_place(|| build_snapshot_archive(db_path))?;
    send_snapshot(ch, secret, &archive, metrics).await?;

    Ok(())
}

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let dest_path = dst.join(entry.file_name());
        if entry.metadata()?.is_dir() {
            copy_dir_recursive(&entry.path(), &dest_path)?;
        } else {
            std::fs::copy(entry.path(), &dest_path)?;
        }
    }
    Ok(())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_and_extract_snapshot() {
        let src_dir = tempfile::tempdir().unwrap();

        std::fs::write(src_dir.path().join("data.mdb"), b"fake lmdb data").unwrap();
        std::fs::write(src_dir.path().join("data.mdb-lock"), b"lock").unwrap();
        std::fs::create_dir(src_dir.path().join("indexes")).unwrap();
        std::fs::write(src_dir.path().join("indexes/test.idx"), b"index data").unwrap();
        std::fs::create_dir(src_dir.path().join("cluster")).unwrap();
        std::fs::write(src_dir.path().join("cluster/raft.db"), b"raft state").unwrap();

        let archive = build_snapshot_archive(src_dir.path()).unwrap();
        assert!(!archive.is_empty());

        let dst_dir = tempfile::tempdir().unwrap();
        extract_snapshot_archive(&archive, dst_dir.path()).unwrap();

        assert!(dst_dir.path().join("data.mdb").exists());
        assert!(dst_dir.path().join("indexes/test.idx").exists());
        assert!(!dst_dir.path().join("data.mdb-lock").exists());
        assert!(!dst_dir.path().join("cluster").exists());
        assert_eq!(
            std::fs::read_to_string(dst_dir.path().join("data.mdb")).unwrap(),
            "fake lmdb data"
        );
    }

    #[test]
    fn test_copy_dir_recursive() {
        let src = tempfile::tempdir().unwrap();
        std::fs::create_dir(src.path().join("sub")).unwrap();
        std::fs::write(src.path().join("file.txt"), b"hello").unwrap();
        std::fs::write(src.path().join("sub/nested.txt"), b"world").unwrap();

        let dst = tempfile::tempdir().unwrap();
        let dst_path = dst.path().join("copy");
        copy_dir_recursive(src.path(), &dst_path).unwrap();

        assert_eq!(std::fs::read_to_string(dst_path.join("file.txt")).unwrap(), "hello");
        assert_eq!(std::fs::read_to_string(dst_path.join("sub/nested.txt")).unwrap(), "world");
    }

    #[test]
    fn test_empty_directory_snapshot() {
        let src_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(src_dir.path().join("cluster")).unwrap();

        let archive = build_snapshot_archive(src_dir.path()).unwrap();
        assert!(!archive.is_empty());

        let dst_dir = tempfile::tempdir().unwrap();
        extract_snapshot_archive(&archive, dst_dir.path()).unwrap();
    }

    #[test]
    fn test_build_manifest() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("VERSION"), b"1.14.0").unwrap();
        std::fs::create_dir_all(dir.path().join("tasks")).unwrap();
        std::fs::write(dir.path().join("tasks/data.mdb"), vec![0u8; 4 * 1024 * 1024 + 100]).unwrap();
        std::fs::create_dir_all(dir.path().join("indexes/abc")).unwrap();
        std::fs::write(dir.path().join("indexes/abc/data.mdb"), vec![0u8; 8 * 1024 * 1024]).unwrap();

        let manifest = build_manifest(dir.path()).unwrap();

        assert_eq!(manifest.files.len(), 3); // VERSION + tasks/data.mdb + indexes/abc/data.mdb
        assert_eq!(manifest.chunk_size, CHUNK_SIZE as u64);

        // VERSION file: 6 bytes → 1 chunk
        let version_file = manifest.files.iter().find(|f| f.path == "VERSION").unwrap();
        assert_eq!(version_file.size, 6);
        assert_eq!(version_file.chunk_count, 1);

        // tasks/data.mdb: 4MB + 100 bytes → 2 chunks
        let tasks_file = manifest.files.iter().find(|f| f.path == "tasks/data.mdb").unwrap();
        assert_eq!(tasks_file.size, 4 * 1024 * 1024 + 100);
        assert_eq!(tasks_file.chunk_count, 2);

        // indexes/abc/data.mdb: 8MB → exactly 2 chunks
        let index_file = manifest.files.iter().find(|f| f.path == "indexes/abc/data.mdb").unwrap();
        assert_eq!(index_file.size, 8 * 1024 * 1024);
        assert_eq!(index_file.chunk_count, 2);

        assert_eq!(manifest.total_chunks, 5);
    }

    #[test]
    fn test_chunk_header_roundtrip() {
        let header = encode_chunk_header(42, 99, 0xDEADBEEF12345678, 4096);
        let (fi, ci, hash, len) = decode_chunk_header(&header).unwrap();
        assert_eq!(fi, 42);
        assert_eq!(ci, 99);
        assert_eq!(hash, 0xDEADBEEF12345678);
        assert_eq!(len, 4096);
    }

    #[test]
    fn test_xxhash_chunk_integrity() {
        let data = b"Hello, chunked transfer protocol!";
        let hash = xxh64(data, 0);
        assert_ne!(hash, 0);
        // Verify deterministic
        assert_eq!(hash, xxh64(data, 0));
        // Verify different data → different hash
        assert_ne!(hash, xxh64(b"Different data", 0));
    }

    #[test]
    fn test_read_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.bin");

        // Create a file with 2.5 chunks worth of data
        let data_size = CHUNK_SIZE * 2 + CHUNK_SIZE / 2;
        let data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();
        std::fs::write(&file_path, &data).unwrap();

        // Read first chunk
        let chunk0 = read_chunk(&file_path, 0, CHUNK_SIZE).unwrap();
        assert_eq!(chunk0.len(), CHUNK_SIZE);
        assert_eq!(&chunk0[..], &data[..CHUNK_SIZE]);

        // Read second chunk
        let chunk1 = read_chunk(&file_path, 1, CHUNK_SIZE).unwrap();
        assert_eq!(chunk1.len(), CHUNK_SIZE);
        assert_eq!(&chunk1[..], &data[CHUNK_SIZE..2 * CHUNK_SIZE]);

        // Read last (partial) chunk
        let chunk2 = read_chunk(&file_path, 2, CHUNK_SIZE).unwrap();
        assert_eq!(chunk2.len(), CHUNK_SIZE / 2);
        assert_eq!(&chunk2[..], &data[2 * CHUNK_SIZE..]);
    }

    #[test]
    fn test_find_missing_chunks() {
        let mut received = vec![
            vec![true, true, false], // file 0: chunk 2 missing
            vec![true, true],        // file 1: all received
            vec![false, true],       // file 2: chunk 0 missing
        ];

        let missing = find_missing_chunks(&received);
        assert_eq!(missing.len(), 2);
        assert_eq!(missing[0].file_index, 0);
        assert_eq!(missing[0].chunk_index, 2);
        assert_eq!(missing[1].file_index, 2);
        assert_eq!(missing[1].chunk_index, 0);

        // Mark them as received
        received[0][2] = true;
        received[2][0] = true;
        let missing = find_missing_chunks(&received);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_install_snapshot_preserves_cluster() {
        let target = tempfile::tempdir().unwrap();
        let snapshot = tempfile::tempdir().unwrap();

        // Set up target with cluster dir and some data
        std::fs::create_dir(target.path().join("cluster")).unwrap();
        std::fs::write(target.path().join("cluster/raft.db"), b"my raft state").unwrap();
        std::fs::write(target.path().join("old_data"), b"old").unwrap();

        // Set up snapshot with new data
        std::fs::write(snapshot.path().join("new_data"), b"new").unwrap();
        std::fs::create_dir_all(snapshot.path().join("indexes/abc")).unwrap();
        std::fs::write(snapshot.path().join("indexes/abc/data.mdb"), b"index").unwrap();

        install_snapshot(snapshot.path(), target.path()).unwrap();

        // Cluster dir preserved
        assert_eq!(
            std::fs::read_to_string(target.path().join("cluster/raft.db")).unwrap(),
            "my raft state"
        );
        // Old data removed
        assert!(!target.path().join("old_data").exists());
        // New data installed
        assert_eq!(
            std::fs::read_to_string(target.path().join("new_data")).unwrap(),
            "new"
        );
        assert_eq!(
            std::fs::read_to_string(target.path().join("indexes/abc/data.mdb")).unwrap(),
            "index"
        );
    }
}

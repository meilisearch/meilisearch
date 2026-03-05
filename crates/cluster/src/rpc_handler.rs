use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use openraft::Raft;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::transport::{ChannelPair, ClusterTransport, Peer};
use crate::types::TypeConfig;

/// Check if an anyhow error chain contains a quinn stream/connection closure.
/// Used to detect clean stream shutdown vs unexpected errors in RPC and DML loops.
fn is_stream_closed(e: &anyhow::Error) -> bool {
    for cause in e.chain() {
        if cause.downcast_ref::<quinn::ReadExactError>().is_some()
            || cause.downcast_ref::<quinn::ClosedStream>().is_some()
            || cause.downcast_ref::<quinn::ConnectionError>().is_some()
        {
            return true;
        }
    }
    false
}

/// Tagged envelope for inbound Raft RPCs so the handler can determine
/// which openraft method to dispatch to.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpc {
    Vote(openraft::raft::VoteRequest<u64>),
    AppendEntries(openraft::raft::AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(openraft::raft::InstallSnapshotRequest<TypeConfig>),
    /// A follower requests the leader to remove it from cluster membership.
    RemoveNode { node_id: u64 },
}

/// Tagged envelope for outbound Raft RPC responses.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpcResponse {
    Vote(Result<openraft::raft::VoteResponse<u64>, openraft::error::RaftError<u64>>),
    AppendEntries(
        Result<openraft::raft::AppendEntriesResponse<u64>, openraft::error::RaftError<u64>>,
    ),
    InstallSnapshot(
        Result<
            openraft::raft::InstallSnapshotResponse<u64>,
            openraft::error::RaftError<u64, openraft::error::InstallSnapshotError>,
        >,
    ),
    /// Result of a RemoveNode request.
    RemoveNode(Result<(), String>),
}

/// Spawn a task that reads inbound Raft RPCs from the peer's raft channel,
/// dispatches to the local Raft instance, and sends responses back.
///
/// This runs on the **acceptor** side using the peer returned by `accept_peer()`.
/// The connector side uses `rpc_raft()` for outbound RPCs — these are different
/// Peer instances backed by different QUIC connections, so there's no conflict.
pub fn spawn_rpc_handler(
    peer: Arc<Peer>,
    raft: Raft<TypeConfig>,
    secret: Vec<u8>,
    peer_label: String,
    transport: Arc<ClusterTransport>,
    accepted_node_id: u64,
) {
    tokio::spawn(async move {
        if let Err(e) = rpc_loop(&peer, &raft, &secret, &transport, accepted_node_id).await {
            warn!(peer = %peer_label, error = %e, "Raft RPC handler exited");
        } else {
            debug!(peer = %peer_label, "Raft RPC handler finished (stream closed)");
        }
        // Clean up accepted peer entry on exit
        transport.remove_accepted_peer(accepted_node_id).await;
    });
}

/// Main RPC dispatch loop. Reads requests, dispatches to openraft, sends responses.
///
/// The lock is split into three phases to avoid holding the mutex across async
/// Raft dispatch (which may take significant time for consensus operations):
///   1. Lock → read request → unlock
///   2. Dispatch to openraft (no lock held)
///   3. Lock → write response → unlock
async fn rpc_loop(
    peer: &Peer,
    raft: &Raft<TypeConfig>,
    secret: &[u8],
    transport: &ClusterTransport,
    accepted_node_id: u64,
) -> Result<()> {
    loop {
        // Phase 1: Lock, read, unlock
        let (seq, request) = {
            let ch = &mut *peer.raft.lock().await;
            match read_rpc(ch, secret).await {
                Ok(v) => v,
                Err(e) => {
                    if is_stream_closed(&e) {
                        return Ok(());
                    }
                    return Err(e);
                }
            }
        };

        // Update last-activity timestamp for idle tracking
        transport.touch_accepted_peer(accepted_node_id).await;

        // Phase 2: Dispatch to openraft (no lock held)
        let response = dispatch(raft, request).await;

        // Phase 3: Lock, write response, unlock
        {
            let ch = &mut *peer.raft.lock().await;
            write_rpc(ch, seq, &response, secret).await?;
        }
    }
}

/// Read a single RPC request from the channel.
async fn read_rpc(ch: &mut ChannelPair, secret: &[u8]) -> Result<(u64, RaftRpc)> {
    let (seq, data) = crate::framing::recv_signed(&mut ch.recv, secret).await?;

    // Verify monotonically increasing sequence
    if seq <= ch.recv_seq {
        anyhow::bail!("replay detected: received seq {seq}, expected > {}", ch.recv_seq);
    }
    ch.recv_seq = seq;

    let rpc: RaftRpc = bincode::deserialize(&data).context("failed to deserialize RPC")?;
    Ok((seq, rpc))
}

/// Write an RPC response to the channel.
async fn write_rpc(
    ch: &mut ChannelPair,
    _request_seq: u64,
    response: &RaftRpcResponse,
    secret: &[u8],
) -> Result<()> {
    let data = bincode::serialize(response).context("failed to serialize RPC response")?;
    let seq = ch.send_seq;
    ch.send_seq += 1;
    crate::framing::send_signed(&mut ch.send, seq, &data, secret).await
}

/// Dispatch an RPC request to the appropriate openraft handler.
async fn dispatch(raft: &Raft<TypeConfig>, rpc: RaftRpc) -> RaftRpcResponse {
    match rpc {
        RaftRpc::Vote(req) => {
            debug!("handling inbound Vote RPC");
            RaftRpcResponse::Vote(raft.vote(req).await)
        }
        RaftRpc::AppendEntries(req) => {
            debug!("handling inbound AppendEntries RPC");
            RaftRpcResponse::AppendEntries(raft.append_entries(req).await)
        }
        RaftRpc::InstallSnapshot(req) => {
            debug!("handling inbound InstallSnapshot RPC");
            RaftRpcResponse::InstallSnapshot(raft.install_snapshot(req).await)
        }
        RaftRpc::RemoveNode { node_id } => {
            info!(node_id, "Handling RemoveNode RPC from follower");
            let metrics = raft.metrics().borrow().clone();
            let mut voter_ids: std::collections::BTreeSet<u64> = metrics
                .membership_config
                .membership()
                .voter_ids()
                .collect();
            if !voter_ids.remove(&node_id) {
                return RaftRpcResponse::RemoveNode(Err(format!(
                    "node {node_id} is not a voter"
                )));
            }
            match raft.change_membership(voter_ids, false).await {
                Ok(_) => {
                    info!(node_id, "Successfully removed node from cluster membership");
                    RaftRpcResponse::RemoveNode(Ok(()))
                }
                Err(e) => {
                    warn!(node_id, error = %e, "Failed to remove node from membership");
                    RaftRpcResponse::RemoveNode(Err(e.to_string()))
                }
            }
        }
    }
}

// --- DML channel: streaming out-of-band file transfers ---

/// Header sent before streaming a file's raw bytes on the DML channel.
/// Small and bincode-serialized. The raw file bytes follow immediately after.
#[derive(Debug, Serialize, Deserialize)]
pub struct DmlHeader {
    pub uuid_str: String,
    pub size: u64,
}

/// Response types for DML channel messages.
#[derive(Debug, Serialize, Deserialize)]
pub enum DmlResponse {
    FileTransferAck,
}

/// File fetch request sent on the snapshot channel (repurposed for file serving
/// after the initial join snapshot transfer is complete).
#[derive(Debug, Serialize, Deserialize)]
pub struct FileFetchRequest {
    pub uuid_str: String,
}

/// File fetch response header.
#[derive(Debug, Serialize, Deserialize)]
pub enum FileFetchResponse {
    /// File found — raw chunks follow, then ACK expected.
    Found { size: u64 },
    /// File not found on this node.
    NotFound,
}

/// Chunk size for streaming file transfers (64 KB).
pub const DML_CHUNK_SIZE: usize = 64 * 1024;

/// Spawn a task that reads DML messages (file transfers) from the peer's DML channel.
/// Writes received files to the update file store directory.
pub fn spawn_dml_handler(
    peer: Arc<Peer>,
    update_file_path: PathBuf,
    secret: Vec<u8>,
    peer_label: String,
) {
    tokio::spawn(async move {
        if let Err(e) = dml_loop(&peer, &update_file_path, &secret).await {
            warn!(peer = %peer_label, error = %e, "DML handler exited");
        } else {
            debug!(peer = %peer_label, "DML handler finished (stream closed)");
        }
    });
}

/// DML channel handler loop. Receives streaming file transfers and writes to disk.
///
/// Protocol per transfer:
///   1. Read signed DmlHeader (uuid + size)
///   2. Read `size` raw bytes in chunks from the stream (signed frames)
///   3. Write to tempfile → fsync → rename (crash-safe)
///   4. Send signed ACK
async fn dml_loop(peer: &Peer, update_file_path: &Path, secret: &[u8]) -> Result<()> {
    loop {
        // Phase 1: Lock → read header → unlock
        let header = {
            let ch = &mut *peer.dml.lock().await;
            let (seq, data) = match crate::framing::recv_signed(&mut ch.recv, secret).await {
                Ok(v) => v,
                Err(e) => {
                    if is_stream_closed(&e) {
                        return Ok(());
                    }
                    return Err(e);
                }
            };

            if seq <= ch.recv_seq {
                anyhow::bail!(
                    "replay detected on DML channel: seq {seq}, expected > {}",
                    ch.recv_seq
                );
            }
            ch.recv_seq = seq;

            bincode::deserialize::<DmlHeader>(&data)
                .context("failed to deserialize DML header")?
        };

        let uuid: uuid::Uuid = header.uuid_str.parse().context("invalid UUID in DML header")?;
        let dest = update_file_path.join(uuid.to_string());
        let already_exists = dest.exists();

        if !already_exists {
            info!(%uuid, size = header.size, "Receiving streaming file transfer");
        } else {
            debug!(%uuid, "File already exists, will discard incoming bytes");
        }

        // Bound check: reject files larger than 10 GB to prevent memory exhaustion
        const MAX_DML_FILE_SIZE: u64 = 10 * 1024 * 1024 * 1024;
        if header.size > MAX_DML_FILE_SIZE {
            anyhow::bail!("DML file too large: {} bytes (max {} bytes)", header.size, MAX_DML_FILE_SIZE);
        }

        // Phase 2: Read streaming chunks and write to disk (lock held per chunk read)
        let total = header.size as usize;
        let mut received = 0usize;

        // Create tempfile for writing if needed
        let mut tmp_file = if !already_exists {
            Some(
                tokio::task::block_in_place(|| {
                    tempfile::NamedTempFile::new_in(update_file_path)
                        .with_context(|| format!("failed to create tempfile for {uuid}"))
                })?,
            )
        } else {
            None
        };

        while received < total {
            let chunk = {
                let ch = &mut *peer.dml.lock().await;
                let (seq, data) =
                    crate::framing::recv_signed(&mut ch.recv, secret).await?;
                if seq <= ch.recv_seq {
                    anyhow::bail!(
                        "replay on DML chunk: seq {seq}, expected > {}",
                        ch.recv_seq
                    );
                }
                ch.recv_seq = seq;
                data
            };

            received += chunk.len();

            if let Some(ref mut tmp) = tmp_file {
                tokio::task::block_in_place(|| {
                    use std::io::Write;
                    tmp.write_all(&chunk)
                        .with_context(|| format!("failed to write chunk for {uuid}"))
                })?;
            }
        }

        // Finalize: fsync + rename
        if let Some(tmp) = tmp_file {
            tokio::task::block_in_place(|| {
                tmp.as_file()
                    .sync_all()
                    .with_context(|| format!("failed to fsync file {uuid}"))?;
                tmp.persist(&dest)
                    .with_context(|| format!("failed to persist file {uuid}"))?;
                Ok::<_, anyhow::Error>(())
            })?;
            info!(%uuid, size = total, "File transfer complete, written to disk");
        }

        // Phase 3: Lock → send ACK → unlock
        {
            let ch = &mut *peer.dml.lock().await;
            let ack_data = bincode::serialize(&DmlResponse::FileTransferAck)
                .context("failed to serialize DML ack")?;
            let send_seq = ch.send_seq;
            ch.send_seq += 1;
            crate::framing::send_signed(&mut ch.send, send_seq, &ack_data, secret).await?;
        }
    }
}

// --- File serve handler: content file requests on the snapshot channel ---

/// Spawn a handler that serves content file requests on the snapshot channel.
///
/// Used on the leader (and other nodes) to serve content files to followers
/// catching up via Raft log replay after a restart. The leader retains content
/// files for a configurable period so they're available for catch-up.
pub fn spawn_file_serve_handler(
    peer: Arc<Peer>,
    update_file_path: PathBuf,
    retained_file_path: PathBuf,
    secret: Vec<u8>,
    peer_label: String,
) {
    tokio::spawn(async move {
        if let Err(e) =
            file_serve_loop(&peer, &update_file_path, &retained_file_path, &secret).await
        {
            if !is_stream_closed(&e) {
                warn!(peer = %peer_label, error = %e, "File serve handler exited with error");
            }
        } else {
            debug!(peer = %peer_label, "File serve handler finished (stream closed)");
        }
    });
}

/// File serve loop: reads file fetch requests and serves files from disk.
async fn file_serve_loop(
    peer: &Peer,
    update_file_path: &Path,
    retained_file_path: &Path,
    secret: &[u8],
) -> Result<()> {
    loop {
        // Read file fetch request
        let request = {
            let ch = &mut *peer.snapshot.lock().await;
            let (seq, data) = match crate::framing::recv_signed(&mut ch.recv, secret).await {
                Ok(v) => v,
                Err(e) => {
                    if is_stream_closed(&e) {
                        return Ok(());
                    }
                    return Err(e);
                }
            };
            if seq <= ch.recv_seq {
                anyhow::bail!(
                    "replay detected on file-serve channel: seq {seq}, expected > {}",
                    ch.recv_seq
                );
            }
            ch.recv_seq = seq;
            bincode::deserialize::<FileFetchRequest>(&data)
                .context("failed to deserialize file fetch request")?
        };

        let uuid: uuid::Uuid = request
            .uuid_str
            .parse()
            .context("invalid UUID in file fetch request")?;

        // Look for the file in update_files first, then retained
        let file_path = {
            let primary = update_file_path.join(uuid.to_string());
            if primary.exists() {
                Some(primary)
            } else {
                let retained = retained_file_path.join(uuid.to_string());
                if retained.exists() {
                    Some(retained)
                } else {
                    None
                }
            }
        };

        match file_path {
            Some(path) => {
                let size = tokio::task::block_in_place(|| {
                    std::fs::metadata(&path).map(|m| m.len())
                })
                .with_context(|| format!("failed to stat file {uuid}"))?;
                info!(%uuid, size, "Serving content file to peer");

                // Send Found response + stream chunks (hold lock for entire transfer)
                {
                    let ch = &mut *peer.snapshot.lock().await;
                    let resp = FileFetchResponse::Found { size };
                    let data = bincode::serialize(&resp)?;
                    let seq = ch.send_seq;
                    ch.send_seq += 1;
                    crate::framing::send_signed(&mut ch.send, seq, &data, secret).await?;

                    // Stream file chunks
                    let file = tokio::task::block_in_place(|| std::fs::File::open(&path))?;
                    let mut reader =
                        std::io::BufReader::with_capacity(DML_CHUNK_SIZE, file);
                    loop {
                        let buf = tokio::task::block_in_place(|| {
                            use std::io::Read;
                            let mut buf = vec![0u8; DML_CHUNK_SIZE];
                            let n = reader.read(&mut buf)?;
                            buf.truncate(n);
                            Ok::<_, std::io::Error>(buf)
                        })?;
                        if buf.is_empty() {
                            break;
                        }
                        let seq = ch.send_seq;
                        ch.send_seq += 1;
                        crate::framing::send_signed(&mut ch.send, seq, &buf, secret).await?;
                    }
                }

                // Read ACK
                {
                    let ch = &mut *peer.snapshot.lock().await;
                    let (ack_seq, _) =
                        crate::framing::recv_signed(&mut ch.recv, secret).await?;
                    if ack_seq <= ch.recv_seq {
                        anyhow::bail!("replay detected on file-serve ACK");
                    }
                    ch.recv_seq = ack_seq;
                }
            }
            None => {
                warn!(%uuid, "Content file not found for peer request");
                let ch = &mut *peer.snapshot.lock().await;
                let resp = FileFetchResponse::NotFound;
                let data = bincode::serialize(&resp)?;
                let seq = ch.send_seq;
                ch.send_seq += 1;
                crate::framing::send_signed(&mut ch.send, seq, &data, secret).await?;
            }
        }
    }
}

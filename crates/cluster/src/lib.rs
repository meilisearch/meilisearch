pub mod framing;
pub mod lmdb_store;
pub mod messages;
pub mod raft_network;
pub mod rpc_handler;
pub mod snapshot;
pub mod task_applier;
pub mod transport;
pub mod types;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use openraft::error::ClientWriteError;
use openraft::raft::ClientWriteResponse;
use openraft::{BasicNode, Config, Raft};
use rand::distributions::Alphanumeric;
use rand::Rng;
use tracing::{debug, error, info, warn};

use crate::lmdb_store::LmdbRaftStore;
pub use crate::lmdb_store::{
    has_persisted_cluster, load_node_config, save_node_config, validate_raft_log_compatibility,
    NodeConfig,
};
use crate::messages::{JoinRequest, JoinResponse, PeerHandshake};
use crate::raft_network::QuinnNetworkFactory;
use crate::transport::ClusterTransport;

/// Protocol versions supported by this binary.
/// Used during join handshake and peer handshake for auto-upgrade negotiation.
pub const SUPPORTED_PROTOCOLS: &[u32] = &[1];

/// Explicit lifecycle state for a cluster node.
///
/// Provides clear observability (metrics, `/cluster/status`) and correctness
/// (guards against operations in invalid states).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum NodeLifecycle {
    /// `ClusterNode::create()` in progress — bootstrapping a new cluster.
    Bootstrapping = 0,
    /// `ClusterNode::join()` in progress — connecting to an existing cluster.
    Joining = 1,
    /// Joined the cluster but not yet promoted to voter.
    Learner = 2,
    /// Voter, not the current leader.
    Follower = 3,
    /// Voter, elected leader.
    Leader = 4,
    /// Removed from cluster membership (evicted or left).
    Evicted = 5,
    /// Graceful shutdown initiated.
    ShuttingDown = 6,
}

impl NodeLifecycle {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Bootstrapping,
            1 => Self::Joining,
            2 => Self::Learner,
            3 => Self::Follower,
            4 => Self::Leader,
            5 => Self::Evicted,
            6 => Self::ShuttingDown,
            _ => {
                tracing::warn!("unknown lifecycle value: {v}, defaulting to Follower");
                Self::Follower
            }
        }
    }
}

impl std::fmt::Display for NodeLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bootstrapping => write!(f, "bootstrapping"),
            Self::Joining => write!(f, "joining"),
            Self::Learner => write!(f, "learner"),
            Self::Follower => write!(f, "follower"),
            Self::Leader => write!(f, "leader"),
            Self::Evicted => write!(f, "evicted"),
            Self::ShuttingDown => write!(f, "shutting_down"),
        }
    }
}

/// Configurable cluster parameters (threaded from CLI args / env vars).
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub heartbeat_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    /// Timeout for accepting all 3 QUIC streams from a peer (milliseconds).
    pub accept_timeout_ms: u64,
    /// Maximum message size in megabytes (for DML file transfers).
    pub max_message_size_mb: u64,
    /// LMDB map size in megabytes for the Raft state store.
    pub raft_db_size_mb: u64,
    /// Maximum consecutive file transfer failures before evicting a follower.
    pub max_transfer_failures: u32,
    /// Maximum replication lag (in log entries) before evicting a follower.
    /// Set to 0 to disable lag-based eviction.
    pub max_replication_lag: u64,
    /// Timeout in seconds for `client_write` Raft proposals.
    pub write_timeout_secs: u64,
    /// Maximum age (seconds) of the last LMDB compaction before a snapshot
    /// transfer triggers a fresh compaction. Compaction produces smaller files.
    /// - `None` → never compact before snapshot (raw LMDB copy)
    /// - `Some(0)` → always compact before snapshot (smallest transfer)
    /// - `Some(N)` → compact only if last compaction was more than N seconds ago
    ///   Default: `Some(300)` (5 minutes).
    pub snapshot_max_compaction_age_s: Option<u64>,
    /// Enable TLS encryption on QUIC cluster transport.
    /// Derives a self-signed certificate from the cluster secret.
    pub tls: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: 500,
            election_timeout_min_ms: 1500,
            election_timeout_max_ms: 3000,
            accept_timeout_ms: 10_000,
            max_message_size_mb: 512,
            raft_db_size_mb: 256,
            max_transfer_failures: 3,
            max_replication_lag: 10_000,
            write_timeout_secs: 10,
            snapshot_max_compaction_age_s: Some(300),
            tls: false,
        }
    }
}
use crate::types::{RaftRequest, TypeConfig};

/// Errors from `client_write`, classified for proper HTTP status codes.
///
/// Allows callers to distinguish between:
/// - No leader available (503: forward-to-leader with unknown leader)
/// - Quorum unavailable (503: write stall due to timeout)
/// - Other failures (500: internal error)
#[derive(Debug, thiserror::Error)]
pub enum ClusterWriteError {
    #[error("No leader available in the cluster")]
    NoLeader,
    #[error("This node is not the leader (leader: {leader_id:?})")]
    NotLeader { leader_id: Option<u64> },
    #[error("Cluster quorum unavailable, writes are temporarily stalled")]
    QuorumUnavailable,
    #[error("{0}")]
    Other(#[source] anyhow::Error),
}

/// Encode QUIC and HTTP addresses into a single string for `BasicNode.addr`.
/// Format: `"quic_addr|http_addr"`, e.g. `"127.0.0.1:9000|http://127.0.0.1:7700"`.
fn encode_node_addr(quic_addr: &str, http_addr: &str) -> String {
    format!("{quic_addr}|{http_addr}")
}

/// Extract the HTTP address from a combined `BasicNode.addr`.
/// Falls back to the whole string if no separator is found (backward compat).
fn decode_node_http_addr(combined: &str) -> &str {
    combined.split_once('|').map_or(combined, |(_, http)| http)
}

/// Extract the QUIC socket address from a combined `BasicNode.addr`.
/// Supports both numeric IPs (e.g., "127.0.0.1:7701") and hostnames (e.g., "node1:7701").
pub(crate) fn decode_node_quic_addr(combined: &str) -> Option<SocketAddr> {
    match combined.split_once('|') {
        Some((quic, _)) => quic.parse().ok().or_else(|| {
            // Try DNS resolution for hostname-based addresses (Docker/K8s)
            use std::net::ToSocketAddrs;
            quic.to_socket_addrs().ok().and_then(|mut addrs| addrs.next()).or_else(|| {
                warn!(addr = combined, "Failed to parse or resolve QUIC address");
                None
            })
        }),
        None => {
            warn!(addr = combined, "Node address missing pipe separator");
            None
        }
    }
}

/// A cluster node wrapping openraft + QUIC transport.
pub struct ClusterNode {
    pub raft: Raft<TypeConfig>,
    pub transport: Arc<ClusterTransport>,
    pub node_id: u64,
    /// Retained reference to the LMDB store for post-creation wiring (set_task_applier).
    pub state_machine: LmdbRaftStore,
    /// Path to the update files directory for DML file transfers.
    /// Set after construction via `set_update_file_path()`.
    update_file_path: Arc<std::sync::OnceLock<std::path::PathBuf>>,
    /// Per-follower consecutive file transfer failure counter.
    /// Reset to 0 on any successful transfer. Used for eviction decisions.
    consecutive_transfer_failures: Arc<tokio::sync::RwLock<HashMap<u64, u32>>>,
    /// Maximum consecutive transfer failures before evicting a follower.
    max_transfer_failures: u32,
    /// Counter for file transfer failures (for Prometheus metrics).
    pub file_transfer_failures: Arc<AtomicU64>,
    /// Counter for nodes evicted (for Prometheus metrics).
    pub nodes_evicted: Arc<AtomicU64>,
    /// Maximum replication lag before eviction (0 = disabled).
    max_replication_lag: u64,
    /// Cluster configuration (retained for join validation).
    cluster_config: ClusterConfig,
    /// Explicit lifecycle state, updated at each transition point.
    lifecycle: Arc<AtomicU8>,
    /// Base data directory (db_path). Used for snapshot bootstrap.
    db_path: PathBuf,
    /// Snapshot transfer metrics (bytes transferred).
    pub snapshot_metrics: crate::snapshot::SnapshotMetrics,
    /// Provider for consistent LMDB snapshots. Set after construction via `set_snapshot_provider()`.
    snapshot_provider: Arc<std::sync::OnceLock<Arc<dyn crate::snapshot::SnapshotProvider>>>,
    /// Maps retained content file UUID → estimated Raft log index.
    /// Populated at retain time. Not persisted (handled by fallback timeout).
    /// Used by the GC to determine when all voters have replicated past a file.
    retained_file_log_index: Arc<tokio::sync::RwLock<HashMap<uuid::Uuid, u64>>>,
}

#[allow(clippy::too_many_arguments)]
impl ClusterNode {
    /// Create a new cluster (bootstrap mode).
    /// Returns the node and the cluster key used.
    ///
    /// If `derived_secret` is `Some`, that key is used (deterministic, derived from master key).
    /// If `None`, a random 32-char key is generated.
    ///
    /// `data_path` is the base data directory (e.g., `opt.db_path`). The Raft LMDB
    /// environment is created at `{data_path}/cluster/`.
    ///
    /// `http_addr` is this node's HTTP listen address (e.g., "http://127.0.0.1:7700"),
    /// stored in Raft membership for write-forwarding resolution.
    pub async fn create(
        node_id: u64,
        bind_addr: SocketAddr,
        advertise_addr: String,
        http_addr: String,
        data_path: &Path,
        cluster_config: &ClusterConfig,
        derived_secret: Option<String>,
    ) -> Result<(Self, String)> {
        // Set global max message size for framing validation
        crate::framing::set_max_message_size(
            (cluster_config.max_message_size_mb as usize) * 1024 * 1024,
        );

        // Use the derived secret if provided, otherwise generate a random 32-char key
        let secret: String = derived_secret.unwrap_or_else(|| {
            rand::thread_rng().sample_iter(&Alphanumeric).take(32).map(char::from).collect()
        });

        let transport = Arc::new(
            ClusterTransport::new(
                bind_addr,
                secret.as_bytes().to_vec(),
                Duration::from_millis(cluster_config.accept_timeout_ms),
                cluster_config.tls,
            )
            .await?,
        );

        let network_factory =
            QuinnNetworkFactory { transport: transport.clone(), our_node_id: node_id };

        let config = Config {
            heartbeat_interval: cluster_config.heartbeat_ms,
            election_timeout_min: cluster_config.election_timeout_min_ms,
            election_timeout_max: cluster_config.election_timeout_max_ms,
            ..Default::default()
        };
        let config = Arc::new(config.validate().context("invalid raft config")?);

        let store = crate::lmdb_store::open_raft_store(data_path, cluster_config.raft_db_size_mb)
            .context("failed to open LMDB raft store")?;
        let store_ref = store.clone();

        let raft = Raft::new(node_id, config, network_factory, store.clone(), store)
            .await
            .context("failed to create raft instance")?;

        // Bootstrap single-node membership — store combined QUIC|HTTP addr
        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode { addr: encode_node_addr(&advertise_addr, &http_addr) });
        raft.initialize(members).await.context("failed to initialize single-node cluster")?;

        // Persist node config for restart without CLI args
        crate::lmdb_store::save_node_config(
            data_path,
            &crate::lmdb_store::NodeConfig { node_id, bind_addr, secret: secret.clone() },
        )?;

        info!(
            node_id,
            %bind_addr,
            "Cluster created (single-node bootstrap)"
        );

        // Start as Bootstrapping; the leader watcher will transition to Leader
        // once Raft confirms leadership.
        let lifecycle = Arc::new(AtomicU8::new(NodeLifecycle::Bootstrapping as u8));

        Ok((
            ClusterNode {
                raft,
                transport,
                node_id,
                state_machine: store_ref,
                update_file_path: Arc::new(std::sync::OnceLock::new()),
                consecutive_transfer_failures: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
                max_transfer_failures: cluster_config.max_transfer_failures,
                file_transfer_failures: Arc::new(AtomicU64::new(0)),
                nodes_evicted: Arc::new(AtomicU64::new(0)),
                max_replication_lag: cluster_config.max_replication_lag,
                cluster_config: cluster_config.clone(),
                lifecycle,
                db_path: data_path.to_path_buf(),
                snapshot_metrics: crate::snapshot::SnapshotMetrics::default(),
                snapshot_provider: Arc::new(std::sync::OnceLock::new()),
                retained_file_log_index: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            },
            secret,
        ))
    }

    /// Join an existing cluster using a cluster key.
    /// Connects to the bootstrap node, sends a join handshake, and waits
    /// for the leader to add this node to Raft membership.
    ///
    /// `data_path` is the base data directory (e.g., `opt.db_path`). The Raft LMDB
    /// environment is created at `{data_path}/cluster/`.
    ///
    /// `http_addr` is this node's HTTP listen address (e.g., "http://127.0.0.1:7700"),
    /// included in the join request so the leader stores it in membership for forwarding.
    pub async fn join(
        node_id: u64,
        bind_addr: SocketAddr,
        advertise_addr: String,
        http_addr: String,
        bootstrap_addr: SocketAddr,
        secret: String,
        data_path: &Path,
        cluster_config: &ClusterConfig,
        binary_version: &str,
        compile_features: Vec<String>,
    ) -> Result<Self> {
        crate::framing::set_max_message_size(
            (cluster_config.max_message_size_mb as usize) * 1024 * 1024,
        );

        let secret_bytes = secret.as_bytes().to_vec();
        let transport = Arc::new(
            ClusterTransport::new(
                bind_addr,
                secret_bytes,
                Duration::from_millis(cluster_config.accept_timeout_ms),
                cluster_config.tls,
            )
            .await?,
        );

        // Connect to bootstrap node with a temporary sentinel ID (will be re-registered after handshake).
        // Uses u64::MAX to avoid collision with any real node ID (including the default 0).
        const JOIN_SENTINEL_ID: u64 = u64::MAX;
        let outcome = transport
            .connect_peer(JOIN_SENTINEL_ID, bootstrap_addr, crate::transport::PROTO_JOIN)
            .await
            .with_context(|| format!("failed to connect to bootstrap node at {bootstrap_addr}"))?;
        // Sentinel ID should never collide — this is always a fresh connection.
        debug_assert!(
            matches!(outcome, crate::transport::ConnectOutcome::Connected),
            "join sentinel connect should always be Connected"
        );

        // Send join request and receive response on the raft channel.
        // Use advertise_addr (not bind_addr) — this is the address peers use to connect back.
        let join_req = JoinRequest {
            node_id,
            quic_addr: advertise_addr,
            http_addr,
            binary_version: binary_version.to_string(),
            supported_protocols: SUPPORTED_PROTOCOLS.to_vec(),
            compile_features,
            heartbeat_ms: cluster_config.heartbeat_ms,
            election_timeout_min_ms: cluster_config.election_timeout_min_ms,
            election_timeout_max_ms: cluster_config.election_timeout_max_ms,
            max_message_size_mb: cluster_config.max_message_size_mb,
        };
        let join_resp: JoinResponse = {
            let peer = transport.get_peer(JOIN_SENTINEL_ID).await?;
            let ch = &mut *peer.raft.lock().await;

            // Send request
            let data = bincode::serialize(&join_req).context("failed to serialize JoinRequest")?;
            let seq = ch.send_seq;
            ch.send_seq += 1;
            crate::framing::send_signed(&mut ch.send, seq, &data, transport.secret()).await?;

            // Receive response with replay protection
            let (recv_seq, resp_data) =
                crate::framing::recv_signed(&mut ch.recv, transport.secret()).await?;
            if recv_seq <= ch.recv_seq {
                anyhow::bail!(
                    "replay detected on join response: received seq {recv_seq}, expected > {}",
                    ch.recv_seq
                );
            }
            ch.recv_seq = recv_seq;
            bincode::deserialize(&resp_data).context("failed to deserialize JoinResponse")?
        };

        if !join_resp.success {
            let msg = join_resp.error.unwrap_or_else(|| "unknown error".into());
            error!("Cluster join rejected by leader: {msg}");
            anyhow::bail!("join rejected by leader: {msg}");
        }

        // Use the leader-assigned node ID if we requested auto-assignment (node_id == 0)
        let node_id = if let Some(assigned) = join_resp.assigned_node_id {
            info!(
                requested_node_id = node_id,
                assigned_node_id = assigned,
                "Leader assigned node ID"
            );
            assigned
        } else {
            node_id
        };

        info!(
            node_id,
            leader_id = join_resp.leader_id,
            members = ?join_resp.members,
            "Join accepted by leader"
        );

        // Re-register the join connection under the leader's actual node ID.
        // This outbound connection is used by rpc_raft(leader_id) for our Raft RPCs.
        let leader_id = join_resp.leader_id;
        let leader_peer = transport.get_peer(JOIN_SENTINEL_ID).await?;
        if leader_id != JOIN_SENTINEL_ID {
            transport.register_peer(leader_id, leader_peer.clone()).await;
        }
        transport.remove_peer(JOIN_SENTINEL_ID).await;

        // If the cluster has existing data, request a snapshot from the leader.
        // This transfers the leader's database contents (indexes, documents, auth keys)
        // so the new node starts with the full dataset instead of empty.
        if join_resp.has_data {
            info!("Leader reports existing data — requesting snapshot bootstrap (chunked protocol)");
            let snapshot_metrics = crate::snapshot::SnapshotMetrics::default();
            let ch = &mut *leader_peer.snapshot.lock().await;
            crate::snapshot::receive_snapshot_chunked(
                ch,
                transport.secret(),
                data_path,
                &snapshot_metrics,
            )
            .await
            .context("failed to receive chunked snapshot from leader")?;
            info!("Snapshot bootstrap complete — database populated from leader");
        }

        let network_factory =
            QuinnNetworkFactory { transport: transport.clone(), our_node_id: node_id };

        let config = Config {
            heartbeat_interval: cluster_config.heartbeat_ms,
            election_timeout_min: cluster_config.election_timeout_min_ms,
            election_timeout_max: cluster_config.election_timeout_max_ms,
            ..Default::default()
        };
        let config = Arc::new(config.validate().context("invalid raft config")?);

        let store = crate::lmdb_store::open_raft_store(data_path, cluster_config.raft_db_size_mb)
            .context("failed to open LMDB raft store")?;
        let store_ref = store.clone();

        let raft = Raft::new(node_id, config, network_factory, store.clone(), store)
            .await
            .context("failed to create raft instance")?;

        // Persist node config for restart without CLI args
        crate::lmdb_store::save_node_config(
            data_path,
            &crate::lmdb_store::NodeConfig { node_id, bind_addr, secret },
        )?;

        info!(node_id, %bind_addr, %bootstrap_addr, "Joined cluster");

        // Remove the stale join connection from the outbound peers map.
        // The join connection's snapshot channel was consumed by the snapshot
        // bootstrap and has no file_serve_handler. Keeping it would cause
        // content file fetch requests to go to a dead channel. Removing it
        // forces auto_connect to create a fresh connection that gets proper
        // RPC, DML, and file-serve handlers via handle_raft_peer.
        transport.remove_peer(leader_id).await;

        // Starts as Joining; transition to Learner happens after Raft is created.
        // The leader watcher will further transition to Follower/Leader when promoted.
        let lifecycle = Arc::new(AtomicU8::new(NodeLifecycle::Joining as u8));

        Ok(ClusterNode {
            raft,
            transport,
            node_id,
            state_machine: store_ref,
            update_file_path: Arc::new(std::sync::OnceLock::new()),
            consecutive_transfer_failures: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            max_transfer_failures: cluster_config.max_transfer_failures,
            file_transfer_failures: Arc::new(AtomicU64::new(0)),
            nodes_evicted: Arc::new(AtomicU64::new(0)),
            max_replication_lag: cluster_config.max_replication_lag,
            cluster_config: cluster_config.clone(),
            lifecycle,
            db_path: data_path.to_path_buf(),
            snapshot_metrics: crate::snapshot::SnapshotMetrics::default(),
            snapshot_provider: Arc::new(std::sync::OnceLock::new()),
            retained_file_log_index: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        })
    }

    /// Restart a cluster node from persisted LMDB state.
    ///
    /// Opens the existing LMDB store at `{data_path}/cluster/` and creates a new Raft
    /// instance that resumes from the persisted state (vote, log, last_applied).
    /// Does NOT call `raft.initialize()` — the stored state already has membership.
    ///
    /// The caller must start the accept loop (`spawn_accept_loop`) and reconnect to
    /// peers (which happens automatically via on-demand connections in the network factory).
    pub async fn restart(
        node_id: u64,
        bind_addr: SocketAddr,
        secret: String,
        data_path: &Path,
        cluster_config: &ClusterConfig,
    ) -> Result<Self> {
        crate::framing::set_max_message_size(
            (cluster_config.max_message_size_mb as usize) * 1024 * 1024,
        );

        let transport = Arc::new(
            ClusterTransport::new(
                bind_addr,
                secret.as_bytes().to_vec(),
                Duration::from_millis(cluster_config.accept_timeout_ms),
                cluster_config.tls,
            )
            .await?,
        );

        let network_factory =
            QuinnNetworkFactory { transport: transport.clone(), our_node_id: node_id };

        let config = Config {
            heartbeat_interval: cluster_config.heartbeat_ms,
            election_timeout_min: cluster_config.election_timeout_min_ms,
            election_timeout_max: cluster_config.election_timeout_max_ms,
            ..Default::default()
        };
        let config = Arc::new(config.validate().context("invalid raft config")?);

        let store = crate::lmdb_store::open_raft_store(data_path, cluster_config.raft_db_size_mb)
            .context("failed to open LMDB raft store")?;
        let store_ref = store.clone();

        let raft = Raft::new(node_id, config, network_factory, store.clone(), store)
            .await
            .context("failed to create raft instance")?;

        info!(node_id, %bind_addr, "Cluster node restarted from persisted state");

        // Starts as follower — leader watcher will update if we become leader
        let lifecycle = Arc::new(AtomicU8::new(NodeLifecycle::Follower as u8));

        Ok(ClusterNode {
            raft,
            transport,
            node_id,
            state_machine: store_ref,
            update_file_path: Arc::new(std::sync::OnceLock::new()),
            consecutive_transfer_failures: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            max_transfer_failures: cluster_config.max_transfer_failures,
            file_transfer_failures: Arc::new(AtomicU64::new(0)),
            nodes_evicted: Arc::new(AtomicU64::new(0)),
            max_replication_lag: cluster_config.max_replication_lag,
            cluster_config: cluster_config.clone(),
            lifecycle,
            db_path: data_path.to_path_buf(),
            snapshot_metrics: crate::snapshot::SnapshotMetrics::default(),
            snapshot_provider: Arc::new(std::sync::OnceLock::new()),
            retained_file_log_index: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        })
    }

    /// Connect to a peer for Raft RPCs (non-join connection).
    /// Sends `PROTO_RAFT_RPC` discriminator and a `PeerHandshake`, then performs DML handshake.
    /// If the peer is already connected (race resolved), skips handshakes.
    pub async fn connect_raft_peer(&self, peer_id: u64, addr: SocketAddr) -> Result<()> {
        let outcome =
            self.transport.connect_peer(peer_id, addr, crate::transport::PROTO_RAFT_RPC).await?;

        if matches!(outcome, crate::transport::ConnectOutcome::AlreadyConnected) {
            debug!(peer_id, "Peer already connected, skipping handshakes");
            return Ok(());
        }

        let peer = self.transport.get_peer(peer_id).await?;

        // Send PeerHandshake so the acceptor can register us and store version info
        {
            let handshake = PeerHandshake {
                node_id: self.node_id,
                binary_version: env!("CARGO_PKG_VERSION").to_string(),
                supported_protocols: SUPPORTED_PROTOCOLS.to_vec(),
            };
            let data = bincode::serialize(&handshake)
                .context("failed to serialize PeerHandshake")?;
            let ch = &mut *peer.raft.lock().await;
            let seq = ch.send_seq;
            ch.send_seq += 1;
            crate::framing::send_signed(
                &mut ch.send,
                seq,
                &data,
                self.transport.secret(),
            )
            .await?;
        }

        // Perform DML channel handshake (connector side) with timeout to prevent
        // hanging if the acceptor is stuck or the connection is broken.
        {
            let ch = &mut *peer.dml.lock().await;
            tokio::time::timeout(
                Duration::from_secs(5),
                crate::framing::dml_handshake_connector(ch, self.transport.secret()),
            )
            .await
            .map_err(|_| anyhow::anyhow!("DML handshake timed out connecting to peer {peer_id}"))?
            .with_context(|| format!("DML handshake failed connecting to peer {peer_id}"))?;
        }

        Ok(())
    }

    /// Get the current lifecycle state.
    pub fn lifecycle(&self) -> NodeLifecycle {
        NodeLifecycle::from_u8(self.lifecycle.load(Ordering::Acquire))
    }

    /// Set the lifecycle state.
    fn set_lifecycle(&self, state: NodeLifecycle) {
        let prev = NodeLifecycle::from_u8(self.lifecycle.swap(state as u8, Ordering::Release));
        if prev != state {
            info!(node_id = self.node_id, from = %prev, to = %state, "Lifecycle transition");
        }
    }

    /// Check if this node believes it is the current Raft leader.
    /// Uses local metrics (no network round-trip). For linearizable reads,
    /// use `raft.ensure_linearizable()` directly instead.
    pub fn is_leader(&self) -> bool {
        self.leader_id() == Some(self.node_id)
    }

    /// Get the current leader's node ID, if known.
    pub fn leader_id(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }

    /// Get the current leader's HTTP address from Raft membership.
    /// Returns `None` if no leader or leader's address is unknown.
    pub fn leader_http_addr(&self) -> Option<String> {
        let metrics = self.raft.metrics();
        let borrowed = metrics.borrow();
        let leader_id = borrowed.current_leader?;
        let addr = borrowed
            .membership_config
            .membership()
            .nodes()
            .find(|(id, _)| **id == leader_id)
            .map(|(_, node)| node.addr.clone())?;
        Some(decode_node_http_addr(&addr).to_string())
    }

    /// Get the current Raft term.
    pub fn current_term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    /// Get the last applied log index, if any.
    pub fn last_applied_log(&self) -> Option<u64> {
        self.raft.metrics().borrow().last_applied.map(|id| id.index)
    }

    /// Get the number of members (voters + learners) in the cluster.
    pub fn members_total(&self) -> usize {
        self.raft.metrics().borrow().membership_config.membership().nodes().count()
    }

    /// Get the current voter IDs from Raft membership.
    pub fn voter_ids(&self) -> Vec<u64> {
        self.raft.metrics().borrow().membership_config.membership().voter_ids().collect()
    }

    /// Check if this node has been evicted from the cluster membership.
    pub fn is_evicted(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        let members: BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        !members.is_empty() && !members.contains(&self.node_id)
    }

    /// Get the current cluster protocol version (read from LMDB).
    pub fn cluster_protocol_version(&self) -> u32 {
        self.state_machine.cluster_protocol_version()
    }

    /// Store compile-time features for a specific node in LMDB.
    pub fn store_node_features(&self, node_id: u64, features: &[String]) {
        self.state_machine.store_node_features(node_id, features);
    }

    /// Get compile-time features for all known nodes.
    pub fn all_node_features(&self) -> std::collections::BTreeMap<u64, Vec<String>> {
        self.state_machine.all_node_features()
    }

    /// Store binary version for a specific node in LMDB.
    pub fn store_node_version(&self, node_id: u64, version: &str) {
        self.state_machine.store_node_version(node_id, version);
    }

    /// Get binary versions for all known nodes.
    pub fn all_node_versions(&self) -> std::collections::BTreeMap<u64, String> {
        self.state_machine.all_node_versions()
    }

    /// Store supported protocol versions for a specific node in LMDB.
    pub fn store_node_protocols(&self, node_id: u64, protocols: &[u32]) {
        self.state_machine.store_node_protocols(node_id, protocols);
    }

    /// Get supported protocol versions for all known nodes.
    pub fn all_node_protocols(&self) -> std::collections::BTreeMap<u64, Vec<u32>> {
        self.state_machine.all_node_protocols()
    }

    /// Check if all cluster members support a higher protocol version than the current one,
    /// and if so, propose a `ClusterProtocolUpgrade` through Raft.
    /// Returns `Ok(true)` if an upgrade was proposed.
    pub async fn check_and_propose_protocol_upgrade(&self) -> Result<bool> {
        let current = self.cluster_protocol_version();

        // Get all current member node IDs
        let member_ids: Vec<u64> = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .collect();

        let all_protocols = self.state_machine.all_node_protocols();

        // If any member is missing protocol data, we can't upgrade
        for id in &member_ids {
            if !all_protocols.contains_key(id) {
                return Ok(false);
            }
        }

        // Find the highest protocol version that ALL members support
        let min_max_protocol = member_ids
            .iter()
            .filter_map(|id| all_protocols.get(id))
            .map(|protocols| protocols.iter().copied().max().unwrap_or(1))
            .min()
            .unwrap_or(1);

        if min_max_protocol > current {
            info!(
                current_protocol = current,
                new_protocol = min_max_protocol,
                "All nodes support protocol {min_max_protocol}, proposing upgrade"
            );
            let request = crate::types::RaftRequest::ClusterProtocolUpgrade {
                version: min_max_protocol,
            };
            self.client_write(request).await.map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Compute the intersection of compile-time features across all current cluster members.
    /// Only features present on ALL nodes are included.
    pub fn effective_compile_features(&self) -> Vec<String> {
        let members: Vec<u64> = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .collect();

        if members.is_empty() {
            return Vec::new();
        }

        let all_features = self.state_machine.all_node_features();

        // Start with the first member that has stored features
        let mut intersection: Option<std::collections::BTreeSet<String>> = None;

        for id in &members {
            let node_features = match all_features.get(id) {
                Some(f) => f.clone(),
                None => {
                    tracing::warn!(
                        node_id = id,
                        "No compile features stored for member — skipping in intersection"
                    );
                    continue;
                }
            };
            let feature_set: std::collections::BTreeSet<String> =
                node_features.into_iter().collect();
            intersection = Some(match intersection {
                Some(current) => current.intersection(&feature_set).cloned().collect(),
                None => feature_set,
            });
        }

        intersection.unwrap_or_default().into_iter().collect()
    }

    /// Propose a client write through Raft consensus.
    ///
    /// Wraps the raw openraft `client_write` with a timeout and classifies
    /// the error for clean HTTP status codes:
    /// - `ForwardToLeader`   → `ClusterWriteError::NotLeader` (503, includes leader_id if known)
    /// - Timeout             → `ClusterWriteError::QuorumUnavailable` (503)
    /// - All other errors    → `ClusterWriteError::Other` (500)
    pub async fn client_write(
        &self,
        request: RaftRequest,
    ) -> std::result::Result<ClientWriteResponse<TypeConfig>, ClusterWriteError> {
        let timeout_secs = self.cluster_config.write_timeout_secs;
        match tokio::time::timeout(Duration::from_secs(timeout_secs), self.raft.client_write(request)).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(raft_err)) => {
                // Check for ForwardToLeader (this node is not the leader)
                if let Some(fwd) = raft_err.forward_to_leader() {
                    return Err(ClusterWriteError::NotLeader {
                        leader_id: fwd.leader_id,
                    });
                }
                // Check for ChangeMembershipError in the API error
                if let openraft::error::RaftError::APIError(
                    ClientWriteError::ChangeMembershipError(_),
                ) = &raft_err
                {
                    return Err(ClusterWriteError::Other(anyhow::anyhow!("{raft_err}")));
                }
                // Fatal errors (storage failure, etc.)
                Err(ClusterWriteError::Other(anyhow::anyhow!("{raft_err}")))
            }
            Err(_elapsed) => {
                warn!("client_write timed out after {timeout_secs}s — likely quorum loss");
                Err(ClusterWriteError::QuorumUnavailable)
            }
        }
    }

    /// Set the update file path for DML file transfers.
    /// Called once during startup after the IndexScheduler is created.
    pub fn set_update_file_path(&self, path: std::path::PathBuf) {
        let _ = self.update_file_path.set(path);
    }

    /// Set the snapshot provider for consistent LMDB-based snapshots.
    /// Called after construction to wire the index-scheduler as the provider.
    pub fn set_snapshot_provider(&self, provider: Arc<dyn crate::snapshot::SnapshotProvider>) {
        let _ = self.snapshot_provider.set(provider);
        info!(node_id = self.node_id, "Snapshot provider configured");
    }

    /// Request a missing content file from the current leader.
    ///
    /// Uses the snapshot channel (repurposed for file serving) on the outbound
    /// connection to the leader. If no outbound connection exists, auto-connects.
    pub async fn request_content_file_from_leader(&self, uuid: uuid::Uuid) -> Result<()> {
        let update_path = self
            .update_file_path
            .get()
            .ok_or_else(|| anyhow::anyhow!("update_file_path not set"))?;
        let dest = update_path.join(uuid.to_string());
        if dest.exists() {
            return Ok(()); // Already have it
        }

        // Build a list of peers to try: leader first (if known), then all others.
        // This handles the case where `current_leader` is not yet set (e.g.,
        // during partition recovery when the state machine applies entries
        // before the leadership metric is updated).
        let metrics = self.raft.metrics().borrow().clone();
        let leader_id = metrics.current_leader;
        let peers: Vec<(u64, Option<SocketAddr>)> = metrics
            .membership_config
            .membership()
            .nodes()
            .filter(|(id, _)| **id != self.node_id)
            .map(|(id, node)| (*id, decode_node_quic_addr(&node.addr)))
            .collect();

        if peers.is_empty() {
            anyhow::bail!("no peers available for content file fetch");
        }

        // Sort: leader first (if known), then others
        let mut ordered_peers = Vec::new();
        if let Some(lid) = leader_id {
            if let Some(p) = peers.iter().find(|(id, _)| *id == lid) {
                ordered_peers.push(p.clone());
            }
        }
        for p in &peers {
            if Some(p.0) != leader_id {
                ordered_peers.push(p.clone());
            }
        }

        // Try each peer until one succeeds
        for (peer_id, addr) in &ordered_peers {
            // Auto-connect if needed
            if !self.transport.has_peer(*peer_id).await {
                if let Some(addr) = addr {
                    if let Err(e) = self.connect_raft_peer(*peer_id, *addr).await {
                        warn!(peer_id, error = %e, "Failed to connect for content file fetch, trying next peer");
                        continue;
                    }
                } else {
                    continue;
                }
            }

            info!(%uuid, peer_id, "Requesting missing content file from peer");
            // Timeout the fetch to avoid hanging on a stale/dead connection.
            match tokio::time::timeout(
                Duration::from_secs(10),
                self.transport.fetch_file_from_peer(*peer_id, &uuid.to_string(), &dest),
            ).await {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(e)) => {
                    warn!(peer_id, %uuid, error = %e, "File fetch from peer failed, trying next");
                    continue;
                }
                Err(_) => {
                    warn!(peer_id, %uuid, "File fetch from peer timed out (10s), trying next");
                    // Remove the stale peer so the next attempt reconnects cleanly
                    self.transport.remove_peer(*peer_id).await;
                    continue;
                }
            }
        }

        anyhow::bail!(
            "failed to fetch content file {uuid} from any peer (tried {} peers)",
            ordered_peers.len()
        )
    }

    /// Spawn periodic cleanup of retained content files using three-tier GC:
    ///
    /// 1. **Match-based (leader only)**: If `min_voter_matched >= file_log_index`,
    ///    all voters have replicated past this entry — safe to delete.
    /// 2. **Purge-based (any node)**: If `last_purged_index >= file_log_index`,
    ///    the Raft log was compacted past this entry. Any follower behind the
    ///    purge point gets a full snapshot anyway — safe to delete.
    /// 3. **Time-based fallback**: Files with no recorded log index (e.g., from
    ///    before a restart) are deleted after 1 hour. Safety net.
    ///
    /// All tiers enforce a minimum age of 30 seconds to avoid racing with
    /// in-flight DML transfers.
    pub fn spawn_retained_file_cleanup(self: &Arc<Self>) {
        let node = self.clone();
        tokio::spawn(async move {
            let retained_dir = node.db_path.join("cluster").join("retained");
            let min_age = Duration::from_secs(30);
            let fallback_max_age = Duration::from_secs(3600); // 1 hour

            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;

                let entries = match std::fs::read_dir(&retained_dir) {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                // Snapshot current Raft metrics once per GC cycle
                let metrics = node.raft.metrics().borrow().clone();
                let is_leader = metrics.current_leader == Some(node.node_id);

                // Tier 1: Find minimum matched index across all voters (leader only).
                // ReplicationMetrics is BTreeMap<NodeId, Option<LogId>> where
                // LogId has { leader_id, index }.
                let min_voter_matched: Option<u64> = if is_leader {
                    metrics
                        .replication
                        .as_ref()
                        .and_then(|repl| {
                            repl.values()
                                .filter_map(|opt| opt.as_ref())
                                .map(|log_id| log_id.index)
                                .min()
                        })
                } else {
                    None
                };

                // Tier 2: Last purged log index (any node).
                let last_purged = metrics.purged.map(|id| id.index).unwrap_or(0);

                let file_indices = node.retained_file_log_index.read().await;
                let mut cleaned = 0u32;
                let mut cleaned_uuids = Vec::new();

                for entry in entries.flatten() {
                    let metadata = match entry.metadata() {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    // Minimum age guard: don't delete files younger than 30 seconds
                    let age = metadata
                        .modified()
                        .ok()
                        .and_then(|t| t.elapsed().ok())
                        .unwrap_or_default();
                    if age < min_age {
                        continue;
                    }

                    let file_name = entry.file_name();
                    let file_uuid = file_name
                        .to_str()
                        .and_then(|s| uuid::Uuid::parse_str(s).ok());

                    let log_index = file_uuid.and_then(|u| file_indices.get(&u).copied());

                    let should_delete = match log_index {
                        Some(idx) => {
                            // Tier 1: All voters have replicated past this entry
                            if let Some(min_matched) = min_voter_matched {
                                if min_matched >= idx {
                                    debug!(
                                        ?file_uuid,
                                        file_log_index = idx,
                                        min_voter_matched = min_matched,
                                        "GC tier 1 (match-based): all voters past this entry"
                                    );
                                    true
                                } else {
                                    false
                                }
                            }
                            // Tier 2: Raft log compacted past this entry
                            else if last_purged >= idx {
                                debug!(
                                    ?file_uuid,
                                    file_log_index = idx,
                                    last_purged,
                                    "GC tier 2 (purge-based): log compacted past this entry"
                                );
                                true
                            } else {
                                false
                            }
                        }
                        None => {
                            // Tier 3: No log index recorded — use time-based fallback
                            if age > fallback_max_age {
                                debug!(
                                    ?file_uuid,
                                    age_secs = age.as_secs(),
                                    "GC tier 3 (time-based): fallback max age exceeded"
                                );
                                true
                            } else {
                                false
                            }
                        }
                    };

                    if should_delete {
                        let _ = std::fs::remove_file(entry.path());
                        cleaned += 1;
                        if let Some(u) = file_uuid {
                            cleaned_uuids.push(u);
                        }
                    }
                }

                drop(file_indices);

                // Clean up the log index map for deleted files
                if !cleaned_uuids.is_empty() {
                    let mut file_indices = node.retained_file_log_index.write().await;
                    for u in &cleaned_uuids {
                        file_indices.remove(u);
                    }
                }

                if cleaned > 0 {
                    info!(count = cleaned, "Cleaned up retained content files");
                }
            }
        });
    }

    /// Send a file to all current followers via the DML channel (streaming).
    /// Called by the leader before proposing a TaskEnqueued entry for
    /// DocumentAdditionOrUpdate tasks, so followers have the file on disk
    /// before the Raft entry arrives.
    ///
    /// Streams the file in 64KB chunks to avoid loading the entire file into memory
    /// per-follower. Retries up to 3 times per follower with exponential backoff.
    /// Transfers to all followers run in parallel — one slow follower doesn't block others.
    /// Tracks consecutive failures per follower and evicts followers that exceed
    /// the configured threshold (`max_transfer_failures`, default 3).
    pub async fn send_file_to_followers(
        &self,
        uuid: uuid::Uuid,
        file_path: &std::path::Path,
    ) -> Result<()> {
        let file_size = std::fs::metadata(file_path)
            .with_context(|| format!("failed to stat file {}", file_path.display()))?
            .len();

        let header = Arc::new(crate::rpc_handler::DmlHeader {
            uuid_str: uuid.to_string(),
            size: file_size,
        });
        let file_path = Arc::new(file_path.to_path_buf());

        let metrics = self.raft.metrics().borrow().clone();
        let all_followers: Vec<(u64, String)> = metrics
            .membership_config
            .membership()
            .nodes()
            .filter(|(id, _)| **id != self.node_id)
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect();

        if all_followers.is_empty() {
            debug!(%uuid, "No followers — skipping DML file transfer");
            return Ok(());
        }

        info!(
            %uuid,
            file_size,
            follower_count = all_followers.len(),
            "Sending content file to followers via DML"
        );

        // Skip blocked peers (fault injection) — don't count failures toward eviction.
        // The content file will be retained so the peer can catch up after unblocking.
        let blocked = self.transport.blocked_peers_list().await;
        let mut any_blocked = false;
        let followers: Vec<(u64, String)> = all_followers
            .into_iter()
            .filter(|(id, _)| {
                if blocked.contains(id) {
                    debug!(peer_id = id, "Skipping blocked peer for file transfer (fault injection)");
                    any_blocked = true;
                    false
                } else {
                    true
                }
            })
            .collect();

        if followers.is_empty() && any_blocked {
            // All followers are blocked — retain the file for later catch-up.
            self.retain_content_file(&uuid, file_path.as_ref());
            return Ok(());
        }

        // Launch parallel transfers — each follower gets its own retry loop
        let mut join_set = tokio::task::JoinSet::new();

        // Per-follower transfer timeout: must complete well within write_timeout_secs
        let per_follower_timeout = Duration::from_secs(self.cluster_config.write_timeout_secs / 2)
            .max(Duration::from_secs(5));

        for (id, addr) in followers {
            let transport = self.transport.clone();
            let header = header.clone();
            let file_path = file_path.clone();
            let failures = self.consecutive_transfer_failures.clone();
            let file_transfer_failures = self.file_transfer_failures.clone();
            let node_id = self.node_id;
            let max_transfer_failures = self.max_transfer_failures;
            let raft = self.raft.clone();
            let nodes_evicted = self.nodes_evicted.clone();
            let state_machine = self.state_machine.clone();
            let timeout = per_follower_timeout;

            join_set.spawn(async move {
                // Wrap the entire per-follower transfer in a timeout so a dead
                // node can't block the write path indefinitely.
                match tokio::time::timeout(timeout, async {
                    Self::transfer_file_to_follower(
                        id, &addr, &transport, node_id, &header, &file_path,
                        &failures, &file_transfer_failures, max_transfer_failures,
                        &raft, &nodes_evicted, &state_machine,
                    ).await
                }).await {
                    Ok(()) => {}
                    Err(_elapsed) => {
                        warn!(
                            target_node = id,
                            timeout_secs = timeout.as_secs(),
                            "File transfer timed out — follower may be unreachable"
                        );
                        // Remove stale peer so next attempt reconnects cleanly
                        transport.remove_peer(id).await;
                        Self::handle_transfer_failure(
                            id,
                            &failures,
                            &file_transfer_failures,
                            max_transfer_failures,
                            &raft,
                            &nodes_evicted,
                            Some(&state_machine),
                        )
                        .await;
                    }
                }
            });
        }

        // Wait for all transfers to complete and check for any failures
        let mut any_failure = false;
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                warn!(error = %e, "File transfer task panicked");
                any_failure = true;
            }
        }

        // If any follower missed the transfer, retain the content file so it can
        // be fetched later when the follower catches up via Raft log replay.
        // Check by looking at the consecutive failure counters.
        let has_failures = {
            let map = self.consecutive_transfer_failures.read().await;
            !map.is_empty()
        };
        if any_failure || has_failures || any_blocked {
            self.retain_content_file(&uuid, file_path.as_ref());
        }

        Ok(())
    }

    /// Hard-link (or copy) a content file to the retained directory so it survives
    /// deletion by the scheduler. Retained files are served to followers catching up
    /// via the file serve handler on the snapshot channel.
    fn retain_content_file(&self, uuid: &uuid::Uuid, file_path: &std::path::Path) {
        let retained_dir = self.db_path.join("cluster").join("retained");
        if let Err(e) = std::fs::create_dir_all(&retained_dir) {
            warn!(%uuid, error = %e, "Failed to create retained files directory");
            return;
        }
        let retained_path = retained_dir.join(uuid.to_string());
        if retained_path.exists() {
            return; // Already retained
        }
        match std::fs::hard_link(file_path, &retained_path) {
            Ok(()) => info!(%uuid, "Retained content file for follower catch-up"),
            Err(_) => {
                // Hard link might fail cross-device; fall back to copy
                match std::fs::copy(file_path, &retained_path) {
                    Ok(_) => info!(%uuid, "Copied content file for follower catch-up"),
                    Err(e) => {
                        warn!(%uuid, error = %e, "Failed to retain content file")
                    }
                }
            }
        }

        // Record the estimated Raft log index for GC decisions.
        // Use last_applied + 1 as a conservative estimate: the entry being
        // proposed hasn't been applied yet, so any voter that has matched past
        // this index has already replicated the file.
        let log_index = self
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|id| id.index + 1)
            .unwrap_or(1);
        let file_index = self.retained_file_log_index.clone();
        let uuid_owned = *uuid;
        tokio::spawn(async move {
            file_index.write().await.insert(uuid_owned, log_index);
        });
    }

    /// Transfer a file to a single follower with connect + retry logic.
    /// Extracted from `send_file_to_followers` so the outer loop can wrap it in a timeout.
    #[allow(clippy::too_many_arguments)]
    async fn transfer_file_to_follower(
        id: u64,
        addr: &str,
        transport: &ClusterTransport,
        node_id: u64,
        header: &crate::rpc_handler::DmlHeader,
        file_path: &std::path::Path,
        failures: &tokio::sync::RwLock<HashMap<u64, u32>>,
        file_transfer_failures: &AtomicU64,
        max_transfer_failures: u32,
        raft: &Raft<TypeConfig>,
        nodes_evicted: &AtomicU64,
        state_machine: &LmdbRaftStore,
    ) {
        // Ensure outbound connection exists
        if !transport.has_peer(id).await {
            let quic_addr = decode_node_quic_addr(addr);
            if let Some(quic_addr) = quic_addr {
                if let Err(e) =
                    Self::connect_peer_for_transfer(transport, node_id, id, quic_addr)
                        .await
                {
                    warn!(
                        target_node = id, error = %e,
                        "Failed to connect for file transfer"
                    );
                    Self::handle_transfer_failure(
                        id, failures, file_transfer_failures,
                        max_transfer_failures, raft, nodes_evicted,
                        Some(state_machine),
                    )
                    .await;
                    return;
                }
            } else {
                warn!(
                    target_node = id,
                    "No QUIC address for follower, skipping file transfer"
                );
                return;
            }
        }

        // Retry up to 3 times with exponential backoff
        let retry_delays = [
            Duration::from_millis(100),
            Duration::from_millis(500),
            Duration::from_secs(2),
        ];
        let mut succeeded = false;
        for (attempt, delay) in retry_delays.iter().enumerate() {
            match transport
                .rpc_dml_stream_file(id, header, file_path)
                .await
            {
                Ok(_) => {
                    debug!(target_node = id, "Streamed file to follower via DML");
                    failures.write().await.remove(&id);
                    succeeded = true;
                    break;
                }
                Err(e) => {
                    // Remove stale peer so retry can reconnect
                    transport.remove_peer(id).await;
                    if attempt < retry_delays.len() - 1 {
                        warn!(
                            target_node = id, attempt = attempt + 1, error = %e,
                            "File transfer attempt failed, retrying after {:?}", delay
                        );
                        tokio::time::sleep(*delay).await;
                    } else {
                        warn!(
                            target_node = id, error = %e,
                            "File transfer failed after {} attempts",
                            retry_delays.len()
                        );
                    }
                }
            }
        }

        if !succeeded {
            Self::handle_transfer_failure(
                id, failures, file_transfer_failures,
                max_transfer_failures, raft, nodes_evicted,
                Some(state_machine),
            )
            .await;
        }
    }

    /// Connect to a peer for file transfer (static helper for spawned tasks).
    /// If the peer is already connected (race resolved), skips handshakes.
    async fn connect_peer_for_transfer(
        transport: &ClusterTransport,
        our_node_id: u64,
        peer_id: u64,
        quic_addr: SocketAddr,
    ) -> Result<()> {
        let outcome = transport
            .connect_peer(peer_id, quic_addr, crate::transport::PROTO_RAFT_RPC)
            .await?;

        if matches!(outcome, crate::transport::ConnectOutcome::AlreadyConnected) {
            debug!(peer_id, "Peer already connected for transfer, skipping handshakes");
            return Ok(());
        }

        let peer = transport.get_peer(peer_id).await?;

        // Send PeerHandshake so the acceptor can register us
        {
            let handshake = PeerHandshake {
                node_id: our_node_id,
                binary_version: env!("CARGO_PKG_VERSION").to_string(),
                supported_protocols: SUPPORTED_PROTOCOLS.to_vec(),
            };
            let data = bincode::serialize(&handshake)
                .context("failed to serialize PeerHandshake")?;
            let ch = &mut *peer.raft.lock().await;
            let seq = ch.send_seq;
            ch.send_seq += 1;
            crate::framing::send_signed(
                &mut ch.send,
                seq,
                &data,
                transport.secret(),
            )
            .await?;
        }

        // Perform DML handshake with timeout
        {
            let ch = &mut *peer.dml.lock().await;
            tokio::time::timeout(
                Duration::from_secs(5),
                crate::framing::dml_handshake_connector(ch, transport.secret()),
            )
            .await
            .map_err(|_| anyhow::anyhow!("DML handshake timed out connecting to peer {peer_id}"))?
            .with_context(|| format!("DML handshake failed connecting to peer {peer_id}"))?;
        }

        Ok(())
    }

    /// Handle a file transfer failure: increment counters and evict if threshold exceeded.
    async fn handle_transfer_failure(
        node_id: u64,
        failures: &tokio::sync::RwLock<HashMap<u64, u32>>,
        file_transfer_failures: &AtomicU64,
        max_transfer_failures: u32,
        raft: &Raft<TypeConfig>,
        nodes_evicted: &AtomicU64,
        state_machine: Option<&LmdbRaftStore>,
    ) {
        file_transfer_failures.fetch_add(1, Ordering::Relaxed);

        let count = {
            let mut map = failures.write().await;
            let count = map.entry(node_id).or_insert(0);
            *count += 1;
            *count
        };

        if count >= max_transfer_failures {
            let details = format!(
                "consecutive failures: {count}, threshold: {max_transfer_failures}"
            );
            Self::evict_node_static(
                node_id,
                raft,
                nodes_evicted,
                state_machine,
                "transfer_failure",
                &details,
            )
            .await;
        }
    }

    /// Evict a node from the cluster (static helper for spawned tasks).
    async fn evict_node_static(
        target_node_id: u64,
        raft: &Raft<TypeConfig>,
        nodes_evicted: &AtomicU64,
        state_machine: Option<&LmdbRaftStore>,
        reason: &str,
        details: &str,
    ) {
        let metrics = raft.metrics().borrow().clone();
        let current_members: BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect();

        if !current_members.contains(&target_node_id) {
            return;
        }

        let mut new_members = current_members;
        new_members.remove(&target_node_id);

        if new_members.is_empty() {
            tracing::warn!(
                "Cannot evict node {target_node_id}: would leave cluster with no members"
            );
            return;
        }

        match raft.change_membership(new_members, false).await {
            Ok(_) => {
                nodes_evicted.fetch_add(1, Ordering::Relaxed);
                if let Some(sm) = state_machine {
                    sm.remove_node_features(target_node_id);
                    sm.remove_node_version(target_node_id);
                    sm.remove_node_protocols(target_node_id);
                }
                tracing::error!(
                    target: "meilisearch::cluster::eviction",
                    event = "node_evicted",
                    node_id = target_node_id,
                    reason = reason,
                    details = details,
                    "Node evicted from cluster"
                );
            }
            Err(e) => {
                tracing::error!(
                    target_node_id, error = %e,
                    "Failed to evict node from cluster"
                );
            }
        }
    }

    /// Spawn a background task that watches Raft metrics for leadership changes
    /// and updates the `is_leader` flag + signals `wake_up` accordingly.
    pub fn spawn_leader_watcher(
        &self,
        is_leader: Arc<std::sync::atomic::AtomicBool>,
        wake_up: Arc<synchronoise::SignalEvent>,
    ) {
        let mut rx = self.raft.metrics();
        let raft = self.raft.clone();
        let node_id = self.node_id;
        let lifecycle = self.lifecycle.clone();

        tokio::spawn(async move {
            // Set initial leadership state from current Raft metrics immediately.
            // This closes the startup race where is_leader defaults to true before
            // the cluster join code can set it to false.
            let initial_metrics = rx.borrow_and_update().clone();
            let mut was_leader = initial_metrics.current_leader == Some(node_id);
            is_leader.store(was_leader, std::sync::atomic::Ordering::Release);

            // Update lifecycle from initial metrics
            let initial_lifecycle = Self::derive_lifecycle_from_raft(
                &raft,
                node_id,
                &lifecycle,
            );
            lifecycle.store(initial_lifecycle as u8, Ordering::Release);
            info!(node_id, was_leader, lifecycle = %initial_lifecycle, "Leader watcher started");

            loop {
                if rx.changed().await.is_err() {
                    info!(node_id, "Leader watcher stopping: raft shut down");
                    break;
                }

                let metrics = rx.borrow_and_update().clone();
                let now_leader = metrics.current_leader == Some(node_id);

                // Update lifecycle state from raft metrics
                let new_lifecycle = Self::derive_lifecycle_from_raft(
                    &raft,
                    node_id,
                    &lifecycle,
                );
                let prev = lifecycle.swap(new_lifecycle as u8, Ordering::Release);
                if prev != new_lifecycle as u8 {
                    let prev_state = NodeLifecycle::from_u8(prev);
                    info!(
                        node_id,
                        from = %prev_state,
                        to = %new_lifecycle,
                        "Lifecycle transition"
                    );
                }

                if now_leader != was_leader {
                    info!(
                        node_id,
                        now_leader,
                        current_leader = ?metrics.current_leader,
                        "Leadership changed"
                    );
                    is_leader.store(now_leader, std::sync::atomic::Ordering::Release);
                    // Wake the scheduler so it either starts or stops processing batches.
                    wake_up.signal();
                    was_leader = now_leader;
                }
            }
        });
    }

    /// Derive the lifecycle state from current Raft metrics.
    /// Respects terminal states (ShuttingDown, Evicted) — once in those, don't revert.
    fn derive_lifecycle_from_raft(
        raft: &Raft<TypeConfig>,
        node_id: u64,
        current: &AtomicU8,
    ) -> NodeLifecycle {
        let cur = NodeLifecycle::from_u8(current.load(Ordering::Acquire));
        // Terminal states are sticky
        if matches!(cur, NodeLifecycle::ShuttingDown | NodeLifecycle::Evicted) {
            return cur;
        }

        let metrics = raft.metrics().borrow().clone();

        let voters: BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect();

        // Check if we've been evicted (removed from voters while voters exist)
        if !voters.is_empty() && !voters.contains(&node_id) {
            // Check if we're a learner
            let is_learner = metrics
                .membership_config
                .membership()
                .nodes()
                .any(|(id, _)| *id == node_id);
            if is_learner {
                return NodeLifecycle::Learner;
            }
            return NodeLifecycle::Evicted;
        }

        if metrics.current_leader == Some(node_id) {
            NodeLifecycle::Leader
        } else {
            NodeLifecycle::Follower
        }
    }

    /// Spawn a background task that periodically checks replication lag on the leader
    /// and evicts followers whose lag exceeds `max_replication_lag` entries.
    ///
    /// Also performs connectivity-based eviction: if the leader's Raft RPCs to a
    /// follower have failed consecutively (tracked by the transport layer), the
    /// follower is evicted. This detects killed nodes even without ongoing writes.
    ///
    /// Only active when this node is leader (replication metrics are only available then).
    /// Runs every 5 seconds. Does nothing if `max_replication_lag` is 0 (disabled).
    pub fn spawn_lag_eviction(&self) {
        if self.max_replication_lag == 0 {
            return;
        }

        let raft = self.raft.clone();
        let node_id = self.node_id;
        let max_lag = self.max_replication_lag;
        let nodes_evicted = self.nodes_evicted.clone();
        let state_machine = self.state_machine.clone();
        let transport = self.transport.clone();

        // Evict a follower if no successful Raft RPC has been received for this long.
        // openraft sends heartbeats every ~500ms, so a healthy follower always has a
        // recent success timestamp. Dead followers' RPCs time out (openraft's 500ms
        // timeout cancels the future) and the timestamp goes stale.
        const UNREACHABLE_THRESHOLD: Duration = Duration::from_secs(15);

        info!(
            node_id,
            max_lag,
            unreachable_threshold_secs = UNREACHABLE_THRESHOLD.as_secs(),
            "Starting eviction loop"
        );

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.tick().await; // skip immediate first tick

            loop {
                interval.tick().await;

                let metrics = raft.metrics().borrow().clone();

                // Only check when we are the leader
                if metrics.current_leader != Some(node_id) {
                    debug!(
                        node_id,
                        current_leader = ?metrics.current_leader,
                        "Eviction check: not leader, skipping"
                    );
                    continue;
                }
                debug!(node_id, "Eviction check: running as leader");

                // --- Lag-based eviction ---
                // Skip blocked peers (fault injection) — don't evict test-partitioned nodes.
                let blocked = transport.blocked_peers_list().await;
                let leader_last_applied = metrics.last_applied.map(|id| id.index).unwrap_or(0);

                if let Some(ref replication) = metrics.replication {
                    for (follower_id, matched) in replication {
                        if *follower_id == node_id || blocked.contains(follower_id) {
                            continue;
                        }

                        let follower_matched = matched.map(|log_id| log_id.index).unwrap_or(0);
                        let lag = leader_last_applied.saturating_sub(follower_matched);

                        if lag > max_lag {
                            let details = format!(
                                "lag: {lag}, threshold: {max_lag}, leader_applied: {leader_last_applied}, follower_matched: {follower_matched}"
                            );
                            Self::evict_node_static(
                                *follower_id,
                                &raft,
                                &nodes_evicted,
                                Some(&state_machine),
                                "replication_lag",
                                &details,
                            )
                            .await;
                        }
                    }
                }

                // --- Connectivity-based eviction (last-success timestamp) ---
                let voter_ids: BTreeSet<u64> = metrics
                    .membership_config
                    .membership()
                    .voter_ids()
                    .collect();

                for &vid in &voter_ids {
                    if vid == node_id || blocked.contains(&vid) {
                        continue;
                    }

                    if let Some(last_ok) = transport.rpc_last_success(vid).await {
                        let stale = last_ok.elapsed();
                        if stale > UNREACHABLE_THRESHOLD {
                            info!(
                                node_id = vid,
                                stale_secs = stale.as_secs(),
                                threshold_secs = UNREACHABLE_THRESHOLD.as_secs(),
                                "Evicting unreachable follower (no successful RPC)"
                            );
                            Self::evict_node_static(
                                vid,
                                &raft,
                                &nodes_evicted,
                                Some(&state_machine),
                                "unreachable",
                                &format!("no successful RPC for {}s", stale.as_secs()),
                            )
                            .await;
                        }
                    }
                    // If no last_rpc_success entry exists yet, the peer hasn't had
                    // any successful RPCs — but it may still be connecting. Don't
                    // evict until we've seen at least one success go stale.
                }
            }
        });
    }

    /// Spawn the accept loop that handles incoming peer connections.
    /// Must be called after `create()` or `join()`.
    pub fn spawn_accept_loop(self: &Arc<Self>) {
        let node = self.clone();
        tokio::spawn(async move {
            info!(node_id = node.node_id, "Accept loop started");
            loop {
                match node.transport.accept_peer().await {
                    Ok((peer, proto, remote_addr)) => {
                        let node = node.clone();
                        tokio::spawn(async move {
                            match proto {
                                crate::transport::PROTO_JOIN => {
                                    if let Err(e) = node.handle_join_peer(peer, remote_addr).await {
                                        warn!(error = %e, "Failed to handle joining peer");
                                    }
                                }
                                crate::transport::PROTO_RAFT_RPC => {
                                    if let Err(e) = node.handle_raft_peer(peer).await {
                                        warn!(error = %e, "Failed to handle raft peer");
                                    }
                                }
                                other => {
                                    warn!(proto = other, "Unknown protocol discriminator");
                                }
                            }
                        });
                    }
                    Err(e) => {
                        // Only break on endpoint-closed (no more connections possible).
                        // Connection-level errors (timeouts, bad handshakes) are logged
                        // and the loop continues accepting new connections.
                        let msg = e.to_string();
                        if msg.contains("endpoint") && msg.contains("closed") {
                            info!("Accept loop stopping: endpoint closed");
                            break;
                        }
                        warn!(error = %e, "Accept loop: connection-level error, continuing");
                        continue;
                    }
                }
            }
        });
    }

    /// Spawn a background task that periodically cleans up idle accepted peers.
    /// Runs every 60 seconds, removes peers idle for longer than 5 minutes.
    pub fn spawn_idle_peer_cleanup(self: &Arc<Self>) {
        let transport = self.transport.clone();
        tokio::spawn(async move {
            let check_interval = Duration::from_secs(60);
            let max_idle = Duration::from_secs(300); // 5 minutes
            loop {
                tokio::time::sleep(check_interval).await;
                let removed = transport.cleanup_idle_accepted_peers(max_idle).await;
                if !removed.is_empty() {
                    info!(count = removed.len(), "Cleaned up idle accepted peers");
                }
            }
        });
    }

    /// Handle a PROTO_RAFT_RPC connection: a peer that already joined wants to
    /// exchange Raft RPCs. Read the `PeerHandshake`, register, and spawn the handler.
    async fn handle_raft_peer(self: &Arc<Self>, peer: Arc<crate::transport::Peer>) -> Result<()> {
        // Read the PeerHandshake from the raft channel with replay protection
        let (node_id, handshake) = {
            let ch = &mut *peer.raft.lock().await;
            let (seq, data) =
                crate::framing::recv_signed(&mut ch.recv, self.transport.secret()).await?;
            if seq <= ch.recv_seq {
                anyhow::bail!(
                    "replay detected on raft peer handshake: received seq {seq}, expected > {}",
                    ch.recv_seq
                );
            }
            ch.recv_seq = seq;
            let handshake: PeerHandshake = bincode::deserialize(&data)
                .context("failed to deserialize PeerHandshake")?;
            let node_id = handshake.node_id;
            (node_id, handshake)
        };

        // Store peer's version/protocol info
        self.store_node_version(node_id, &handshake.binary_version);
        self.store_node_protocols(node_id, &handshake.supported_protocols);

        // Reject inbound connections from blocked peers (fault injection).
        if self.transport.is_blocked(node_id).await {
            warn!(node_id, "Rejecting inbound connection from blocked peer (fault injection)");
            anyhow::bail!("peer {node_id} is blocked (fault injection)");
        }

        info!(node_id, "Accepted inbound Raft RPC connection");

        // Register as accepted peer with current timestamp
        self.transport.register_accepted_peer(node_id).await;

        // Perform DML channel handshake before spawning the handler.
        // This validates the peer knows the cluster secret upfront.
        // Timeout prevents hanging on broken connections.
        {
            let ch = &mut *peer.dml.lock().await;
            tokio::time::timeout(
                Duration::from_secs(5),
                crate::framing::dml_handshake_acceptor(ch, self.transport.secret()),
            )
            .await
            .map_err(|_| anyhow::anyhow!("DML handshake timed out for node {node_id}"))?
            .with_context(|| format!("DML handshake failed for node {node_id}"))?;
        }
        debug!(node_id, "DML handshake succeeded");

        // Spawn the RPC handler on the ACCEPTED connection (inbound RPCs only).
        // The accepted peer is NOT registered in the peers map — that map is
        // exclusively for outbound connections used by rpc_raft().
        crate::rpc_handler::spawn_rpc_handler(
            peer.clone(),
            self.raft.clone(),
            self.transport.secret().to_vec(),
            format!("node-{node_id}-inbound"),
            self.transport.clone(),
            node_id,
        );

        // Spawn a DML handler for out-of-band file transfers on the same connection.
        if let Some(path) = self.update_file_path.get() {
            crate::rpc_handler::spawn_dml_handler(
                peer.clone(),
                path.clone(),
                self.transport.secret().to_vec(),
                format!("node-{node_id}-dml"),
            );

            // Spawn a file serve handler on the snapshot channel so reconnecting
            // followers can fetch content files they missed while down.
            let retained_dir = self.db_path.join("cluster").join("retained");
            crate::rpc_handler::spawn_file_serve_handler(
                peer,
                path.clone(),
                retained_dir,
                self.transport.secret().to_vec(),
                format!("node-{node_id}-file-serve"),
            );
        }

        // Check for protocol upgrade opportunity (leader only)
        if self.lifecycle() == NodeLifecycle::Leader {
            if let Err(e) = self.check_and_propose_protocol_upgrade().await {
                debug!("Protocol upgrade check failed: {e}");
            }
        }

        Ok(())
    }

    /// Handle a PROTO_JOIN connection: read join handshake, add as learner, send response,
    /// then connect back and promote to voter in the background.
    ///
    /// The join flow is split into two phases to avoid a deadlock:
    /// 1. **Synchronous:** Add learner (commits with old majority) → send JoinResponse
    /// 2. **Background:** Connect back to joiner → promote to voter (needs joiner's Raft running)
    ///
    /// The joiner can't start its Raft until it receives the JoinResponse, so we must
    /// send the response before attempting to connect back or promote to voter.
    async fn handle_join_peer(
        self: &Arc<Self>,
        peer: Arc<crate::transport::Peer>,
        remote_addr: std::net::SocketAddr,
    ) -> Result<()> {
        // Read join request from the raft channel with replay protection
        let join_req = {
            let ch = &mut *peer.raft.lock().await;
            let (seq, data) =
                crate::framing::recv_signed(&mut ch.recv, self.transport.secret()).await?;
            if seq <= ch.recv_seq {
                anyhow::bail!(
                    "replay detected on join request: received seq {seq}, expected > {}",
                    ch.recv_seq
                );
            }
            ch.recv_seq = seq;
            let req: JoinRequest =
                bincode::deserialize(&data).context("failed to deserialize JoinRequest")?;
            req
        };

        info!(
            joining_node_id = join_req.node_id,
            joining_quic_addr = %join_req.quic_addr,
            joining_http_addr = %join_req.http_addr,
            joining_binary_version = %join_req.binary_version,
            joining_protocols = ?join_req.supported_protocols,
            joining_features = ?join_req.compile_features,
            "Received join request"
        );

        // Validate binary version compatibility:
        // - Major must match
        // - Minor may differ by at most 1 (allows rolling upgrade window)
        // - Patch may differ freely
        if !join_req.binary_version.is_empty() {
            let our_version = env!("CARGO_PKG_VERSION");
            let our_parts: Vec<&str> = our_version.split('.').collect();
            let their_parts: Vec<&str> = join_req.binary_version.split('.').collect();
            let version_rejected = if our_parts.len() >= 2 && their_parts.len() >= 2 {
                if our_parts[0] != their_parts[0] {
                    Some("Major version mismatch")
                } else {
                    let our_minor: u64 = our_parts[1].parse().unwrap_or(0);
                    let their_minor: u64 = their_parts[1].parse().unwrap_or(0);
                    if our_minor.abs_diff(their_minor) > 1 {
                        Some("Minor versions too far apart (max 1 apart for rolling upgrades)")
                    } else {
                        None
                    }
                }
            } else {
                None
            };
            if let Some(reason) = version_rejected {
                let error_msg = format!(
                    "Binary version incompatible: leader is v{our_version}, joining node is v{}. \
                     {reason}.",
                    join_req.binary_version
                );
                warn!("{error_msg}");
                let response = JoinResponse {
                    success: false,
                    leader_id: self.node_id,
                    members: BTreeMap::new(),
                    error: Some(error_msg),
                    cluster_protocol_version: self.cluster_protocol_version(),
                    has_data: false,
                    assigned_node_id: None,
                };
                let ch = &mut *peer.raft.lock().await;
                let data =
                    bincode::serialize(&response).context("failed to serialize JoinResponse")?;
                let seq = ch.send_seq;
                ch.send_seq += 1;
                crate::framing::send_signed(&mut ch.send, seq, &data, self.transport.secret())
                    .await?;
                return Ok(());
            }
        }

        // Validate protocol compatibility (joining node must support current cluster protocol)
        let cluster_proto = self.cluster_protocol_version();
        if !join_req.supported_protocols.is_empty()
            && !join_req.supported_protocols.contains(&cluster_proto)
        {
            let error_msg = format!(
                "Protocol version mismatch: cluster is at protocol v{cluster_proto}, \
                 but joining node supports {:?}",
                join_req.supported_protocols
            );
            warn!("{error_msg}");
            let response = JoinResponse {
                success: false,
                leader_id: self.node_id,
                members: BTreeMap::new(),
                error: Some(error_msg),
                cluster_protocol_version: cluster_proto,
                has_data: false,
                assigned_node_id: None,
            };
            let ch = &mut *peer.raft.lock().await;
            let data =
                bincode::serialize(&response).context("failed to serialize JoinResponse")?;
            let seq = ch.send_seq;
            ch.send_seq += 1;
            crate::framing::send_signed(&mut ch.send, seq, &data, self.transport.secret()).await?;
            return Ok(());
        }

        // Validate cluster config compatibility (non-zero values indicate the joiner sent config)
        {
            let mut mismatches = Vec::new();
            let cfg = &self.cluster_config;
            if join_req.heartbeat_ms != 0 && join_req.heartbeat_ms != cfg.heartbeat_ms {
                mismatches.push(format!(
                    "heartbeat_ms: leader={}, joiner={}",
                    cfg.heartbeat_ms, join_req.heartbeat_ms
                ));
            }
            if join_req.election_timeout_min_ms != 0
                && join_req.election_timeout_min_ms != cfg.election_timeout_min_ms
            {
                mismatches.push(format!(
                    "election_timeout_min_ms: leader={}, joiner={}",
                    cfg.election_timeout_min_ms, join_req.election_timeout_min_ms
                ));
            }
            if join_req.election_timeout_max_ms != 0
                && join_req.election_timeout_max_ms != cfg.election_timeout_max_ms
            {
                mismatches.push(format!(
                    "election_timeout_max_ms: leader={}, joiner={}",
                    cfg.election_timeout_max_ms, join_req.election_timeout_max_ms
                ));
            }
            if join_req.max_message_size_mb != 0
                && join_req.max_message_size_mb != cfg.max_message_size_mb
            {
                mismatches.push(format!(
                    "max_message_size_mb: leader={}, joiner={}",
                    cfg.max_message_size_mb, join_req.max_message_size_mb
                ));
            }
            if !mismatches.is_empty() {
                let error_msg = format!(
                    "Config mismatch: {}. All nodes in a cluster must use the same configuration.",
                    mismatches.join("; ")
                );
                warn!("{error_msg}");
                let response = JoinResponse {
                    success: false,
                    leader_id: self.node_id,
                    members: BTreeMap::new(),
                    error: Some(error_msg),
                    cluster_protocol_version: self.cluster_protocol_version(),
                    has_data: false,
                    assigned_node_id: None,
                };
                let ch = &mut *peer.raft.lock().await;
                let data =
                    bincode::serialize(&response).context("failed to serialize JoinResponse")?;
                let seq = ch.send_seq;
                ch.send_seq += 1;
                crate::framing::send_signed(&mut ch.send, seq, &data, self.transport.secret())
                    .await?;
                return Ok(());
            }
        }

        // Parse QUIC address early (used by add_learner and background connect).
        // Three cases:
        // 1. Numeric IP:port (e.g., "10.0.0.2:7701") — use as-is.
        // 2. Unspecified IP (0.0.0.0 / ::) — replace with remote connection IP.
        // 3. Hostname:port (e.g., "node2:7701") — resolve via DNS.
        let quic_addr: SocketAddr = match join_req.quic_addr.parse::<SocketAddr>() {
            Ok(addr) if addr.ip().is_unspecified() => {
                let fixed = SocketAddr::new(remote_addr.ip(), addr.port());
                info!(
                    advertised = %addr,
                    resolved = %fixed,
                    "Joiner advertised unspecified IP, using remote connection IP instead"
                );
                fixed
            }
            Ok(addr) => addr,
            Err(_) => {
                // Try DNS resolution for hostname-based addresses (Docker/K8s)
                use std::net::ToSocketAddrs;
                join_req.quic_addr.to_socket_addrs()
                    .ok()
                    .and_then(|mut addrs| addrs.next())
                    .with_context(|| format!(
                        "cannot parse or resolve QUIC address from joiner: {}",
                        join_req.quic_addr
                    ))?
            }
        };

        // Auto-assign node ID if the joiner requested it (node_id == 0).
        // Pick max(existing_node_ids) + 1 to guarantee uniqueness.
        let effective_node_id = if join_req.node_id == 0 {
            let metrics = self.raft.metrics().borrow().clone();
            let max_id = metrics
                .membership_config
                .membership()
                .nodes()
                .map(|(id, _)| *id)
                .max()
                .unwrap_or(0);
            let assigned = max_id + 1;
            info!(
                requested_node_id = join_req.node_id,
                assigned_node_id = assigned,
                "Auto-assigning node ID to joiner"
            );
            assigned
        } else {
            join_req.node_id
        };
        let assigned = if join_req.node_id == 0 { Some(effective_node_id) } else { None };

        // Phase 1: Add as learner only (non-blocking, commits with old majority)
        let response = match self
            .add_learner_to_cluster(effective_node_id, quic_addr, &join_req.http_addr)
            .await
        {
            Ok(members) => {
                // Store the joining node's compile-time features
                if !join_req.compile_features.is_empty() {
                    self.store_node_features(effective_node_id, &join_req.compile_features);
                    info!(
                        node_id = effective_node_id,
                        features = ?join_req.compile_features,
                        "Stored compile-time features for joining node"
                    );
                }
                // Store the joining node's version and protocol info
                if !join_req.binary_version.is_empty() {
                    self.store_node_version(effective_node_id, &join_req.binary_version);
                }
                if !join_req.supported_protocols.is_empty() {
                    self.store_node_protocols(effective_node_id, &join_req.supported_protocols);
                }
                // Detect if the cluster has meaningful data by checking for index directories on disk.
                // This is more reliable than counting Raft log entries.
                let has_data = std::fs::read_dir(self.db_path.join("indexes"))
                    .map(|entries| {
                        entries
                            .filter_map(|e| e.ok())
                            .any(|e| e.metadata().map(|m| m.is_dir()).unwrap_or(false))
                    })
                    .unwrap_or(false);
                let snapshot_available = self.snapshot_provider.get().is_some();
                info!(
                    joining_node_id = effective_node_id,
                    has_data,
                    snapshot_available,
                    "Join handshake: data check complete"
                );
                JoinResponse {
                    success: true,
                    leader_id: self.node_id,
                    members,
                    error: None,
                    cluster_protocol_version: self.cluster_protocol_version(),
                    has_data,
                    assigned_node_id: assigned,
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to add learner to cluster");
                JoinResponse {
                    success: false,
                    leader_id: self.leader_id().unwrap_or(self.node_id),
                    members: BTreeMap::new(),
                    error: Some(e.to_string()),
                    cluster_protocol_version: self.cluster_protocol_version(),
                    has_data: false,
                    assigned_node_id: None,
                }
            }
        };

        // Send join response immediately so the joiner can start its Raft
        {
            let ch = &mut *peer.raft.lock().await;
            let data = bincode::serialize(&response).context("failed to serialize JoinResponse")?;
            let seq = ch.send_seq;
            ch.send_seq += 1;
            crate::framing::send_signed(&mut ch.send, seq, &data, self.transport.secret()).await?;
        }

        if response.success {
            // Register the joining peer for idle tracking
            self.transport.register_accepted_peer(effective_node_id).await;

            // Spawn the RPC handler on the ACCEPTED connection (inbound RPCs only).
            // The accepted peer is NOT registered in the peers map — that map is
            // exclusively for outbound connections used by rpc_raft().
            crate::rpc_handler::spawn_rpc_handler(
                peer.clone(),
                self.raft.clone(),
                self.transport.secret().to_vec(),
                format!("node-{}-inbound", effective_node_id),
                self.transport.clone(),
                effective_node_id,
            );

            // Spawn a DML handler for out-of-band file transfers on the same connection.
            if let Some(path) = self.update_file_path.get() {
                crate::rpc_handler::spawn_dml_handler(
                    peer.clone(),
                    path.clone(),
                    self.transport.secret().to_vec(),
                    format!("node-{}-dml", effective_node_id),
                );
            }

            // Spawn a snapshot handler on the snapshot channel for data bootstrap.
            // If the joining node requests a snapshot, the leader streams the db_path
            // using the chunked transfer protocol with xxhash64 integrity verification.
            {
                let snapshot_provider = self.snapshot_provider.clone();
                let max_compaction_age_s = self.cluster_config.snapshot_max_compaction_age_s;
                let secret = self.transport.secret().to_vec();
                let metrics = crate::snapshot::SnapshotMetrics::default();
                let joining_id = effective_node_id;
                tokio::spawn(async move {
                    let ch = &mut *peer.snapshot.lock().await;
                    // Wait for a snapshot request with a generous timeout.
                    // If the joiner doesn't request a snapshot (e.g., first join to empty cluster),
                    // this task will time out and exit cleanly.
                    let result = if let Some(provider) = snapshot_provider.get() {
                        // Use the SnapshotProvider for consistent LMDB snapshots
                        tokio::time::timeout(
                            Duration::from_secs(600), // 10 min for large snapshots
                            crate::snapshot::handle_snapshot_request_chunked(
                                ch, &secret, provider.as_ref(), max_compaction_age_s, &metrics,
                            ),
                        )
                        .await
                    } else {
                        // Fallback: no provider wired yet (shouldn't happen in practice)
                        warn!("No SnapshotProvider available, snapshot transfer will fail");
                        tokio::time::timeout(
                            Duration::from_secs(30),
                            async { anyhow::bail!("SnapshotProvider not configured") },
                        )
                        .await
                    };
                    match result {
                        Ok(Ok(())) => {
                            info!(node_id = joining_id, "Snapshot transfer to joiner complete");
                        }
                        Ok(Err(e)) => {
                            warn!(
                                node_id = joining_id,
                                error = %e,
                                "Snapshot transfer to joiner failed"
                            );
                        }
                        Err(_) => {
                            // Timeout — joiner didn't request a snapshot (normal for empty clusters)
                            debug!(node_id = joining_id, "No snapshot requested by joiner (timeout)");
                        }
                    }
                });
            }

            // Phase 2 (background): Connect back to joiner and promote to voter.
            // The joiner needs time to create its Raft instance and start accepting
            // connections, so we retry with backoff.
            let node = self.clone();
            let joining_id = effective_node_id;
            tokio::spawn(async move {
                if let Err(e) = node.connect_and_promote(joining_id, quic_addr).await {
                    warn!(
                        node_id = joining_id,
                        error = %e,
                        "Failed to complete join (connect back + promote to voter)"
                    );
                } else {
                    // After successful join+promotion, check for protocol upgrade
                    if let Err(e) = node.check_and_propose_protocol_upgrade().await {
                        debug!("Protocol upgrade check after join failed: {e}");
                    }
                }
            });
        }

        Ok(())
    }

    /// Background task: connect back to a joining node and promote it to voter.
    /// Retries the connection with exponential backoff since the joiner needs time
    /// to start its accept loop after receiving the JoinResponse.
    async fn connect_and_promote(&self, node_id: u64, addr: SocketAddr) -> Result<()> {
        info!(node_id, %addr, "Connecting back to joiner for promotion");

        // Retry connecting with exponential backoff: 100ms, 200ms, 400ms, ... capped at 2s
        let max_attempts = 20;
        let mut delay = Duration::from_millis(100);
        for attempt in 0..max_attempts {
            if attempt > 0 {
                info!(
                    node_id,
                    %addr,
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    "Retrying connection to joiner"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
            match self.connect_raft_peer(node_id, addr).await {
                Ok(()) => {
                    info!(node_id, %addr, attempt, "Connected back to joiner");
                    break;
                }
                Err(e) if attempt == max_attempts - 1 => {
                    return Err(e.context(format!(
                        "failed to connect to joiner node {node_id} at {addr} after {max_attempts} attempts"
                    )));
                }
                Err(e) => {
                    debug!(
                        node_id,
                        attempt,
                        error = %e,
                        "Connect to joiner failed, will retry"
                    );
                }
            }
        }

        // Promote learner to voter
        self.promote_learner_to_voter(node_id).await?;

        let voter_ids: Vec<u64> = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        info!(node_id, ?voter_ids, "Joiner promoted to voter — join complete");

        Ok(())
    }

    /// Gracefully leave the cluster by removing self from membership, then shut down.
    ///
    /// If this node is the leader, it proposes a membership change removing itself.
    /// If this node is a follower, it forwards the leave request to the leader.
    /// After membership change commits, shuts down the Raft engine and transport.
    ///
    /// Returns `Ok(())` on success. On timeout or error, the caller should fall back
    /// to a hard shutdown.
    pub async fn leave(&self) -> Result<()> {
        self.set_lifecycle(NodeLifecycle::ShuttingDown);
        info!(node_id = self.node_id, "Initiating graceful leave");

        let metrics = self.raft.metrics().borrow().clone();
        let voter_ids: BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect();

        let current_voters: Vec<u64> = voter_ids.iter().copied().collect();
        if !voter_ids.contains(&self.node_id) {
            info!(node_id = self.node_id, "Not a voter, skipping membership change");
        } else if voter_ids.len() <= 1 {
            anyhow::bail!("Cannot leave: last node in cluster (would lose data)");
        } else if self.is_leader() {
            // Leader: directly change membership
            let mut new_ids = voter_ids.clone();
            new_ids.remove(&self.node_id);
            let new_voters: Vec<u64> = new_ids.iter().copied().collect();
            info!(
                node_id = self.node_id,
                ?current_voters,
                ?new_voters,
                "Leader removing self from voters"
            );
            self.raft.change_membership(new_ids, false).await
                .context("leader failed to remove self from membership")?;
            info!(node_id = self.node_id, "Removed self from cluster membership");
        } else {
            // Follower: send RemoveNode RPC to the leader
            let leader_id = self
                .leader_id()
                .ok_or_else(|| anyhow::anyhow!("no known leader to forward leave request"))?;

            info!(
                node_id = self.node_id,
                leader_id,
                ?current_voters,
                "Follower requesting leader to remove us from voters"
            );

            // Auto-connect to leader if not already connected
            if !self.transport.has_peer(leader_id).await {
                let leader_addr = metrics
                    .membership_config
                    .membership()
                    .nodes()
                    .find(|(id, _)| **id == leader_id)
                    .and_then(|(_, node)| decode_node_quic_addr(&node.addr));

                if let Some(addr) = leader_addr {
                    self.connect_raft_peer(leader_id, addr).await?;
                } else {
                    anyhow::bail!("cannot determine leader address for leave request");
                }
            }

            let rpc = crate::rpc_handler::RaftRpc::RemoveNode { node_id: self.node_id };
            let data = bincode::serialize(&rpc).context("failed to serialize RemoveNode RPC")?;
            let resp_data = self.transport.rpc_raft(leader_id, &data).await
                .context("failed to send RemoveNode RPC to leader")?;
            let resp: crate::rpc_handler::RaftRpcResponse = bincode::deserialize(&resp_data)
                .context("failed to deserialize RemoveNode response")?;

            match resp {
                crate::rpc_handler::RaftRpcResponse::RemoveNode(Ok(())) => {
                    info!(node_id = self.node_id, "Leader confirmed removal from membership");
                }
                crate::rpc_handler::RaftRpcResponse::RemoveNode(Err(e)) => {
                    anyhow::bail!("Leader refused removal: {e}");
                }
                other => {
                    anyhow::bail!("Unexpected RPC response for RemoveNode: {other:?}");
                }
            }
        }

        // Clean up node features locally. This is intentionally NOT replicated via Raft:
        // the leaving node is being removed from membership, so its features become stale
        // but harmless — effective_compile_features() already filters by current voter IDs,
        // so stale entries don't affect the cluster's feature intersection.
        self.state_machine.remove_node_features(self.node_id);
        self.state_machine.remove_node_version(self.node_id);
        self.state_machine.remove_node_protocols(self.node_id);

        self.raft.shutdown().await.context("raft shutdown failed")?;
        self.transport.shutdown();
        info!(node_id = self.node_id, "Graceful leave complete");
        Ok(())
    }

    /// Gracefully shut down this cluster node without leaving the cluster.
    /// Shuts down the Raft engine (stops heartbeats, elections) and closes
    /// the QUIC transport (which causes the accept loop to exit).
    pub async fn shutdown(&self) -> Result<()> {
        self.set_lifecycle(NodeLifecycle::ShuttingDown);
        info!(node_id = self.node_id, "Shutting down cluster node");
        self.raft.shutdown().await.context("raft shutdown failed")?;
        self.transport.shutdown();
        Ok(())
    }

    /// Add a node as a Raft learner (non-blocking).
    /// Returns the current members map for the JoinResponse.
    /// Does NOT promote to voter — that happens in `connect_and_promote` after
    /// the joiner's Raft is running.
    async fn add_learner_to_cluster(
        &self,
        node_id: u64,
        quic_addr: SocketAddr,
        http_addr: &str,
    ) -> Result<BTreeMap<u64, String>> {
        let node = BasicNode { addr: encode_node_addr(&quic_addr.to_string(), http_addr) };

        // Non-blocking: commits add-learner with old majority, returns immediately
        // without waiting for the learner to catch up on the log.
        self.raft.add_learner(node_id, node, false).await.context("failed to add learner")?;

        // Build members map from current membership for the response
        let metrics = self.raft.metrics().borrow().clone();
        let members: BTreeMap<u64, String> = metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect();

        let member_ids: Vec<u64> = members.keys().copied().collect();
        info!(node_id, ?member_ids, "Added learner to cluster");

        Ok(members)
    }

    /// Promote a learner to voter via change_membership.
    /// The learner must be reachable (outbound connection established) for the
    /// joint consensus protocol to succeed.
    async fn promote_learner_to_voter(&self, node_id: u64) -> Result<()> {
        let metrics = self.raft.metrics().borrow().clone();
        let mut voter_ids: BTreeSet<u64> =
            metrics.membership_config.membership().voter_ids().collect();
        voter_ids.insert(node_id);

        let new_voters: Vec<u64> = voter_ids.iter().copied().collect();
        info!(node_id, ?new_voters, "Promoting learner to voter");

        self.raft
            .change_membership(voter_ids, false)
            .await
            .context("failed to change membership (promote learner to voter)")?;

        info!(node_id, ?new_voters, "Promoted learner to voter");

        Ok(())
    }
}

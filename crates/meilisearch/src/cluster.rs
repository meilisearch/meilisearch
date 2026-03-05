#[cfg(feature = "cluster")]
use std::sync::Arc;

use actix_web::{HttpRequest, HttpResponse};
use meilisearch_types::error::ResponseError;
use tracing::debug;

use crate::extractors::payload::Payload;
use crate::Opt;

/// Role of this node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRole {
    /// No cluster configuration — standalone mode.
    Standalone,
    /// This node is the leader and accepts writes directly.
    Leader,
    /// This node forwards writes to the leader.
    Follower,
}

/// Cluster state shared across all request handlers via `web::Data<ClusterState>`.
#[derive(Clone)]
pub struct ClusterState {
    pub role: ClusterRole,
    pub node_id: String,
    pub leader_url: Option<String>,
    pub peers: Vec<String>,
    pub cluster_secret: Option<String>,
    /// Raft cluster node handle (Phase 3). When Some, role is determined by Raft election.
    #[cfg(feature = "cluster")]
    pub raft_node: Option<Arc<meilisearch_cluster::ClusterNode>>,
    /// Signal to the main loop that the node should gracefully leave and shut down.
    /// The HTTP leave handler notifies; the main loop selects on it alongside Ctrl+C.
    pub leave_notify: std::sync::Arc<tokio::sync::Notify>,
    /// QUIC bind address for this node (set during cluster create/join/restart).
    pub quic_bind_addr: Option<String>,
    /// How the cluster secret was sourced (e.g., "derived from --master-key").
    pub secret_source: Option<String>,
    /// Whether fault-injection test endpoints are enabled (--cluster-enable-test-endpoints).
    pub enable_test_endpoints: bool,
}

impl std::fmt::Debug for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterState")
            .field("role", &self.role)
            .field("node_id", &self.node_id)
            .field("leader_url", &self.leader_url)
            .field("peers", &self.peers)
            .finish()
    }
}

impl ClusterState {
    /// Build a ClusterState from CLI options.
    pub fn from_opts(opts: &Opt) -> Self {
        let role = match opts.cluster_role.as_deref() {
            Some("leader") => ClusterRole::Leader,
            Some("follower") => ClusterRole::Follower,
            _ => ClusterRole::Standalone,
        };

        let node_id = opts.node_id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let peers: Vec<String> = opts
            .cluster_peers
            .as_ref()
            .map(|p| p.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
            .unwrap_or_default();

        let leader_url = if role == ClusterRole::Follower { peers.first().cloned() } else { None };

        // Derive cluster_secret from master_key if not explicitly set
        let cluster_secret = opts.cluster_secret.clone().or_else(|| {
            opts.master_key.as_ref().map(|mk| {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(b"meili-cluster-secret:");
                hasher.update(mk.as_bytes());
                hex_encode(&hasher.finalize())
            })
        });

        if role == ClusterRole::Follower {
            tracing::info!(
                node_id = %node_id,
                leader_url = ?leader_url,
                "Starting as cluster follower"
            );
        } else if role == ClusterRole::Leader {
            tracing::info!(node_id = %node_id, "Starting as cluster leader");
        }

        ClusterState {
            role,
            node_id,
            leader_url,
            peers,
            cluster_secret,
            #[cfg(feature = "cluster")]
            raft_node: None,
            leave_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
            quic_bind_addr: None,
            secret_source: None,
            enable_test_endpoints: opts.cluster_enable_test_endpoints,
        }
    }

    /// Set the Raft cluster node handle.
    #[cfg(feature = "cluster")]
    pub fn set_raft_node(&mut self, node: Arc<meilisearch_cluster::ClusterNode>) {
        self.raft_node = Some(node);
    }

    /// Store cluster join info (QUIC bind address and secret source) for the status endpoint.
    pub fn set_join_info(&mut self, quic_bind_addr: String, secret_source: String) {
        self.quic_bind_addr = Some(quic_bind_addr);
        self.secret_source = Some(secret_source);
    }

    /// Get the current role as a display string.
    /// When Raft is active, derives from Raft election state.
    pub fn current_role(&self) -> String {
        #[cfg(feature = "cluster")]
        if let Some(ref raft_node) = self.raft_node {
            return if raft_node.leader_id() == Some(raft_node.node_id) {
                "leader".to_string()
            } else {
                "follower".to_string()
            };
        }
        match &self.role {
            ClusterRole::Standalone => "standalone",
            ClusterRole::Leader => "leader",
            ClusterRole::Follower => "follower",
        }
        .to_string()
    }

    pub fn is_follower(&self) -> bool {
        // When Raft is active, derive role from Raft election state
        #[cfg(feature = "cluster")]
        if let Some(ref raft_node) = self.raft_node {
            return raft_node.leader_id() != Some(raft_node.node_id);
        }
        // Manual mode (Phase 2) or standalone
        self.role == ClusterRole::Follower
    }

    /// Get the current leader's URL.
    /// When Raft is active, resolves from Raft membership metrics.
    /// Falls back to the statically configured leader_url for manual mode.
    pub fn current_leader_url(&self) -> Option<String> {
        #[cfg(feature = "cluster")]
        if let Some(ref raft_node) = self.raft_node {
            return raft_node.leader_http_addr();
        }
        self.leader_url.clone()
    }

    /// Check if a write request should be redirected to the leader.
    /// Returns `Ok(Some(307 redirect))` if this node is a follower,
    /// `Ok(None)` if this node is the leader (or standalone).
    ///
    /// Uses HTTP 307 Temporary Redirect which preserves the original method
    /// (POST stays POST) per RFC 7538. The client re-sends the request
    /// directly to the leader, eliminating the 2x latency of proxying.
    pub async fn forward_if_follower(
        &self,
        req: &HttpRequest,
        _body: &[u8],
    ) -> Result<Option<HttpResponse>, ResponseError> {
        if !self.is_follower() {
            return Ok(None);
        }

        let leader_url = match self.current_leader_url() {
            Some(url) => url,
            None => {
                return Ok(Some(no_leader_response()));
            }
        };

        let path = req.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or(req.path());
        let location = format!("{}{path}", leader_url.trim_end_matches('/'));

        debug!(
            method = %req.method(),
            path = %req.path(),
            location = %location,
            "Redirecting write request to leader (307)"
        );

        Ok(Some(
            HttpResponse::TemporaryRedirect()
                .insert_header(("Location", location.as_str()))
                .insert_header(("X-Meili-Cluster-Leader", leader_url.as_str()))
                .json(serde_json::json!({
                    "message": "Redirecting to cluster leader",
                    "leaderUrl": leader_url,
                    "location": location,
                })),
        ))
    }

    /// Redirect a streaming write request to the leader via 307.
    /// The payload is not consumed — the client will re-send it directly to the leader.
    pub async fn forward_streaming_if_follower(
        &self,
        req: &HttpRequest,
        _payload: Payload,
    ) -> Result<Option<HttpResponse>, ResponseError> {
        self.forward_if_follower(req, &[]).await
    }
}

fn no_leader_response() -> HttpResponse {
    HttpResponse::ServiceUnavailable()
        .insert_header(("Retry-After", "1"))
        .insert_header(("X-Meili-Cluster-State", "no-leader"))
        .json(serde_json::json!({
            "message": "This node is a cluster follower with no leader configured",
            "code": "cluster_no_leader",
            "type": "system",
            "link": "https://docs.meilisearch.com/errors#cluster_no_leader"
        }))
}

/// Hex-encode bytes. Used for cluster_secret derivation from master key.
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

/// Convert a `ClusterWriteError` to an appropriate `ResponseError`.
#[cfg(feature = "cluster")]
pub fn cluster_write_error_to_response(
    e: meilisearch_cluster::ClusterWriteError,
) -> meilisearch_types::error::ResponseError {
    use meilisearch_types::error::{Code, ResponseError};
    match e {
        meilisearch_cluster::ClusterWriteError::NoLeader
        | meilisearch_cluster::ClusterWriteError::NotLeader { .. } => {
            ResponseError::from_msg(e.to_string(), Code::ClusterNoLeader)
        }
        meilisearch_cluster::ClusterWriteError::QuorumUnavailable => {
            ResponseError::from_msg(e.to_string(), Code::ClusterQuorumUnavailable)
        }
        meilisearch_cluster::ClusterWriteError::Other(ref _inner) => {
            ResponseError::from_msg(e.to_string(), Code::Internal)
        }
    }
}

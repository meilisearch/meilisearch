//! GET /cluster/status — Returns Raft cluster status.
//!
//! Only available when built with the `cluster` feature.

use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use index_scheduler::IndexScheduler;
use meilisearch_types::keys::actions;
use serde::Serialize;
use serde_json::json;
use utoipa::{OpenApi, ToSchema};

use crate::cluster::ClusterState;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;

#[derive(OpenApi)]
#[openapi(
    paths(get_cluster_status),
    tags((
        name = "Cluster",
        description = "Cluster status and management endpoints for Raft-based clusters.",
    )),
)]
pub struct ClusterStatusApi;

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
struct ClusterStatusResponse {
    role: String,
    node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    lifecycle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    raft_node_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    raft_leader_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    leader_url: Option<String>,
    peers: Vec<String>,
    /// Current Raft voter IDs (cluster membership).
    #[serde(skip_serializing_if = "Option::is_none")]
    voters: Option<Vec<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster_protocol_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_batch_uid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_batch_checksum: Option<String>,
    /// Per-node binary version and protocol info (populated from peer handshakes).
    #[serde(skip_serializing_if = "Option::is_none")]
    node_versions: Option<Vec<NodeVersionInfo>>,
    /// Join information for operators (only present on leader nodes with Raft active).
    #[serde(skip_serializing_if = "Option::is_none")]
    join_info: Option<JoinInfo>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
struct NodeVersionInfo {
    node_id: u64,
    binary_version: String,
    supported_protocols: Vec<u32>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
struct JoinInfo {
    quic_addr: String,
    http_addr: String,
    secret_source: String,
    join_command: String,
}

/// Get cluster status
///
/// Returns the current Raft cluster status including role, leader information,
/// protocol version, and the last completed batch for divergence detection.
#[utoipa::path(
    get,
    path = "",
    tag = "Cluster",
    security(("Bearer" = ["experimental_features.get", "experimental_features.*", "*"])),
    responses(
        (status = 200, description = "Cluster status is returned.", body = ClusterStatusResponse, content_type = "application/json"),
        (status = 401, description = "The authorization header is missing.", body = meilisearch_types::error::ResponseError, content_type = "application/json"),
    )
)]
async fn get_cluster_status(
    _index_scheduler: GuardedData<
        ActionPolicy<{ actions::EXPERIMENTAL_FEATURES_GET }>,
        Data<IndexScheduler>,
    >,
    cluster_state: Data<ClusterState>,
    index_scheduler: Data<IndexScheduler>,
) -> HttpResponse {
    // Derive role from Raft state when active, else from static config
    let role = cluster_state.current_role();

    #[cfg(feature = "cluster")]
    let (raft_node_id, raft_leader_id) = {
        if let Some(ref node) = cluster_state.raft_node {
            (Some(node.node_id), node.leader_id())
        } else {
            (None, None)
        }
    };

    #[cfg(not(feature = "cluster"))]
    let (raft_node_id, raft_leader_id): (Option<u64>, Option<u64>) = (None, None);

    #[cfg(feature = "cluster")]
    let cluster_protocol_version = cluster_state
        .raft_node
        .as_ref()
        .map(|node| node.cluster_protocol_version());

    #[cfg(not(feature = "cluster"))]
    let cluster_protocol_version: Option<u32> = None;

    #[cfg(feature = "cluster")]
    let lifecycle = cluster_state
        .raft_node
        .as_ref()
        .map(|node| node.lifecycle().to_string());

    #[cfg(not(feature = "cluster"))]
    let lifecycle: Option<String> = None;

    #[cfg(feature = "cluster")]
    let voters = cluster_state.raft_node.as_ref().map(|node| node.voter_ids());

    #[cfg(not(feature = "cluster"))]
    let voters: Option<Vec<u64>> = None;

    #[cfg(feature = "cluster")]
    let node_versions: Option<Vec<NodeVersionInfo>> = cluster_state.raft_node.as_ref().map(|node| {
        let versions = node.all_node_versions();
        let protocols = node.all_node_protocols();
        let mut infos: Vec<NodeVersionInfo> = versions
            .into_iter()
            .map(|(node_id, binary_version)| NodeVersionInfo {
                node_id,
                binary_version,
                supported_protocols: protocols
                    .get(&node_id)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect();
        infos.sort_by_key(|i| i.node_id);
        infos
    });

    #[cfg(not(feature = "cluster"))]
    let node_versions: Option<Vec<NodeVersionInfo>> = None;

    // Fetch the last completed batch for divergence detection
    let (last_batch_uid, last_batch_checksum) = match index_scheduler.last_completed_batch() {
        Ok(Some(batch)) => (Some(batch.uid), batch.checksum),
        _ => (None, None),
    };

    // Build join_info for leader nodes with Raft active
    let join_info = if role == "leader" {
        if let (Some(quic_addr), Some(secret_source)) =
            (cluster_state.quic_bind_addr.as_ref(), cluster_state.secret_source.as_ref())
        {
            let http_addr = cluster_state
                .current_leader_url()
                .unwrap_or_else(|| format!("http://<unknown>"));
            let join_command = if secret_source.contains("master-key") {
                format!(
                    "meilisearch --cluster-join {quic_addr} \
                     --master-key <same-master-key> \
                     --cluster-bind <host:port>"
                )
            } else {
                format!(
                    "meilisearch --cluster-join {quic_addr} \
                     --cluster-secret <cluster-secret> \
                     --cluster-bind <host:port>"
                )
            };
            Some(JoinInfo {
                quic_addr: quic_addr.clone(),
                http_addr,
                secret_source: secret_source.clone(),
                join_command,
            })
        } else {
            None
        }
    } else {
        None
    };

    HttpResponse::Ok().json(ClusterStatusResponse {
        role,
        node_id: cluster_state.node_id.clone(),
        lifecycle,
        raft_node_id,
        raft_leader_id,
        leader_url: cluster_state.current_leader_url(),
        peers: cluster_state.peers.clone(),
        voters,
        cluster_protocol_version,
        last_batch_uid,
        last_batch_checksum,
        node_versions,
        join_info,
    })
}

/// Graceful leave — remove this node from cluster membership and shut down.
///
/// Only available when a Raft cluster node is active. Returns 200 on success,
/// 404 if no cluster is active, 500 on failure.
#[utoipa::path(
    post,
    path = "/leave",
    tag = "Cluster",
    security(("Bearer" = ["experimental_features.update", "experimental_features.*", "*"])),
    responses(
        (status = 200, description = "Node is leaving the cluster.", content_type = "application/json"),
        (status = 401, description = "The authorization header is missing.", body = meilisearch_types::error::ResponseError, content_type = "application/json"),
        (status = 404, description = "No active cluster.", content_type = "application/json"),
        (status = 409, description = "Cannot leave: last node in cluster.", content_type = "application/json"),
    )
)]
async fn post_cluster_leave(
    _index_scheduler: GuardedData<
        ActionPolicy<{ actions::EXPERIMENTAL_FEATURES_UPDATE }>,
        Data<IndexScheduler>,
    >,
    cluster_state: Data<ClusterState>,
) -> HttpResponse {
    let _ = &cluster_state; // used only with "cluster" feature
    #[cfg(feature = "cluster")]
    {
        if cluster_state.raft_node.is_some() {
            // Signal the main loop to initiate graceful leave + shutdown.
            // The main loop awaits this alongside Ctrl+C, then runs the same
            // clean shutdown path (leave → raft shutdown → transport shutdown).
            cluster_state.leave_notify.notify_one();
            return HttpResponse::Ok().json(serde_json::json!({
                "message": "Node is leaving the cluster"
            }));
        }
    }

    HttpResponse::NotFound().json(serde_json::json!({
        "message": "No active cluster on this node",
        "code": "cluster_not_active"
    }))
}

/// Health check: writer capability.
///
/// Returns 200 if this node can accept writes (leader or standalone), 503 otherwise.
/// Unauthenticated — intended for load balancer health checks.
async fn cluster_health_writer(cluster_state: Data<ClusterState>) -> HttpResponse {
    let role = cluster_state.current_role();
    let can_write = !cluster_state.is_follower();

    if can_write {
        HttpResponse::Ok().json(json!({
            "status": "available",
            "role": role,
            "capability": "writer"
        }))
    } else {
        HttpResponse::ServiceUnavailable().json(json!({
            "status": "unavailable",
            "role": role,
            "capability": "writer"
        }))
    }
}

/// Health check: reader capability.
///
/// Returns 200 if this node can serve reads (any healthy node), 503 if evicted or shutting down.
/// Unauthenticated — intended for load balancer health checks.
async fn cluster_health_reader(cluster_state: Data<ClusterState>) -> HttpResponse {
    let role = cluster_state.current_role();

    // Check if the node is in a healthy state for serving reads.
    // When Raft is active, refuse reads only during shutdown/eviction.
    #[cfg(feature = "cluster")]
    let healthy = {
        if let Some(ref raft_node) = cluster_state.raft_node {
            !matches!(
                raft_node.lifecycle(),
                meilisearch_cluster::NodeLifecycle::ShuttingDown
                    | meilisearch_cluster::NodeLifecycle::Evicted
            )
        } else {
            true // standalone is always healthy for reads
        }
    };

    #[cfg(not(feature = "cluster"))]
    let healthy = true;

    if healthy {
        HttpResponse::Ok().json(json!({
            "status": "available",
            "role": role,
            "capability": "reader"
        }))
    } else {
        HttpResponse::ServiceUnavailable().json(json!({
            "status": "unavailable",
            "role": role,
            "capability": "reader"
        }))
    }
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(get_cluster_status))
    );
    cfg.service(
        web::resource("/leave")
            .route(web::post().to(post_cluster_leave))
    );
}

/// Unauthenticated endpoint returning this node's binary version and supported protocols.
/// Used by operators to check node versions during rolling upgrades.
#[cfg(feature = "cluster")]
async fn cluster_version_info() -> HttpResponse {
    HttpResponse::Ok().json(json!({
        "binaryVersion": env!("CARGO_PKG_VERSION"),
        "supportedProtocols": meilisearch_cluster::SUPPORTED_PROTOCOLS,
    }))
}

/// Configure unauthenticated health endpoints under `/cluster`.
/// Separate from `configure()` because these must live outside the
/// auth-gated `/cluster/status` scope.
pub fn configure_health(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/health/writer")
            .route(web::get().to(cluster_health_writer))
    );
    cfg.service(
        web::resource("/health/reader")
            .route(web::get().to(cluster_health_reader))
    );
    // Version info endpoint (unauthenticated, cluster feature only).
    #[cfg(feature = "cluster")]
    cfg.service(
        web::resource("/version-info")
            .route(web::get().to(cluster_version_info))
    );
    // Fault injection endpoints for partition testing (unauthenticated).
    cfg.service(
        web::resource("/test/block-peer/{peer_id}")
            .route(web::post().to(block_peer_handler))
    );
    cfg.service(
        web::resource("/test/unblock-peer/{peer_id}")
            .route(web::post().to(unblock_peer_handler))
    );
    cfg.service(
        web::resource("/test/blocked-peers")
            .route(web::get().to(blocked_peers_handler))
    );
}

/// Block a peer by ID (fault injection for partition testing).
/// POST /cluster/test/block-peer/{peer_id}
///
/// Requires `--cluster-enable-test-endpoints` to be set. Returns 404 otherwise.
async fn block_peer_handler(
    cluster_state: Data<ClusterState>,
    path: web::Path<u64>,
) -> HttpResponse {
    if !cluster_state.enable_test_endpoints {
        return test_endpoints_disabled_response();
    }
    let _peer_id = path.into_inner();
    #[cfg(feature = "cluster")]
    {
        if let Some(ref raft_node) = cluster_state.raft_node {
            raft_node.transport.block_peer(_peer_id).await;
            return HttpResponse::Ok().json(json!({
                "message": format!("Peer {} blocked", _peer_id),
                "peerId": _peer_id
            }));
        }
    }
    let _ = &cluster_state;
    HttpResponse::NotFound().json(json!({
        "message": "No active cluster on this node",
        "code": "cluster_not_active"
    }))
}

/// Unblock a previously blocked peer (fault injection for partition testing).
/// POST /cluster/test/unblock-peer/{peer_id}
///
/// Requires `--cluster-enable-test-endpoints` to be set. Returns 404 otherwise.
async fn unblock_peer_handler(
    cluster_state: Data<ClusterState>,
    path: web::Path<u64>,
) -> HttpResponse {
    if !cluster_state.enable_test_endpoints {
        return test_endpoints_disabled_response();
    }
    let _peer_id = path.into_inner();
    #[cfg(feature = "cluster")]
    {
        if let Some(ref raft_node) = cluster_state.raft_node {
            raft_node.transport.unblock_peer(_peer_id).await;
            return HttpResponse::Ok().json(json!({
                "message": format!("Peer {} unblocked", _peer_id),
                "peerId": _peer_id
            }));
        }
    }
    let _ = &cluster_state;
    HttpResponse::NotFound().json(json!({
        "message": "No active cluster on this node",
        "code": "cluster_not_active"
    }))
}

/// List currently blocked peers (fault injection for partition testing).
/// GET /cluster/test/blocked-peers
///
/// Requires `--cluster-enable-test-endpoints` to be set. Returns 404 otherwise.
async fn blocked_peers_handler(cluster_state: Data<ClusterState>) -> HttpResponse {
    if !cluster_state.enable_test_endpoints {
        return test_endpoints_disabled_response();
    }
    #[cfg(feature = "cluster")]
    {
        if let Some(ref raft_node) = cluster_state.raft_node {
            let blocked = raft_node.transport.blocked_peers_list().await;
            return HttpResponse::Ok().json(json!({
                "blockedPeers": blocked
            }));
        }
    }
    let _ = &cluster_state;
    HttpResponse::NotFound().json(json!({
        "message": "No active cluster on this node",
        "code": "cluster_not_active"
    }))
}

fn test_endpoints_disabled_response() -> HttpResponse {
    HttpResponse::NotFound().json(json!({
        "message": "Test endpoints are disabled. Start with --cluster-enable-test-endpoints to enable.",
        "code": "test_endpoints_disabled"
    }))
}

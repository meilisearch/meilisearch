use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Join handshake: sent by a joining node on the raft channel before
/// any Raft RPCs. The leader reads this, adds the node to Raft membership,
/// and sends back a JoinResponse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: u64,
    /// QUIC bind address for inter-node transport.
    pub quic_addr: String,
    /// HTTP listen address for client-facing API (used for write forwarding).
    pub http_addr: String,
    /// Binary version string (e.g. "1.37.0"). Leader validates major.minor match.
    #[serde(default)]
    pub binary_version: String,
    /// Cluster protocol versions this node supports (e.g. [1, 2]).
    #[serde(default)]
    pub supported_protocols: Vec<u32>,
    /// Compile-time feature flags (e.g. ["metrics", "chat_completions"]).
    #[serde(default)]
    pub compile_features: Vec<String>,
    /// Heartbeat interval in milliseconds. Validated against leader's config.
    #[serde(default)]
    pub heartbeat_ms: u64,
    /// Minimum election timeout in milliseconds. Validated against leader's config.
    #[serde(default)]
    pub election_timeout_min_ms: u64,
    /// Maximum election timeout in milliseconds. Validated against leader's config.
    #[serde(default)]
    pub election_timeout_max_ms: u64,
    /// Maximum message size in megabytes. Validated against leader's config.
    #[serde(default)]
    pub max_message_size_mb: u64,
}

/// Join handshake response from the leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    pub success: bool,
    pub leader_id: u64,
    pub members: BTreeMap<u64, String>,
    pub error: Option<String>,
    /// Current cluster protocol version (the joining node must support it).
    #[serde(default = "default_protocol_version")]
    pub cluster_protocol_version: u32,
    /// Whether the cluster has existing data (indexes/documents). If true, the joiner
    /// should request a snapshot via the snapshot channel after creating its Raft instance.
    #[serde(default)]
    pub has_data: bool,
    /// Node ID assigned by the leader when the joiner requested auto-assignment (node_id=0).
    /// The joiner MUST use this ID for its Raft instance instead of the requested one.
    #[serde(default)]
    pub assigned_node_id: Option<u64>,
}

fn default_protocol_version() -> u32 {
    1
}

/// Peer handshake: sent as the first signed message on the raft channel
/// when two already-joined nodes connect (non-join connection).
/// Replaces the raw 8-byte node_id with version/protocol metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerHandshake {
    pub node_id: u64,
    /// Binary version string (e.g. "1.37.0").
    pub binary_version: String,
    /// Cluster protocol versions this node supports (e.g. [1, 2]).
    pub supported_protocols: Vec<u32>,
}

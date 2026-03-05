use std::io::Cursor; // Required by openraft::declare_raft_types! macro

use serde::{Deserialize, Serialize};

/// The Raft client request — what goes into the Raft log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRequest {
    /// A task has been enqueued (document add, settings update, etc.)
    /// Document content files are transferred out-of-band via the DML channel
    /// before this entry is proposed, so they're not in the Raft log.
    TaskEnqueued {
        kind_bytes: Vec<u8>, // bincode-encoded KindWithContent
    },
    /// Create or update an API key (serialized Key struct).
    /// Used for both create and update — the state machine calls `raw_insert_key`.
    ApiKeyPut { key_bytes: Vec<u8> },
    /// Delete an API key by UUID (serialized Uuid).
    ApiKeyDelete { uid_bytes: Vec<u8> },
    /// No-op entry (used for leader confirmation after election)
    Noop,
    /// Upgrade the cluster protocol version. Proposed by the leader when all
    /// nodes report support for a higher protocol.
    ClusterProtocolUpgrade { version: u32 },
    /// Set runtime feature toggles (replicated to all nodes).
    /// `features_json` is the JSON-serialized `RuntimeTogglableFeatures`.
    SetRuntimeFeatures { features_json: Vec<u8> },
    /// Set log level on all nodes.
    /// `target` is the tracing target filter (e.g., "info" or "meilisearch=debug").
    SetLogLevel { target: String },
}

/// Response returned after applying a Raft log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftResponse {
    TaskRegistered { task_uid: u32 },
    Ok,
}

// openraft type configuration for Meilisearch cluster.
//
// Uses defaults for NodeId (u64), Node (BasicNode), Entry, SnapshotData, etc.
// Only D (request) and R (response) are customized.
openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
);

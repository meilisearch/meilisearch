/// Boxed future type used by async trait methods that can't use `async fn` in traits.
pub type BoxFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Trait for applying committed Raft entries to the local IndexScheduler.
///
/// The cluster crate's state machine calls this when a `TaskEnqueued` entry
/// is committed. The implementation (in the meilisearch binary) deserializes
/// the `KindWithContent` from bytes and calls `IndexScheduler::register_from_raft()`.
///
/// This avoids a circular dependency: the cluster crate doesn't need to know
/// about `IndexScheduler` or `KindWithContent` — it only passes raw bytes.
///
/// Document content files are transferred out-of-band via the DML channel
/// before the Raft entry is proposed, so they're already on disk by the time
/// `apply_task` is called. On restarting nodes that missed DML transfers,
/// `missing_content_uuid` detects the gap and the state machine fetches the
/// file from a peer before applying.
pub trait TaskApplier: Send + Sync {
    /// Apply a committed task entry to the local task queue.
    ///
    /// Task IDs are NOT pre-assigned — each node auto-assigns from its own LMDB.
    /// Raft's deterministic log order guarantees all nodes assign the same IDs.
    ///
    /// The `raft_log_index` is used for idempotency: if the IndexScheduler has
    /// already applied this log index (tracked in its own LMDB), the call is a
    /// no-op. This prevents duplicate tasks when the cluster LMDB commit fails
    /// after the IndexScheduler commit, causing Raft to replay the entry.
    ///
    /// # Parameters
    /// - `kind_bytes`: bincode-encoded `KindWithContent`
    /// - `raft_log_index`: the Raft log entry index for idempotency
    ///
    /// # Returns
    /// The task UID assigned by the local scheduler (or the existing UID if already applied).
    fn apply_task(
        &self,
        kind_bytes: &[u8],
        raft_log_index: u64,
    ) -> Result<u32, Box<dyn std::error::Error + Send + Sync>>;

    /// Check if a task entry references a content file that is missing on disk.
    ///
    /// Called by the state machine before `apply_task` to detect content files
    /// that were not received via DML (e.g., the node was down during transfer).
    /// Returns `Some(uuid)` if the content file is needed but missing,
    /// `None` if the file is present or the task doesn't need one.
    fn missing_content_uuid(
        &self,
        kind_bytes: &[u8],
    ) -> Option<uuid::Uuid> {
        let _ = kind_bytes;
        None
    }
}

/// Trait for fetching missing content files from cluster peers.
///
/// When a node restarts and catches up via Raft log replay, it may encounter
/// `TaskEnqueued` entries whose content files were transferred via DML while
/// the node was down. The state machine calls this to fetch missing files
/// from the leader (which retains content files for a configurable period)
/// before applying the task entry.
pub trait ContentFileFetcher: Send + Sync {
    /// Fetch a content file by UUID from a cluster peer.
    /// The file should be written to the node's update_files directory.
    ///
    /// Returns `Ok(())` if the file was fetched (or already exists).
    /// Returns `Err` if the file could not be fetched from any peer.
    fn fetch_content_file(
        &self,
        uuid: uuid::Uuid,
    ) -> BoxFuture<'_, Result<(), Box<dyn std::error::Error + Send + Sync>>>;
}

/// Trait for applying committed Raft runtime feature entries to the local IndexScheduler.
///
/// The cluster crate's state machine calls `apply_features` when a `SetRuntimeFeatures`
/// entry is committed. The implementation (in the meilisearch binary) deserializes the
/// JSON-encoded `RuntimeTogglableFeatures` and calls `IndexScheduler::put_runtime_features()`.
pub trait FeatureApplier: Send + Sync {
    /// Apply committed runtime feature changes. `features_json` is a JSON-serialized
    /// `RuntimeTogglableFeatures`.
    fn apply_features(
        &self,
        features_json: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Return the current runtime features as JSON bytes (for snapshot building).
    /// Returns `None` if features are at their defaults.
    fn snapshot_features(
        &self,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Trait for applying cluster-wide log level changes.
///
/// The cluster crate's state machine calls `apply_log_level` when a `SetLogLevel`
/// entry is committed. The implementation (in the meilisearch binary) parses the
/// target string and updates the tracing subscriber.
pub trait LogLevelApplier: Send + Sync {
    /// Apply a log level change. `target` is a tracing filter string
    /// (e.g., "info" or "meilisearch=debug,actix_web=warn").
    fn apply_log_level(
        &self,
        target: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Trait for applying committed Raft API key entries to the local auth store.
///
/// The cluster crate's state machine calls these methods when `ApiKeyPut` or
/// `ApiKeyDelete` entries are committed. The implementation (in the meilisearch
/// binary) deserializes the Key/Uuid and calls the auth store directly.
///
/// The `raft_log_index` is used for idempotency: if the auth store has already
/// applied this log index, the call is a no-op. This prevents issues on
/// crash-replay (e.g., a delete replaying against a re-created key).
pub trait AuthApplier: Send + Sync {
    /// Apply a committed key create/update. `key_bytes` is a JSON-serialized `Key`.
    fn apply_key_put(
        &self,
        key_bytes: &[u8],
        raft_log_index: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Apply a committed key delete. `uid_bytes` is a bincode-serialized `Uuid`.
    fn apply_key_delete(
        &self,
        uid_bytes: &[u8],
        raft_log_index: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Return all API keys as JSON-serialized bytes (for snapshot building).
    fn snapshot_keys(&self) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;

    /// Clear all existing keys and insert these (for snapshot installation).
    /// `last_applied_log_index` updates the auth store's idempotency tracker.
    fn install_snapshot_keys(
        &self,
        key_bytes_list: &[Vec<u8>],
        last_applied_log_index: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

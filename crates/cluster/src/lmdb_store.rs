//! LMDB-backed implementations of `RaftLogStorage` and `RaftStateMachine`.
//!
//! Replaces the in-memory storage (`mem_store.rs`) so Raft state survives restarts.
//! Uses a separate LMDB environment at `{db_path}/cluster/` to avoid coupling with
//! the index-scheduler's LMDB env.
//!
//! Layout — 3 named databases inside one env:
//!
//! | Database       | Key codec | Value codec                       | Contents |
//! |----------------|-----------|-----------------------------------|----------|
//! | `raft-logs`    | BEU64     | SerdeBincode<Entry<TypeConfig>>   | Raft log entries keyed by index |
//! | `raft-meta`    | Str       | Bytes                             | vote, committed, last_purged, last_applied, last_membership |
//! | `raft-snapshot` | Str      | Bytes                             | snapshot_meta + snapshot_data |

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use heed::types::{Bytes, SerdeBincode, Str};
use heed::{Database, Env, EnvOpenOptions, WithoutTls};
use openraft::entry::RaftPayload;
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, LogId, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::task_applier::{AuthApplier, FeatureApplier, TaskApplier};
use crate::types::{RaftRequest, RaftResponse, TypeConfig};

type BEU64 = heed::types::U64<heed::byteorder::BE>;

/// Truncate a string to at most `max_bytes` bytes without splitting a UTF-8 character.
fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    match s.char_indices().take_while(|&(i, _)| i < max_bytes).last() {
        Some((i, c)) => &s[..i + c.len_utf8()],
        None => "",
    }
}

// Meta key constants
const META_VOTE: &str = "vote";
const META_COMMITTED: &str = "committed";
const META_LAST_PURGED: &str = "last_purged";
const META_LAST_APPLIED: &str = "last_applied";
const META_LAST_MEMBERSHIP: &str = "last_membership";
const META_PROTOCOL_VERSION: &str = "protocol_version";
/// Prefix for per-node compile feature keys: "node_features:{node_id}"
const META_NODE_FEATURES_PREFIX: &str = "node_features:";
/// Prefix for per-node binary version: "node_version:{node_id}"
const META_NODE_VERSION_PREFIX: &str = "node_version:";
/// Prefix for per-node supported protocols: "node_protocols:{node_id}"
const META_NODE_PROTOCOLS_PREFIX: &str = "node_protocols:";
const SNAPSHOT_META: &str = "meta";
const SNAPSHOT_DATA: &str = "data";

/// Persisted node configuration, allowing restart without re-specifying CLI args.
/// Stored as a JSON file at `{data_path}/cluster/node_config.json` (outside LMDB
/// so it can be read without opening the Raft LMDB environment).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct NodeConfig {
    pub node_id: u64,
    pub bind_addr: std::net::SocketAddr,
    pub secret: String,
}

/// A buffered task entry received before the applier was wired.
struct PendingTask {
    kind_bytes: Vec<u8>,
    raft_log_index: u64,
}

/// A buffered auth operation received before the auth applier was wired.
enum PendingAuthOp {
    Put { key_bytes: Vec<u8>, raft_log_index: u64 },
    Delete { uid_bytes: Vec<u8>, raft_log_index: u64 },
}

/// A buffered feature update received before the feature applier was wired.
struct PendingFeatureOp {
    features_json: Vec<u8>,
    raft_log_index: u64,
}

/// Snapshot payload: API keys and runtime features.
/// Outer struct is bincode-serialized (consistent with Raft meta serialization).
#[derive(Serialize, Deserialize)]
struct SnapshotData {
    /// All API keys, each as JSON-serialized bytes (same format as ApiKeyPut).
    api_keys: Vec<Vec<u8>>,
    /// Current runtime features as JSON bytes. Added in protocol version 2.
    #[serde(default)]
    runtime_features_json: Option<Vec<u8>>,
}

/// A snapshot received before the auth applier was wired.
struct PendingSnapshot {
    api_keys: Vec<Vec<u8>>,
    last_applied_log_index: u64,
}

/// LMDB-backed Raft storage implementing both `RaftLogStorage` and `RaftStateMachine`.
///
/// A single struct backed by one LMDB environment with three databases.
/// Passed to `Raft::new()` as both log store and state machine (via `Clone`).
#[derive(Clone)]
pub struct LmdbRaftStore {
    env: Env<WithoutTls>,
    logs: Database<BEU64, SerdeBincode<Entry<TypeConfig>>>,
    meta: Database<Str, Bytes>,
    snapshot_db: Database<Str, Bytes>,
    /// Tracks Raft log entries that failed to apply. Keyed by log index, value is error message.
    /// Entries in this database are skipped on subsequent apply attempts to prevent crash loops.
    failed_entries: Database<BEU64, Str>,
    /// Counter of failed applies, exposed for Prometheus metrics.
    failed_applies_count: Arc<AtomicU64>,
    /// When set, committed TaskEnqueued entries are applied to the local IndexScheduler.
    task_applier: Arc<std::sync::OnceLock<Arc<dyn TaskApplier>>>,
    /// Entries received before the applier was set. Replayed when set_task_applier is called.
    pending_tasks: Arc<std::sync::Mutex<Vec<PendingTask>>>,
    /// When set, committed API key entries are applied to the local auth store.
    auth_applier: Arc<std::sync::OnceLock<Arc<dyn AuthApplier>>>,
    /// Auth entries received before the auth applier was set. Replayed when set_auth_applier is called.
    pending_auth_ops: Arc<std::sync::Mutex<Vec<PendingAuthOp>>>,
    /// Snapshot received before the auth applier was set. Replayed when set_auth_applier is called.
    pending_snapshot: Arc<std::sync::Mutex<Option<PendingSnapshot>>>,
    /// When set, committed SetRuntimeFeatures entries are applied to the local IndexScheduler.
    feature_applier: Arc<std::sync::OnceLock<Arc<dyn FeatureApplier>>>,
    /// Feature entries received before the feature applier was set. Replayed when set_feature_applier is called.
    pending_features: Arc<std::sync::Mutex<Vec<PendingFeatureOp>>>,
    /// When set, committed SetLogLevel entries are applied to the local tracing subscriber.
    log_level_applier: Arc<std::sync::OnceLock<Arc<dyn crate::task_applier::LogLevelApplier>>>,
    /// When set, used to fetch missing content files from cluster peers during
    /// Raft log replay (e.g., after a node restart that missed DML transfers).
    content_file_fetcher:
        Arc<std::sync::OnceLock<Arc<dyn crate::task_applier::ContentFileFetcher>>>,
}

impl std::fmt::Debug for LmdbRaftStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LmdbRaftStore")
            .field("has_task_applier", &self.task_applier.get().is_some())
            .field("failed_applies", &self.failed_applies_count.load(Ordering::Relaxed))
            .finish()
    }
}

/// Open (or create) the LMDB-backed Raft store at `{data_path}/cluster/`.
///
/// `map_size_mb` controls the LMDB map size in megabytes (default 256).
pub fn open_raft_store(data_path: &Path, map_size_mb: u64) -> Result<LmdbRaftStore> {
    let cluster_path = data_path.join("cluster");
    std::fs::create_dir_all(&cluster_path).with_context(|| {
        format!("failed to create cluster directory: {}", cluster_path.display())
    })?;

    let map_size = (map_size_mb as usize) * 1024 * 1024;
    let env = unsafe {
        EnvOpenOptions::new()
            .read_txn_without_tls()
            .max_dbs(4)
            .map_size(map_size)
            .open(&cluster_path)
    }
    .with_context(|| format!("failed to open LMDB env at {}", cluster_path.display()))?;

    let mut wtxn = env.write_txn().context("failed to open write txn for db creation")?;
    let logs = env.create_database(&mut wtxn, Some("raft-logs")).context("create raft-logs db")?;
    let meta = env.create_database(&mut wtxn, Some("raft-meta")).context("create raft-meta db")?;
    let snapshot_db =
        env.create_database(&mut wtxn, Some("raft-snapshot")).context("create raft-snapshot db")?;
    let failed_entries = env
        .create_database(&mut wtxn, Some("failed-raft-entries"))
        .context("create failed-raft-entries db")?;
    wtxn.commit().context("failed to commit db creation")?;

    // Count pre-existing failed entries (from previous runs)
    let initial_failed_count = {
        let rtxn = env.read_txn().context("read_txn for failed entry count")?;
        failed_entries.len(&rtxn).unwrap_or(0)
    };

    Ok(LmdbRaftStore {
        env,
        logs,
        meta,
        snapshot_db,
        failed_entries,
        failed_applies_count: Arc::new(AtomicU64::new(initial_failed_count)),
        task_applier: Arc::new(std::sync::OnceLock::new()),
        pending_tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
        auth_applier: Arc::new(std::sync::OnceLock::new()),
        pending_auth_ops: Arc::new(std::sync::Mutex::new(Vec::new())),
        pending_snapshot: Arc::new(std::sync::Mutex::new(None)),
        feature_applier: Arc::new(std::sync::OnceLock::new()),
        pending_features: Arc::new(std::sync::Mutex::new(Vec::new())),
        log_level_applier: Arc::new(std::sync::OnceLock::new()),
        content_file_fetcher: Arc::new(std::sync::OnceLock::new()),
    })
}

const NODE_CONFIG_FILE: &str = "node_config.json";

/// Check if a persisted cluster exists at `{data_path}/cluster/`.
/// Returns true if the node config file exists.
pub fn has_persisted_cluster(data_path: &Path) -> bool {
    data_path.join("cluster").join(NODE_CONFIG_FILE).exists()
}

/// Validate that persisted Raft log entries can be deserialized by this binary.
/// Opens the LMDB env, reads a few log entries, and checks deserializability.
/// Returns an error with a clear message if log entries are incompatible.
pub fn validate_raft_log_compatibility(data_path: &Path, map_size_mb: u64) -> Result<()> {
    let cluster_path = data_path.join("cluster");
    if !cluster_path.exists() {
        return Ok(());
    }
    let store = open_raft_store(data_path, map_size_mb)?;
    store.validate_raft_log_compatibility()
}

/// Load the persisted node configuration from `{data_path}/cluster/node_config.json`.
/// Returns `None` if no config file exists.
pub fn load_node_config(data_path: &Path) -> Result<Option<NodeConfig>> {
    let path = data_path.join("cluster").join(NODE_CONFIG_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let data = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let config: NodeConfig =
        serde_json::from_str(&data).with_context(|| format!("corrupt {}", path.display()))?;
    Ok(Some(config))
}

/// Save node configuration to `{data_path}/cluster/node_config.json`.
pub fn save_node_config(data_path: &Path, config: &NodeConfig) -> Result<()> {
    let cluster_path = data_path.join("cluster");
    std::fs::create_dir_all(&cluster_path)?;
    let path = cluster_path.join(NODE_CONFIG_FILE);
    let data = serde_json::to_string_pretty(config).context("failed to serialize node config")?;
    // Write via tempfile + fsync for crash safety
    use std::io::Write;
    let mut tmp = tempfile::NamedTempFile::new_in(&cluster_path)
        .with_context(|| format!("failed to create tempfile for {}", path.display()))?;
    tmp.write_all(data.as_bytes())
        .with_context(|| format!("failed to write {}", path.display()))?;
    tmp.as_file().sync_all().with_context(|| format!("failed to fsync {}", path.display()))?;
    tmp.persist(&path).with_context(|| format!("failed to persist {}", path.display()))?;
    // Restrict permissions: config contains cluster secret
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

impl LmdbRaftStore {
    /// Set the task applier after construction.
    /// Called once during startup after both ClusterNode and IndexScheduler are created.
    /// Replays any task entries that were committed before this was called.
    ///
    /// Failed entries are recorded in the `failed-raft-entries` database and skipped
    /// on subsequent attempts, preventing infinite crash loops.
    pub fn set_task_applier(&self, applier: Arc<dyn TaskApplier>) {
        // TOCTOU fix: acquire pending lock, take ownership of buffered entries,
        // THEN set the OnceLock, THEN drop the lock. This ensures that apply()
        // sees either (a) OnceLock=None + pushes to buffer (taken by us), or
        // (b) OnceLock=Some + applies directly. No entry can be both buffered
        // and applied via the fast path.
        let pending = {
            let mut guard = self.pending_tasks.lock().unwrap_or_else(|e| e.into_inner());
            let taken = std::mem::take(&mut *guard);
            if self.task_applier.set(applier.clone()).is_err() {
                return; // already set
            }
            taken
        };
        // Replay outside the lock — skip entries that fail instead of panicking.
        if !pending.is_empty() {
            tracing::info!(count = pending.len(), "Replaying buffered task entries");
            for task in pending {
                if self.is_failed_entry(task.raft_log_index) {
                    tracing::warn!(
                        log_index = task.raft_log_index,
                        "Skipping previously failed task entry during replay"
                    );
                    continue;
                }
                if let Err(e) = applier.apply_task(&task.kind_bytes, task.raft_log_index) {
                    tracing::error!(
                        log_index = task.raft_log_index,
                        "Failed to replay committed task entry: {e}. \
                         Marking as failed and continuing."
                    );
                    self.record_failed_entry(task.raft_log_index, &e.to_string());
                }
            }
        }
    }

    /// Set the auth applier for API key replication.
    /// Called once during startup after both ClusterNode and AuthController are created.
    /// Replays any auth entries that were committed before this was called.
    ///
    /// Failed entries are recorded in the `failed-raft-entries` database and skipped
    /// on subsequent attempts, preventing infinite crash loops.
    pub fn set_auth_applier(&self, applier: Arc<dyn AuthApplier>) {
        // TOCTOU fix: take entries before setting OnceLock (same pattern as
        // set_task_applier — see comment there).
        let pending = {
            let mut guard = self.pending_auth_ops.lock().unwrap_or_else(|e| e.into_inner());
            let taken = std::mem::take(&mut *guard);
            if self.auth_applier.set(applier.clone()).is_err() {
                return; // already set
            }
            taken
        };
        if !pending.is_empty() {
            tracing::info!(count = pending.len(), "Replaying buffered auth entries");
            for op in pending {
                match op {
                    PendingAuthOp::Put { key_bytes, raft_log_index } => {
                        if self.is_failed_entry(raft_log_index) {
                            tracing::warn!(
                                log_index = raft_log_index,
                                "Skipping previously failed API key put during replay"
                            );
                            continue;
                        }
                        if let Err(e) = applier.apply_key_put(&key_bytes, raft_log_index) {
                            tracing::error!(
                                log_index = raft_log_index,
                                "Failed to replay committed API key put: {e}. \
                                 Marking as failed and continuing."
                            );
                            self.record_failed_entry(raft_log_index, &e.to_string());
                        }
                    }
                    PendingAuthOp::Delete { uid_bytes, raft_log_index } => {
                        if self.is_failed_entry(raft_log_index) {
                            tracing::warn!(
                                log_index = raft_log_index,
                                "Skipping previously failed API key delete during replay"
                            );
                            continue;
                        }
                        if let Err(e) = applier.apply_key_delete(&uid_bytes, raft_log_index) {
                            tracing::error!(
                                log_index = raft_log_index,
                                "Failed to replay committed API key delete: {e}. \
                                 Marking as failed and continuing."
                            );
                            self.record_failed_entry(raft_log_index, &e.to_string());
                        }
                    }
                }
            }
        }

        // Replay any pending snapshot that arrived before the applier was set.
        let pending_snap = {
            let mut guard = self.pending_snapshot.lock().unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(snap) = pending_snap {
            tracing::info!(
                keys = snap.api_keys.len(),
                last_applied = snap.last_applied_log_index,
                "Replaying buffered snapshot keys"
            );
            if let Err(e) =
                applier.install_snapshot_keys(&snap.api_keys, snap.last_applied_log_index)
            {
                tracing::error!(
                    last_applied = snap.last_applied_log_index,
                    "Failed to install buffered snapshot keys: {e}. \
                     Marking as failed and continuing."
                );
                self.record_failed_entry(snap.last_applied_log_index, &e.to_string());
            }
        }
    }

    /// Set the feature applier for runtime feature replication.
    /// Called once during startup after both ClusterNode and IndexScheduler are created.
    /// Replays any feature entries that were committed before this was called.
    pub fn set_feature_applier(&self, applier: Arc<dyn FeatureApplier>) {
        // TOCTOU fix: same pattern as set_task_applier / set_auth_applier.
        let pending = {
            let mut guard = self.pending_features.lock().unwrap_or_else(|e| e.into_inner());
            let taken = std::mem::take(&mut *guard);
            if self.feature_applier.set(applier.clone()).is_err() {
                return; // already set
            }
            taken
        };
        if !pending.is_empty() {
            tracing::info!(count = pending.len(), "Replaying buffered feature entries");
            for op in pending {
                if self.is_failed_entry(op.raft_log_index) {
                    tracing::warn!(
                        log_index = op.raft_log_index,
                        "Skipping previously failed feature entry during replay"
                    );
                    continue;
                }
                if let Err(e) = applier.apply_features(&op.features_json) {
                    tracing::error!(
                        log_index = op.raft_log_index,
                        "Failed to replay committed feature entry: {e}. \
                         Marking as failed and continuing."
                    );
                    self.record_failed_entry(op.raft_log_index, &e.to_string());
                }
            }
        }
    }

    /// Set the log level applier for cluster-wide log level changes.
    pub fn set_log_level_applier(
        &self,
        applier: Arc<dyn crate::task_applier::LogLevelApplier>,
    ) {
        let _ = self.log_level_applier.set(applier);
    }

    /// Set the content file fetcher for missing file recovery during Raft replay.
    pub fn set_content_file_fetcher(
        &self,
        fetcher: Arc<dyn crate::task_applier::ContentFileFetcher>,
    ) {
        let _ = self.content_file_fetcher.set(fetcher);
    }

    /// Returns the total number of failed applies (for Prometheus metrics).
    pub fn failed_applies_count(&self) -> u64 {
        self.failed_applies_count.load(Ordering::Relaxed)
    }

    /// Read the current cluster protocol version (defaults to 1).
    pub fn cluster_protocol_version(&self) -> u32 {
        let rtxn = match self.env.read_txn() {
            Ok(rtxn) => rtxn,
            Err(_) => return 1,
        };
        self.read_meta_bincode::<u32>(&rtxn, META_PROTOCOL_VERSION)
            .ok()
            .flatten()
            .unwrap_or(1)
    }

    /// Store compile-time features for a specific node.
    pub fn store_node_features(&self, node_id: u64, features: &[String]) {
        let key = format!("{META_NODE_FEATURES_PREFIX}{node_id}");
        match self.env.write_txn() {
            Ok(mut wtxn) => {
                if let Err(e) = self.write_meta_bincode(&mut wtxn, &key, &features.to_vec()) {
                    tracing::error!(node_id, "Failed to store node features: {e}");
                    return;
                }
                if let Err(e) = wtxn.commit() {
                    tracing::error!(node_id, "Failed to commit node features: {e}");
                }
            }
            Err(e) => {
                tracing::error!(node_id, "Failed to open write txn for node features: {e}");
            }
        }
    }

    /// Read compile-time features for a specific node.
    pub fn node_features(&self, node_id: u64) -> Vec<String> {
        let key = format!("{META_NODE_FEATURES_PREFIX}{node_id}");
        let rtxn = match self.env.read_txn() {
            Ok(rtxn) => rtxn,
            Err(_) => return Vec::new(),
        };
        self.read_meta_bincode::<Vec<String>>(&rtxn, &key)
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    /// Read compile-time features for all known nodes.
    /// Returns a map of node_id → feature list.
    pub fn all_node_features(&self) -> std::collections::BTreeMap<u64, Vec<String>> {
        let rtxn = match self.env.read_txn() {
            Ok(rtxn) => rtxn,
            Err(_) => return std::collections::BTreeMap::new(),
        };
        let mut result = std::collections::BTreeMap::new();
        // Scan all meta keys with the node_features prefix
        let iter = match self.meta.iter(&rtxn) {
            Ok(iter) => iter,
            Err(_) => return result,
        };
        for item in iter {
            let (key, value) = match item {
                Ok(kv) => kv,
                Err(_) => continue,
            };
            if let Some(id_str) = key.strip_prefix(META_NODE_FEATURES_PREFIX) {
                if let Ok(node_id) = id_str.parse::<u64>() {
                    if let Ok(features) = bincode::deserialize::<Vec<String>>(value) {
                        result.insert(node_id, features);
                    }
                }
            }
        }
        result
    }

    /// Remove compile-time features for a specific node (e.g., after eviction).
    pub fn remove_node_features(&self, node_id: u64) {
        let key = format!("{META_NODE_FEATURES_PREFIX}{node_id}");
        match self.env.write_txn() {
            Ok(mut wtxn) => {
                if let Err(e) = self.meta.delete(&mut wtxn, &key) {
                    tracing::error!(node_id, "Failed to remove node features: {e}");
                    return;
                }
                if let Err(e) = wtxn.commit() {
                    tracing::error!(node_id, "Failed to commit node features removal: {e}");
                }
            }
            Err(e) => {
                tracing::error!(node_id, "Failed to open write txn for node features removal: {e}");
            }
        }
    }

    /// Store the binary version for a specific node.
    pub fn store_node_version(&self, node_id: u64, version: &str) {
        let key = format!("{META_NODE_VERSION_PREFIX}{node_id}");
        match self.env.write_txn() {
            Ok(mut wtxn) => {
                if let Err(e) = self.write_meta_bincode(&mut wtxn, &key, &version.to_string()) {
                    tracing::error!(node_id, "Failed to store node version: {e}");
                    return;
                }
                if let Err(e) = wtxn.commit() {
                    tracing::error!(node_id, "Failed to commit node version: {e}");
                }
            }
            Err(e) => {
                tracing::error!(node_id, "Failed to open write txn for node version: {e}");
            }
        }
    }

    /// Read binary versions for all known nodes.
    pub fn all_node_versions(&self) -> std::collections::BTreeMap<u64, String> {
        let rtxn = match self.env.read_txn() {
            Ok(rtxn) => rtxn,
            Err(_) => return std::collections::BTreeMap::new(),
        };
        let mut result = std::collections::BTreeMap::new();
        let iter = match self.meta.iter(&rtxn) {
            Ok(iter) => iter,
            Err(_) => return result,
        };
        for item in iter {
            let (key, value) = match item {
                Ok(kv) => kv,
                Err(_) => continue,
            };
            if let Some(id_str) = key.strip_prefix(META_NODE_VERSION_PREFIX) {
                if let Ok(node_id) = id_str.parse::<u64>() {
                    if let Ok(version) = bincode::deserialize::<String>(value) {
                        result.insert(node_id, version);
                    }
                }
            }
        }
        result
    }

    /// Remove binary version for a specific node (e.g., after eviction).
    pub fn remove_node_version(&self, node_id: u64) {
        let key = format!("{META_NODE_VERSION_PREFIX}{node_id}");
        match self.env.write_txn() {
            Ok(mut wtxn) => {
                if let Err(e) = self.meta.delete(&mut wtxn, &key) {
                    tracing::error!(node_id, "Failed to remove node version: {e}");
                    return;
                }
                if let Err(e) = wtxn.commit() {
                    tracing::error!(node_id, "Failed to commit node version removal: {e}");
                }
            }
            Err(e) => {
                tracing::error!(node_id, "Failed to open write txn for node version removal: {e}");
            }
        }
    }

    /// Store supported protocol versions for a specific node.
    pub fn store_node_protocols(&self, node_id: u64, protocols: &[u32]) {
        let key = format!("{META_NODE_PROTOCOLS_PREFIX}{node_id}");
        match self.env.write_txn() {
            Ok(mut wtxn) => {
                if let Err(e) = self.write_meta_bincode(&mut wtxn, &key, &protocols.to_vec()) {
                    tracing::error!(node_id, "Failed to store node protocols: {e}");
                    return;
                }
                if let Err(e) = wtxn.commit() {
                    tracing::error!(node_id, "Failed to commit node protocols: {e}");
                }
            }
            Err(e) => {
                tracing::error!(node_id, "Failed to open write txn for node protocols: {e}");
            }
        }
    }

    /// Read supported protocol versions for all known nodes.
    pub fn all_node_protocols(&self) -> std::collections::BTreeMap<u64, Vec<u32>> {
        let rtxn = match self.env.read_txn() {
            Ok(rtxn) => rtxn,
            Err(_) => return std::collections::BTreeMap::new(),
        };
        let mut result = std::collections::BTreeMap::new();
        let iter = match self.meta.iter(&rtxn) {
            Ok(iter) => iter,
            Err(_) => return result,
        };
        for item in iter {
            let (key, value) = match item {
                Ok(kv) => kv,
                Err(_) => continue,
            };
            if let Some(id_str) = key.strip_prefix(META_NODE_PROTOCOLS_PREFIX) {
                if let Ok(node_id) = id_str.parse::<u64>() {
                    if let Ok(protocols) = bincode::deserialize::<Vec<u32>>(value) {
                        result.insert(node_id, protocols);
                    }
                }
            }
        }
        result
    }

    /// Remove supported protocol versions for a specific node (e.g., after eviction).
    pub fn remove_node_protocols(&self, node_id: u64) {
        let key = format!("{META_NODE_PROTOCOLS_PREFIX}{node_id}");
        match self.env.write_txn() {
            Ok(mut wtxn) => {
                if let Err(e) = self.meta.delete(&mut wtxn, &key) {
                    tracing::error!(node_id, "Failed to remove node protocols: {e}");
                    return;
                }
                if let Err(e) = wtxn.commit() {
                    tracing::error!(node_id, "Failed to commit node protocols removal: {e}");
                }
            }
            Err(e) => {
                tracing::error!(node_id, "Failed to open write txn for node protocols removal: {e}");
            }
        }
    }

    /// Validate that persisted Raft log entries can be deserialized by this binary.
    /// Returns an error with a clear message if log entries are incompatible.
    pub fn validate_raft_log_compatibility(&self) -> Result<()> {
        let rtxn = self.env.read_txn().context("failed to open read txn for log validation")?;
        let mut iter =
            self.logs.iter(&rtxn).context("failed to iterate raft-logs for validation")?;

        // Check up to 10 entries for deserializability
        let mut checked = 0u32;
        while let Some(result) = iter.next() {
            match result {
                Ok((_key, _entry)) => {
                    // Successfully deserialized — the SerdeBincode codec did the work
                    checked += 1;
                    if checked >= 10 {
                        break;
                    }
                }
                Err(e) => {
                    anyhow::bail!(
                        "Cluster Raft log is incompatible with this binary version.\n\
                         The persisted log entries cannot be deserialized (likely due to \
                         RaftRequest enum changes between versions).\n\n\
                         Error: {e}\n\n\
                         To fix: run with --cluster-reset to wipe cluster state and \
                         re-create the cluster, or downgrade to the previous binary version."
                    );
                }
            }
        }

        Ok(())
    }

    /// Check if a Raft log entry previously failed to apply (within a write transaction).
    fn is_failed_entry_in_txn(&self, wtxn: &heed::RwTxn<'_>, log_index: u64) -> bool {
        self.failed_entries.get(wtxn, &log_index).ok().flatten().is_some()
    }

    /// Record a failed entry in the LMDB database (within an existing write transaction).
    fn record_failed_entry_in_txn(
        &self,
        wtxn: &mut heed::RwTxn<'_>,
        log_index: u64,
        error: &str,
    ) {
        // Truncate error message to ~1KB (UTF-8 safe) to avoid bloating LMDB
        let error_msg = truncate_utf8(error, 1024);
        if let Err(e) = self.failed_entries.put(wtxn, &log_index, error_msg) {
            tracing::error!(log_index, "Failed to record failed entry in LMDB: {e}");
        }
        self.failed_applies_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed entry using a standalone write transaction (for use outside apply()).
    fn record_failed_entry(&self, log_index: u64, error: &str) {
        let error_msg = truncate_utf8(error, 1024);
        match self.env.write_txn() {
            Ok(mut wtxn) => {
                if let Err(e) = self.failed_entries.put(&mut wtxn, &log_index, error_msg) {
                    tracing::error!(log_index, "Failed to record failed entry in LMDB: {e}");
                    return;
                }
                if let Err(e) = wtxn.commit() {
                    tracing::error!(log_index, "Failed to commit failed entry record: {e}");
                    return;
                }
            }
            Err(e) => {
                tracing::error!(log_index, "Failed to open write txn for failed entry: {e}");
            }
        }
        self.failed_applies_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if a Raft log entry previously failed to apply (standalone read transaction).
    fn is_failed_entry(&self, log_index: u64) -> bool {
        match self.env.read_txn() {
            Ok(rtxn) => self.failed_entries.get(&rtxn, &log_index).ok().flatten().is_some(),
            Err(_) => false,
        }
    }

    // ---- Helper methods for reading/writing meta keys ----

    #[allow(clippy::result_large_err)]
    fn read_meta_bincode<T: serde::de::DeserializeOwned>(
        &self,
        rtxn: &heed::RoTxn<'_>,
        key: &str,
    ) -> Result<Option<T>, StorageError<u64>> {
        match self.meta.get(rtxn, key).map_err(|e| storage_io_error(e, key))? {
            Some(bytes) => {
                let val = bincode::deserialize(bytes)
                    .map_err(|e| storage_io_error(e, &format!("deserialize {key}")))?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    #[allow(clippy::result_large_err)]
    fn write_meta_bincode<T: serde::Serialize>(
        &self,
        wtxn: &mut heed::RwTxn<'_>,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError<u64>> {
        let bytes = bincode::serialize(value)
            .map_err(|e| storage_io_error(e, &format!("serialize {key}")))?;
        self.meta.put(wtxn, key, &bytes).map_err(|e| storage_io_error(e, key))?;
        Ok(())
    }
}

fn storage_io_error(e: impl std::fmt::Display, ctx: &str) -> StorageError<u64> {
    StorageError::IO {
        source: openraft::StorageIOError::new(
            openraft::ErrorSubject::Store,
            openraft::ErrorVerb::Read,
            openraft::AnyError::error(format!("{ctx}: {e}")),
        ),
    }
}

// ---- RaftLogReader ----

impl RaftLogReader<TypeConfig> for LmdbRaftStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let rtxn = self.env.read_txn().map_err(|e| storage_io_error(e, "read_txn"))?;
            let iter =
                self.logs.range(&rtxn, &range).map_err(|e| storage_io_error(e, "logs range"))?;
            let entries: Vec<_> = iter
                .map(|r| r.map(|(_, v)| v).map_err(|e| storage_io_error(e, "log entry")))
                .collect::<Result<_, _>>()?;
            Ok(entries)
        })
    }
}

// ---- RaftLogStorage ----

impl RaftLogStorage<TypeConfig> for LmdbRaftStore {
    type LogReader = LmdbRaftStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let rtxn = self.env.read_txn().map_err(|e| storage_io_error(e, "read_txn"))?;

            let last_purged_log_id: Option<LogId<u64>> =
                self.read_meta_bincode(&rtxn, META_LAST_PURGED)?;

            let last_log_id = self
                .logs
                .last(&rtxn)
                .map_err(|e| storage_io_error(e, "logs last"))?
                .map(|(_, e)| e.log_id)
                .or(last_purged_log_id);

            Ok(LogState { last_purged_log_id, last_log_id })
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let mut wtxn = self.env.write_txn().map_err(|e| storage_io_error(e, "write_txn"))?;
            self.write_meta_bincode(&mut wtxn, META_VOTE, vote)?;
            wtxn.commit().map_err(|e| storage_io_error(e, "commit vote"))?;
            Ok(())
        })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let rtxn = self.env.read_txn().map_err(|e| storage_io_error(e, "read_txn"))?;
            self.read_meta_bincode(&rtxn, META_VOTE)
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let mut wtxn = self.env.write_txn().map_err(|e| storage_io_error(e, "write_txn"))?;
            match committed {
                Some(ref c) => self.write_meta_bincode(&mut wtxn, META_COMMITTED, c)?,
                None => {
                    self.meta
                        .delete(&mut wtxn, META_COMMITTED)
                        .map_err(|e| storage_io_error(e, "delete committed"))?;
                }
            }
            wtxn.commit().map_err(|e| storage_io_error(e, "commit committed"))?;
            Ok(())
        })
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let rtxn = self.env.read_txn().map_err(|e| storage_io_error(e, "read_txn"))?;
            self.read_meta_bincode(&rtxn, META_COMMITTED)
        })
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        tokio::task::block_in_place(|| {
            let mut wtxn = self.env.write_txn().map_err(|e| storage_io_error(e, "write_txn"))?;
            for entry in entries {
                self.logs
                    .put(&mut wtxn, &entry.log_id.index, &entry)
                    .map_err(|e| storage_io_error(e, "put log entry"))?;
            }
            wtxn.commit().map_err(|e| storage_io_error(e, "commit append"))?;
            // Signal IO completion after LMDB commit (durable)
            callback.log_io_completed(Ok(()));
            Ok(())
        })
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let mut wtxn = self.env.write_txn().map_err(|e| storage_io_error(e, "write_txn"))?;
            // Delete all entries with index >= log_id.index
            let to_remove: Vec<u64> = self
                .logs
                .range(&wtxn, &(log_id.index..))
                .map_err(|e| storage_io_error(e, "logs range for truncate"))?
                .map(|r| r.map(|(k, _)| k))
                .collect::<Result<_, _>>()
                .map_err(|e| storage_io_error(e, "truncate iter"))?;
            for k in to_remove {
                self.logs.delete(&mut wtxn, &k).map_err(|e| storage_io_error(e, "delete log"))?;
            }
            wtxn.commit().map_err(|e| storage_io_error(e, "commit truncate"))?;
            Ok(())
        })
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let mut wtxn = self.env.write_txn().map_err(|e| storage_io_error(e, "write_txn"))?;
            // Delete all entries with index <= log_id.index
            let to_remove: Vec<u64> = self
                .logs
                .range(&wtxn, &(..=log_id.index))
                .map_err(|e| storage_io_error(e, "logs range for purge"))?
                .map(|r| r.map(|(k, _)| k))
                .collect::<Result<_, _>>()
                .map_err(|e| storage_io_error(e, "purge iter"))?;
            for k in to_remove {
                self.logs.delete(&mut wtxn, &k).map_err(|e| storage_io_error(e, "delete log"))?;
            }
            self.write_meta_bincode(&mut wtxn, META_LAST_PURGED, &log_id)?;
            wtxn.commit().map_err(|e| storage_io_error(e, "commit purge"))?;
            Ok(())
        })
    }
}

// ---- RaftStateMachine ----

impl RaftStateMachine<TypeConfig> for LmdbRaftStore {
    type SnapshotBuilder = LmdbSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let rtxn = self.env.read_txn().map_err(|e| storage_io_error(e, "read_txn"))?;

            let last_applied: Option<LogId<u64>> =
                self.read_meta_bincode(&rtxn, META_LAST_APPLIED)?;
            let last_membership: Option<StoredMembership<u64, BasicNode>> =
                self.read_meta_bincode(&rtxn, META_LAST_MEMBERSHIP)?;

            Ok((last_applied, last_membership.unwrap_or_default()))
        })
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        // Collect entries first (can't hold write txn across async task applier calls)
        let entries: Vec<_> = entries.into_iter().collect();

        // Pre-fetch missing content files before entering block_in_place.
        // This handles the case where this node was down during DML transfer
        // and is catching up via Raft log replay.
        if let Some(applier) = self.task_applier.get() {
            if let Some(fetcher) = self.content_file_fetcher.get() {
                for entry in &entries {
                    if let openraft::EntryPayload::Normal(ref req) = entry.payload {
                        #[allow(clippy::collapsible_match)]
                        if let crate::types::RaftRequest::TaskEnqueued { kind_bytes } = req {
                            if let Some(uuid) = applier.missing_content_uuid(kind_bytes) {
                                // Retry with backoff — the leader may not be known
                                // yet if this node just reconnected after a partition.
                                let mut fetched = false;
                                for attempt in 0..5u32 {
                                    match fetcher.fetch_content_file(uuid).await {
                                        Ok(()) => {
                                            tracing::info!(
                                                log_index = entry.log_id.index,
                                                %uuid,
                                                attempt,
                                                "Fetched missing content file from peer"
                                            );
                                            fetched = true;
                                            break;
                                        }
                                        Err(e) => {
                                            if attempt < 4 {
                                                tracing::warn!(
                                                    log_index = entry.log_id.index,
                                                    %uuid,
                                                    attempt,
                                                    error = %e,
                                                    "Content file fetch attempt failed, retrying"
                                                );
                                                tokio::time::sleep(
                                                    std::time::Duration::from_millis(
                                                        500 * (1 << attempt)
                                                    )
                                                ).await;
                                            } else {
                                                tracing::error!(
                                                    log_index = entry.log_id.index,
                                                    %uuid,
                                                    error = %e,
                                                    "Failed to fetch missing content file \
                                                     after {} attempts — task will fail on apply",
                                                    attempt + 1
                                                );
                                            }
                                        }
                                    }
                                }
                                if !fetched {
                                    tracing::error!(
                                        log_index = entry.log_id.index,
                                        %uuid,
                                        "Content file missing and could not be fetched — \
                                         Raft entry will be applied without content file"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        tokio::task::block_in_place(|| {
            let mut wtxn = self.env.write_txn().map_err(|e| storage_io_error(e, "write_txn"))?;

            for entry in entries {
                // Skip entries that previously failed to apply (prevents crash loops on restart).
                if self.is_failed_entry_in_txn(&wtxn, entry.log_id.index) {
                    tracing::warn!(
                        log_index = entry.log_id.index,
                        "Skipping previously failed Raft entry"
                    );
                    responses.push(RaftResponse::Ok);
                    self.write_meta_bincode(&mut wtxn, META_LAST_APPLIED, &entry.log_id)?;
                    continue;
                }

                // Track membership changes
                if let Some(membership) = entry.payload.get_membership() {
                    let stored = StoredMembership::new(Some(entry.log_id), membership.clone());
                    self.write_meta_bincode(&mut wtxn, META_LAST_MEMBERSHIP, &stored)?;
                }

                // Apply entries to the local state via the appropriate applier.
                // Failed entries are recorded and skipped rather than panicking,
                // preventing infinite crash loops on persistent failures.
                let response = match entry.payload {
                    openraft::EntryPayload::Normal(ref req) => match req {
                        RaftRequest::TaskEnqueued { kind_bytes } => {
                            if let Some(applier) = self.task_applier.get() {
                                match applier.apply_task(kind_bytes, entry.log_id.index) {
                                    Ok(task_uid) => {
                                        RaftResponse::TaskRegistered { task_uid }
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            log_index = entry.log_id.index,
                                            "Failed to apply committed task entry: {e}. \
                                             Marking as failed and continuing."
                                        );
                                        self.record_failed_entry_in_txn(
                                            &mut wtxn,
                                            entry.log_id.index,
                                            &e.to_string(),
                                        );
                                        RaftResponse::Ok
                                    }
                                }
                            } else {
                                // Lock pending buffer and re-check OnceLock to prevent TOCTOU
                                // race with set_task_applier (which sets OnceLock under this lock).
                                let mut pending =
                                    self.pending_tasks.lock().unwrap_or_else(|e| e.into_inner());
                                if let Some(applier) = self.task_applier.get() {
                                    drop(pending);
                                    match applier.apply_task(kind_bytes, entry.log_id.index) {
                                        Ok(task_uid) => {
                                            RaftResponse::TaskRegistered { task_uid }
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                log_index = entry.log_id.index,
                                                "Failed to apply committed task entry: {e}. \
                                                 Marking as failed and continuing."
                                            );
                                            self.record_failed_entry_in_txn(
                                                &mut wtxn,
                                                entry.log_id.index,
                                                &e.to_string(),
                                            );
                                            RaftResponse::Ok
                                        }
                                    }
                                } else {
                                    warn!("TaskApplier not yet set, buffering TaskEnqueued entry");
                                    pending.push(PendingTask {
                                        kind_bytes: kind_bytes.clone(),
                                        raft_log_index: entry.log_id.index,
                                    });
                                    RaftResponse::Ok
                                }
                            }
                        }
                        RaftRequest::ApiKeyPut { key_bytes } => {
                            if let Some(applier) = self.auth_applier.get() {
                                if let Err(e) =
                                    applier.apply_key_put(key_bytes, entry.log_id.index)
                                {
                                    tracing::error!(
                                        log_index = entry.log_id.index,
                                        "Failed to apply committed API key put: {e}. \
                                         Marking as failed and continuing."
                                    );
                                    self.record_failed_entry_in_txn(
                                        &mut wtxn,
                                        entry.log_id.index,
                                        &e.to_string(),
                                    );
                                }
                            } else {
                                // Lock and re-check (same TOCTOU pattern as TaskEnqueued above).
                                let mut pending =
                                    self.pending_auth_ops.lock().unwrap_or_else(|e| e.into_inner());
                                if let Some(applier) = self.auth_applier.get() {
                                    drop(pending);
                                    if let Err(e) =
                                        applier.apply_key_put(key_bytes, entry.log_id.index)
                                    {
                                        tracing::error!(
                                            log_index = entry.log_id.index,
                                            "Failed to apply committed API key put: {e}. \
                                             Marking as failed and continuing."
                                        );
                                        self.record_failed_entry_in_txn(
                                            &mut wtxn,
                                            entry.log_id.index,
                                            &e.to_string(),
                                        );
                                    }
                                } else {
                                    warn!("AuthApplier not yet set, buffering ApiKeyPut entry");
                                    pending.push(PendingAuthOp::Put {
                                        key_bytes: key_bytes.clone(),
                                        raft_log_index: entry.log_id.index,
                                    });
                                }
                            }
                            RaftResponse::Ok
                        }
                        RaftRequest::ApiKeyDelete { uid_bytes } => {
                            if let Some(applier) = self.auth_applier.get() {
                                if let Err(e) =
                                    applier.apply_key_delete(uid_bytes, entry.log_id.index)
                                {
                                    tracing::error!(
                                        log_index = entry.log_id.index,
                                        "Failed to apply committed API key delete: {e}. \
                                         Marking as failed and continuing."
                                    );
                                    self.record_failed_entry_in_txn(
                                        &mut wtxn,
                                        entry.log_id.index,
                                        &e.to_string(),
                                    );
                                }
                            } else {
                                // Lock and re-check (same TOCTOU pattern as above).
                                let mut pending =
                                    self.pending_auth_ops.lock().unwrap_or_else(|e| e.into_inner());
                                if let Some(applier) = self.auth_applier.get() {
                                    drop(pending);
                                    if let Err(e) =
                                        applier.apply_key_delete(uid_bytes, entry.log_id.index)
                                    {
                                        tracing::error!(
                                            log_index = entry.log_id.index,
                                            "Failed to apply committed API key delete: {e}. \
                                             Marking as failed and continuing."
                                        );
                                        self.record_failed_entry_in_txn(
                                            &mut wtxn,
                                            entry.log_id.index,
                                            &e.to_string(),
                                        );
                                    }
                                } else {
                                    warn!("AuthApplier not yet set, buffering ApiKeyDelete entry");
                                    pending.push(PendingAuthOp::Delete {
                                        uid_bytes: uid_bytes.clone(),
                                        raft_log_index: entry.log_id.index,
                                    });
                                }
                            }
                            RaftResponse::Ok
                        }
                        RaftRequest::ClusterProtocolUpgrade { version } => {
                            tracing::info!(
                                version,
                                "Applying cluster protocol upgrade"
                            );
                            if let Err(e) =
                                self.write_meta_bincode(&mut wtxn, META_PROTOCOL_VERSION, version)
                            {
                                tracing::error!(
                                    version,
                                    "Failed to persist protocol version: {e}"
                                );
                            }
                            RaftResponse::Ok
                        }
                        RaftRequest::SetRuntimeFeatures { features_json } => {
                            if let Some(applier) = self.feature_applier.get() {
                                if let Err(e) = applier.apply_features(features_json) {
                                    tracing::error!(
                                        log_index = entry.log_id.index,
                                        "Failed to apply committed runtime features: {e}. \
                                         Marking as failed and continuing."
                                    );
                                    self.record_failed_entry_in_txn(
                                        &mut wtxn,
                                        entry.log_id.index,
                                        &e.to_string(),
                                    );
                                }
                            } else {
                                // Lock and re-check (same TOCTOU pattern as TaskEnqueued above).
                                let mut pending =
                                    self.pending_features.lock().unwrap_or_else(|e| e.into_inner());
                                if let Some(applier) = self.feature_applier.get() {
                                    drop(pending);
                                    if let Err(e) = applier.apply_features(features_json) {
                                        tracing::error!(
                                            log_index = entry.log_id.index,
                                            "Failed to apply committed runtime features: {e}. \
                                             Marking as failed and continuing."
                                        );
                                        self.record_failed_entry_in_txn(
                                            &mut wtxn,
                                            entry.log_id.index,
                                            &e.to_string(),
                                        );
                                    }
                                } else {
                                    warn!("FeatureApplier not yet set, buffering SetRuntimeFeatures entry");
                                    pending.push(PendingFeatureOp {
                                        features_json: features_json.clone(),
                                        raft_log_index: entry.log_id.index,
                                    });
                                }
                            }
                            RaftResponse::Ok
                        }
                        RaftRequest::SetLogLevel { ref target } => {
                            if let Some(applier) = self.log_level_applier.get() {
                                if let Err(e) = applier.apply_log_level(target) {
                                    tracing::error!(
                                        log_index = entry.log_id.index,
                                        "Failed to apply log level change: {e}"
                                    );
                                }
                            } else {
                                tracing::warn!(
                                    log_index = entry.log_id.index,
                                    "Log level applier not set, ignoring SetLogLevel entry"
                                );
                            }
                            RaftResponse::Ok
                        }
                        RaftRequest::Noop => RaftResponse::Ok,
                    },
                    _ => RaftResponse::Ok,
                };
                responses.push(response);

                // Persist last_applied_log_id after each entry
                self.write_meta_bincode(&mut wtxn, META_LAST_APPLIED, &entry.log_id)?;
            }

            wtxn.commit().map_err(|e| storage_io_error(e, "commit apply"))?;
            Ok(responses)
        })
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        LmdbSnapshotBuilder { store: self.clone() }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let mut wtxn = self.env.write_txn().map_err(|e| storage_io_error(e, "write_txn"))?;

            // Persist snapshot meta and data
            let meta_bytes = bincode::serialize(meta)
                .map_err(|e| storage_io_error(e, "serialize snapshot meta"))?;
            self.snapshot_db
                .put(&mut wtxn, SNAPSHOT_META, &meta_bytes)
                .map_err(|e| storage_io_error(e, "put snapshot meta"))?;

            let data = snapshot.into_inner();
            self.snapshot_db
                .put(&mut wtxn, SNAPSHOT_DATA, &data)
                .map_err(|e| storage_io_error(e, "put snapshot data"))?;

            // Update applied state to match snapshot
            if let Some(ref last_log_id) = meta.last_log_id {
                self.write_meta_bincode(&mut wtxn, META_LAST_APPLIED, last_log_id)?;
            }
            self.write_meta_bincode(&mut wtxn, META_LAST_MEMBERSHIP, &meta.last_membership)?;

            wtxn.commit().map_err(|e| storage_io_error(e, "commit install_snapshot"))?;

            // Deserialize snapshot data (handle empty/legacy data for backward compat)
            let last_applied_index =
                meta.last_log_id.as_ref().map(|id| id.index).unwrap_or(0);
            let (api_keys, runtime_features_json) = if data.is_empty() {
                (Vec::new(), None)
            } else {
                match bincode::deserialize::<SnapshotData>(&data) {
                    Ok(snap_data) => (snap_data.api_keys, snap_data.runtime_features_json),
                    Err(e) => {
                        warn!("Failed to deserialize snapshot data (legacy format?): {e}");
                        (Vec::new(), None)
                    }
                }
            };

            if !api_keys.is_empty() {
                if let Some(applier) = self.auth_applier.get() {
                    tracing::info!(
                        keys = api_keys.len(),
                        "Installing snapshot keys into auth store"
                    );
                    applier
                        .install_snapshot_keys(&api_keys, last_applied_index)
                        .map_err(|e| storage_io_error(e, "install snapshot keys"))?;
                } else {
                    tracing::info!(
                        keys = api_keys.len(),
                        "AuthApplier not yet set, buffering snapshot keys"
                    );
                    let mut guard =
                        self.pending_snapshot.lock().unwrap_or_else(|e| e.into_inner());
                    *guard = Some(PendingSnapshot {
                        api_keys,
                        last_applied_log_index: last_applied_index,
                    });
                }
            }

            // Install runtime features from snapshot
            if let Some(ref features_json) = runtime_features_json {
                if let Some(applier) = self.feature_applier.get() {
                    tracing::info!("Installing snapshot runtime features");
                    if let Err(e) = applier.apply_features(features_json) {
                        tracing::error!(
                            "Failed to install snapshot runtime features: {e}"
                        );
                    }
                } else {
                    tracing::info!("FeatureApplier not yet set, buffering snapshot features");
                    let mut guard =
                        self.pending_features.lock().unwrap_or_else(|e| e.into_inner());
                    guard.push(PendingFeatureOp {
                        features_json: features_json.clone(),
                        raft_log_index: last_applied_index,
                    });
                }
            }

            Ok(())
        })
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        tokio::task::block_in_place(|| {
            let rtxn = self.env.read_txn().map_err(|e| storage_io_error(e, "read_txn"))?;

            let meta_bytes = self
                .snapshot_db
                .get(&rtxn, SNAPSHOT_META)
                .map_err(|e| storage_io_error(e, "get snapshot meta"))?;

            match meta_bytes {
                Some(bytes) => {
                    let meta: SnapshotMeta<u64, BasicNode> = bincode::deserialize(bytes)
                        .map_err(|e| storage_io_error(e, "deserialize snapshot meta"))?;

                    let data = self
                        .snapshot_db
                        .get(&rtxn, SNAPSHOT_DATA)
                        .map_err(|e| storage_io_error(e, "get snapshot data"))?
                        .unwrap_or(&[])
                        .to_vec();

                    Ok(Some(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) }))
                }
                None => Ok(None),
            }
        })
    }
}

// ---- Snapshot Builder ----

pub struct LmdbSnapshotBuilder {
    store: LmdbRaftStore,
}

impl RaftSnapshotBuilder<TypeConfig> for LmdbSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        tokio::task::block_in_place(|| {
            // Read current applied state
            let rtxn = self.store.env.read_txn().map_err(|e| storage_io_error(e, "read_txn"))?;

            let last_applied: Option<LogId<u64>> =
                self.store.read_meta_bincode(&rtxn, META_LAST_APPLIED)?;
            let last_membership: StoredMembership<u64, BasicNode> =
                self.store.read_meta_bincode(&rtxn, META_LAST_MEMBERSHIP)?.unwrap_or_default();

            drop(rtxn);

            let snapshot_id =
                format!("lmdb-{}", last_applied.as_ref().map(|id| id.index).unwrap_or(0));

            let meta = SnapshotMeta { last_log_id: last_applied, last_membership, snapshot_id };

            // Build snapshot data containing API keys and runtime features.
            // If appliers aren't set yet (early startup), produce an empty snapshot.
            let api_keys = if let Some(applier) = self.store.auth_applier.get() {
                applier
                    .snapshot_keys()
                    .map_err(|e| storage_io_error(e, "snapshot_keys"))?
            } else {
                Vec::new()
            };
            let runtime_features_json = if let Some(applier) = self.store.feature_applier.get() {
                applier
                    .snapshot_features()
                    .map_err(|e| storage_io_error(e, "snapshot_features"))?
            } else {
                None
            };
            let snapshot_data = if api_keys.is_empty() && runtime_features_json.is_none() {
                Vec::new()
            } else {
                let snap = SnapshotData { api_keys, runtime_features_json };
                bincode::serialize(&snap)
                    .map_err(|e| storage_io_error(e, "serialize snapshot data"))?
            };

            // Persist to LMDB so get_current_snapshot() can find this snapshot
            let mut wtxn =
                self.store.env.write_txn().map_err(|e| storage_io_error(e, "write_txn"))?;
            let meta_bytes = bincode::serialize(&meta)
                .map_err(|e| storage_io_error(e, "serialize snapshot meta"))?;
            self.store
                .snapshot_db
                .put(&mut wtxn, SNAPSHOT_META, &meta_bytes)
                .map_err(|e| storage_io_error(e, "put snapshot meta"))?;
            self.store
                .snapshot_db
                .put(&mut wtxn, SNAPSHOT_DATA, &snapshot_data)
                .map_err(|e| storage_io_error(e, "put snapshot data"))?;
            wtxn.commit().map_err(|e| storage_io_error(e, "commit build_snapshot"))?;

            Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(snapshot_data)) })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use openraft::CommittedLeaderId;

    use super::*;

    /// Helper to create a LogId with term and index.
    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    fn temp_store() -> (LmdbRaftStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = open_raft_store(dir.path(), 256).unwrap();
        (store, dir)
    }

    /// Direct LMDB write for testing — bypasses LogFlushed callback (which is pub(crate) in openraft).
    fn test_append_entries(store: &LmdbRaftStore, entries: Vec<Entry<TypeConfig>>) {
        let mut wtxn = store.env.write_txn().unwrap();
        for entry in entries {
            store.logs.put(&mut wtxn, &entry.log_id.index, &entry).unwrap();
        }
        wtxn.commit().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lmdb_store_persistence() {
        let dir = tempfile::tempdir().unwrap();

        // Open store, write data, drop it
        {
            let mut store = open_raft_store(dir.path(), 256).unwrap();

            // Save vote
            let vote = Vote::new(1, 1);
            store.save_vote(&vote).await.unwrap();

            // Append entries via direct LMDB write
            let entry1 = Entry::<TypeConfig> {
                log_id: log_id(1, 1),
                payload: openraft::EntryPayload::Normal(RaftRequest::Noop),
            };
            let entry2 = Entry::<TypeConfig> {
                log_id: log_id(1, 2),
                payload: openraft::EntryPayload::Normal(RaftRequest::Noop),
            };
            test_append_entries(&store, vec![entry1, entry2]);

            // Save committed
            store.save_committed(Some(log_id(1, 2))).await.unwrap();
        }

        // Reopen and verify data survived
        {
            let mut store = open_raft_store(dir.path(), 256).unwrap();

            // Verify vote
            let vote = store.read_vote().await.unwrap();
            assert_eq!(vote, Some(Vote::new(1, 1)));

            // Verify committed
            let committed = store.read_committed().await.unwrap();
            assert_eq!(committed, Some(log_id(1, 2)));

            // Verify log state
            let log_state = store.get_log_state().await.unwrap();
            assert_eq!(log_state.last_log_id, Some(log_id(1, 2)));
            assert_eq!(log_state.last_purged_log_id, None);

            // Verify log entries
            let entries = store.try_get_log_entries(1..=2).await.unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].log_id, log_id(1, 1));
            assert_eq!(entries[1].log_id, log_id(1, 2));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lmdb_store_truncate_purge() {
        let (mut store, _dir) = temp_store();

        // Append 5 entries
        let entries: Vec<_> = (1..=5)
            .map(|i| Entry::<TypeConfig> {
                log_id: log_id(1, i),
                payload: openraft::EntryPayload::Normal(RaftRequest::Noop),
            })
            .collect();
        test_append_entries(&store, entries);

        // Truncate from index 4 (removes 4, 5)
        store.truncate(log_id(1, 4)).await.unwrap();
        let remaining = store.try_get_log_entries(1..=5).await.unwrap();
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining.last().unwrap().log_id.index, 3);

        // Purge up to index 2 (removes 1, 2)
        store.purge(log_id(1, 2)).await.unwrap();
        let remaining = store.try_get_log_entries(1..=5).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].log_id.index, 3);

        // Verify last_purged
        let log_state = store.get_log_state().await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(log_id(1, 2)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lmdb_store_log_compaction() {
        let (mut store, _dir) = temp_store();

        // Append 10 entries
        let entries: Vec<_> = (1..=10)
            .map(|i| Entry::<TypeConfig> {
                log_id: log_id(1, i),
                payload: openraft::EntryPayload::Normal(RaftRequest::Noop),
            })
            .collect();
        test_append_entries(&store, entries);

        // Apply all entries (updates last_applied)
        let apply_entries: Vec<_> = (1..=10)
            .map(|i| Entry::<TypeConfig> {
                log_id: log_id(1, i),
                payload: openraft::EntryPayload::Normal(RaftRequest::Noop),
            })
            .collect();
        let responses = store.apply(apply_entries).await.unwrap();
        assert_eq!(responses.len(), 10);

        // Verify last_applied is set
        let (last_applied, _) = store.applied_state().await.unwrap();
        assert_eq!(last_applied, Some(log_id(1, 10)));

        // Build a snapshot — this should persist to LMDB
        let mut builder = store.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(log_id(1, 10)));
        assert!(snapshot.meta.snapshot_id.contains("10"));

        // get_current_snapshot should find the snapshot we just built
        let current = store.get_current_snapshot().await.unwrap().expect("should have snapshot");
        assert_eq!(current.meta.last_log_id, Some(log_id(1, 10)));

        // Purge logs up to the snapshot (simulate openraft's log compaction)
        store.purge(log_id(1, 10)).await.unwrap();

        // All old logs should be gone
        let remaining = store.try_get_log_entries(1..=10).await.unwrap();
        assert!(remaining.is_empty());

        // But log state should show last_purged
        let log_state = store.get_log_state().await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(log_id(1, 10)));

        // Applied state should still be intact (not affected by log purge)
        let (last_applied, _) = store.applied_state().await.unwrap();
        assert_eq!(last_applied, Some(log_id(1, 10)));

        // New entries can be appended after purge
        let new_entries = vec![Entry::<TypeConfig> {
            log_id: log_id(1, 11),
            payload: openraft::EntryPayload::Normal(RaftRequest::Noop),
        }];
        test_append_entries(&store, new_entries);
        let log_state = store.get_log_state().await.unwrap();
        assert_eq!(log_state.last_log_id, Some(log_id(1, 11)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lmdb_store_snapshot_roundtrip() {
        let (mut store, _dir) = temp_store();

        let membership = StoredMembership::new(
            Some(log_id(1, 5)),
            openraft::Membership::new(
                vec![BTreeSet::from([1u64])],
                std::collections::BTreeMap::from([(
                    1u64,
                    BasicNode { addr: "http://localhost:7700".to_string() },
                )]),
            ),
        );

        let meta = SnapshotMeta {
            last_log_id: Some(log_id(1, 5)),
            last_membership: membership,
            snapshot_id: "test-snapshot-1".to_string(),
        };

        // Use valid SnapshotData (with API keys)
        let api_key_json = br#"{"uid":"550e8400-e29b-41d4-a716-446655440000","name":"test"}"#;
        let snap = SnapshotData { api_keys: vec![api_key_json.to_vec()], runtime_features_json: None };
        let snapshot_data = bincode::serialize(&snap).unwrap();
        let snapshot = Box::new(Cursor::new(snapshot_data.clone()));

        // Install snapshot
        store.install_snapshot(&meta, snapshot).await.unwrap();

        // Read it back
        let current = store.get_current_snapshot().await.unwrap().expect("should have snapshot");
        assert_eq!(current.meta.snapshot_id, "test-snapshot-1");
        assert_eq!(current.meta.last_log_id, Some(log_id(1, 5)));

        let data = current.snapshot.into_inner();
        assert_eq!(data, snapshot_data);

        // Verify the snapshot data round-trips correctly
        let deserialized: SnapshotData = bincode::deserialize(&data).unwrap();
        assert_eq!(deserialized.api_keys.len(), 1);
        assert_eq!(deserialized.api_keys[0], api_key_json);

        // Verify applied state was updated
        let (last_applied, last_membership) = store.applied_state().await.unwrap();
        assert_eq!(last_applied, Some(log_id(1, 5)));
        assert_eq!(last_membership.log_id(), &Some(log_id(1, 5)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lmdb_store_snapshot_legacy_data() {
        // Verify that legacy (non-SnapshotData) snapshot bytes are handled gracefully
        let (mut store, _dir) = temp_store();

        let membership = StoredMembership::new(
            Some(log_id(1, 3)),
            openraft::Membership::new(
                vec![BTreeSet::from([1u64])],
                std::collections::BTreeMap::from([(
                    1u64,
                    BasicNode { addr: "http://localhost:7700".to_string() },
                )]),
            ),
        );

        let meta = SnapshotMeta {
            last_log_id: Some(log_id(1, 3)),
            last_membership: membership,
            snapshot_id: "legacy-snapshot".to_string(),
        };

        // Old-format arbitrary bytes — should not panic
        let snapshot = Box::new(Cursor::new(b"old format data".to_vec()));
        store.install_snapshot(&meta, snapshot).await.unwrap();

        let (last_applied, _) = store.applied_state().await.unwrap();
        assert_eq!(last_applied, Some(log_id(1, 3)));
    }

    /// Mock AuthApplier that records calls for testing.
    struct MockAuthApplier {
        keys: std::sync::Mutex<Vec<Vec<u8>>>,
    }

    impl MockAuthApplier {
        fn new(initial_keys: Vec<Vec<u8>>) -> Self {
            Self { keys: std::sync::Mutex::new(initial_keys) }
        }
    }

    impl crate::task_applier::AuthApplier for MockAuthApplier {
        fn apply_key_put(
            &self,
            _key_bytes: &[u8],
            _raft_log_index: u64,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        fn apply_key_delete(
            &self,
            _uid_bytes: &[u8],
            _raft_log_index: u64,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        fn snapshot_keys(
            &self,
        ) -> std::result::Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(self.keys.lock().unwrap().clone())
        }

        fn install_snapshot_keys(
            &self,
            key_bytes_list: &[Vec<u8>],
            _last_applied_log_index: u64,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let mut keys = self.keys.lock().unwrap();
            keys.clear();
            keys.extend_from_slice(key_bytes_list);
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_snapshot_with_auth_keys() {
        let (mut store, _dir) = temp_store();

        // Wire a mock auth applier with some initial keys
        let key1 = br#"{"uid":"00000000-0000-0000-0000-000000000001","name":"key1"}"#.to_vec();
        let key2 = br#"{"uid":"00000000-0000-0000-0000-000000000002","name":"key2"}"#.to_vec();
        let mock = Arc::new(MockAuthApplier::new(vec![key1.clone(), key2.clone()]));
        store.set_auth_applier(mock.clone());

        // Apply some entries so last_applied is set
        let entries: Vec<_> = (1..=5)
            .map(|i| Entry::<TypeConfig> {
                log_id: log_id(1, i),
                payload: openraft::EntryPayload::Normal(RaftRequest::Noop),
            })
            .collect();
        store.apply(entries).await.unwrap();

        // Build a snapshot — should include the 2 keys
        let mut builder = store.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(log_id(1, 5)));

        let snapshot_bytes = snapshot.snapshot.into_inner();
        assert!(!snapshot_bytes.is_empty());

        // Deserialize and verify keys are in the snapshot
        let snap_data: SnapshotData = bincode::deserialize(&snapshot_bytes).unwrap();
        assert_eq!(snap_data.api_keys.len(), 2);
        assert_eq!(snap_data.api_keys[0], key1);
        assert_eq!(snap_data.api_keys[1], key2);

        // Install this snapshot on a fresh store (simulating a new node)
        let (mut store2, _dir2) = temp_store();
        let mock2 = Arc::new(MockAuthApplier::new(Vec::new()));
        store2.set_auth_applier(mock2.clone());

        let membership = StoredMembership::new(
            Some(log_id(1, 5)),
            openraft::Membership::new(
                vec![BTreeSet::from([1u64])],
                std::collections::BTreeMap::from([(
                    1u64,
                    BasicNode { addr: "http://localhost:7700".to_string() },
                )]),
            ),
        );
        let meta = SnapshotMeta {
            last_log_id: Some(log_id(1, 5)),
            last_membership: membership,
            snapshot_id: "test-snap".to_string(),
        };
        store2
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .unwrap();

        // The mock2 auth applier should now have the 2 keys
        let installed_keys = mock2.keys.lock().unwrap();
        assert_eq!(installed_keys.len(), 2);
        assert_eq!(installed_keys[0], key1);
        assert_eq!(installed_keys[1], key2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_snapshot_buffered_when_no_auth_applier() {
        // Verify that a snapshot received before the auth applier is set gets buffered
        // and replayed when set_auth_applier is called.
        let (mut store, _dir) = temp_store();

        // Build snapshot data with keys
        let key1 = br#"{"uid":"00000000-0000-0000-0000-000000000001","name":"key1"}"#.to_vec();
        let snap = SnapshotData { api_keys: vec![key1.clone()], runtime_features_json: None };
        let snapshot_bytes = bincode::serialize(&snap).unwrap();

        let membership = StoredMembership::new(
            Some(log_id(1, 10)),
            openraft::Membership::new(
                vec![BTreeSet::from([1u64])],
                std::collections::BTreeMap::from([(
                    1u64,
                    BasicNode { addr: "http://localhost:7700".to_string() },
                )]),
            ),
        );
        let meta = SnapshotMeta {
            last_log_id: Some(log_id(1, 10)),
            last_membership: membership,
            snapshot_id: "buffered-snap".to_string(),
        };

        // Install snapshot WITHOUT auth applier set — should buffer
        store
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .unwrap();

        // Verify it's buffered
        {
            let guard = store.pending_snapshot.lock().unwrap();
            assert!(guard.is_some());
            let pending = guard.as_ref().unwrap();
            assert_eq!(pending.api_keys.len(), 1);
            assert_eq!(pending.last_applied_log_index, 10);
        }

        // Now set the auth applier — should replay the buffered snapshot
        let mock = Arc::new(MockAuthApplier::new(Vec::new()));
        store.set_auth_applier(mock.clone());

        // Buffer should be cleared
        {
            let guard = store.pending_snapshot.lock().unwrap();
            assert!(guard.is_none());
        }

        // Mock should have the key
        let installed_keys = mock.keys.lock().unwrap();
        assert_eq!(installed_keys.len(), 1);
        assert_eq!(installed_keys[0], key1);
    }
}

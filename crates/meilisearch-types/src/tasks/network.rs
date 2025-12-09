use std::collections::BTreeMap;

use base64::Engine as _;
use itertools::{EitherOrBoth, Itertools as _};
use milli::{CboRoaringBitmapCodec, DocumentId};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::error::ResponseError;
use crate::network::{Network, Remote};
use crate::tasks::{Details, TaskId};

#[cfg(not(feature = "enterprise"))]
mod community_edition;
#[cfg(feature = "enterprise")]
mod enterprise_edition;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged, rename_all = "camelCase")]
// This type is used in the database, care should be taken when modifying it.
pub enum DbTaskNetwork {
    /// Tasks that were duplicated from `origin`
    Origin { origin: Origin },
    /// Tasks that were duplicated as `remote_tasks`
    Remotes {
        remote_tasks: BTreeMap<String, RemoteTask>,
        #[serde(default)]
        network_version: Uuid,
    },
    /// Document import tasks sent in the context of `network_change`
    Import { import_from: ImportData, network_change: Origin },
}

impl DbTaskNetwork {
    pub fn network_version(&self) -> Uuid {
        match self {
            DbTaskNetwork::Origin { origin } => origin.network_version,
            DbTaskNetwork::Remotes { remote_tasks: _, network_version } => *network_version,
            DbTaskNetwork::Import { import_from: _, network_change } => {
                network_change.network_version
            }
        }
    }

    pub fn import_data(&self) -> Option<&ImportData> {
        match self {
            DbTaskNetwork::Origin { .. } | DbTaskNetwork::Remotes { .. } => None,
            DbTaskNetwork::Import { import_from, .. } => Some(import_from),
        }
    }

    pub fn origin(&self) -> Option<&Origin> {
        match self {
            DbTaskNetwork::Origin { origin } => Some(origin),
            DbTaskNetwork::Remotes { .. } => None,
            DbTaskNetwork::Import { network_change, .. } => Some(network_change),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TaskNetwork {
    /// Tasks that were duplicated from `origin`
    Origin { origin: Origin },
    /// Tasks that were duplicated as `remote_tasks`
    Remotes { remote_tasks: BTreeMap<String, RemoteTask>, network_version: Uuid },
    /// Document import tasks sent in the context of `network_change`
    Import { import_from: ImportData, network_change: Origin, metadata: ImportMetadata },
}

impl TaskNetwork {
    pub fn network_version(&self) -> Uuid {
        match self {
            TaskNetwork::Origin { origin } => origin.network_version,
            TaskNetwork::Remotes { remote_tasks: _, network_version } => *network_version,
            TaskNetwork::Import { import_from: _, network_change, metadata: _ } => {
                network_change.network_version
            }
        }
    }
}

impl From<TaskNetwork> for DbTaskNetwork {
    fn from(value: TaskNetwork) -> Self {
        match value {
            TaskNetwork::Origin { origin } => DbTaskNetwork::Origin { origin },
            TaskNetwork::Remotes { remote_tasks, network_version } => {
                DbTaskNetwork::Remotes { remote_tasks, network_version }
            }
            TaskNetwork::Import { import_from, network_change, metadata: _ } => {
                DbTaskNetwork::Import { import_from, network_change }
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Origin {
    pub remote_name: String,
    pub task_uid: u32,
    #[serde(default)]
    pub network_version: Uuid,
}

/// Import data stored in a task
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ImportData {
    /// Remote that this task is imported from
    pub remote_name: String,
    /// Index relevant to this task
    pub index_name: Option<String>,
    /// Number of documents in this task
    pub document_count: u64,
}

/// Import metadata associated with a task but not stored in the task
#[derive(Debug, PartialEq, Clone)]
pub struct ImportMetadata {
    /// Total number of indexes to import from this host
    pub index_count: u64,
    /// Key unique to this (network_change, index, host, key).
    ///
    /// In practice, an internal document id of one of the documents to import.
    pub task_key: Option<DocumentId>,
    /// Total number of documents to import for this index from this host.
    pub total_index_documents: u64,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RemoteTask {
    #[serde(skip_serializing_if = "Option::is_none")]
    task_uid: Option<TaskId>,
    error: Option<ResponseError>,
}

impl From<Result<TaskId, ResponseError>> for RemoteTask {
    fn from(res: Result<TaskId, ResponseError>) -> RemoteTask {
        match res {
            Ok(task_uid) => RemoteTask { task_uid: Some(task_uid), error: None },
            Err(err) => RemoteTask { task_uid: None, error: Some(err) },
        }
    }
}

/// Contains the full state of a network topology change.
///
/// A network topology change task is unique in that it can be processed in multiple different batches, as its resolution
/// depends on various document additions tasks being processed.
///
/// A network topology task has 4 states:
///
/// 1. Processing any task that was meant for an earlier version of the network. This is necessary to know that we have the right version of
///    documents.
/// 2. Sending all documents that must be moved to other remotes.
/// 3. Processing any task coming from the remotes.
/// 4. Finished.
///
/// Furthermore, it maintains some stats
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkTopologyChange {
    state: NetworkTopologyState,
    // in name, `None` if the node is no longer part of the network
    #[serde(skip_serializing_if = "Option::is_none")]
    in_name: Option<String>,
    // out name, `None` if the node is new to the network
    #[serde(skip_serializing_if = "Option::is_none")]
    out_name: Option<String>,
    out_remotes: BTreeMap<String, Remote>,
    in_remotes: BTreeMap<String, InRemote>,
    stats: NetworkTopologyStats,
}

impl NetworkTopologyChange {
    pub fn new(old_network: Network, new_network: Network) -> Self {
        // we use our old name as export name
        let out_name = old_network.local;
        // we use our new name as import name
        let in_name = new_network.local;
        // we export to the new network
        let mut out_remotes = new_network.remotes;
        // don't export to ourselves
        if let Some(in_name) = &in_name {
            out_remotes.remove(in_name);
        }
        let in_remotes = old_network
            .remotes
            .into_keys()
            // don't await imports from ourselves
            .filter(|name| Some(name.as_str()) != out_name.as_deref())
            .map(|name| (name, InRemote::new()))
            .collect();
        Self {
            state: NetworkTopologyState::WaitingForOlderTasks,
            in_name,
            out_name,
            out_remotes,
            in_remotes,
            stats: NetworkTopologyStats { moved_documents: 0 },
        }
    }

    pub fn state(&self) -> NetworkTopologyState {
        self.state
    }

    pub fn out_name(&self) -> Option<&str> {
        // unwrap: one of out name or in_name must be defined
        self.out_name.as_deref()
    }

    pub fn in_name(&self) -> Option<&str> {
        self.in_name.as_deref()
    }

    pub fn to_details(&self) -> Details {
        let message = match self.state {
            NetworkTopologyState::WaitingForOlderTasks => {
                "Waiting for tasks enqueued before the network change to finish processing".into()
            }
            NetworkTopologyState::ExportingDocuments => "Exporting documents".into(),
            NetworkTopologyState::ImportingDocuments => {
                let mut finished_count = 0;
                let mut first_ongoing = None;
                let mut ongoing_total_indexes = 0;
                let mut ongoing_processed_documents = 0;
                let mut ongoing_missing_documents = 0;
                let mut ongoing_total_documents = 0;
                let mut other_ongoing_count = 0;
                let mut first_waiting = None;
                let mut other_waiting_count = 0;
                for (remote_name, in_remote) in &self.in_remotes {
                    match &in_remote.import_state {
                        ImportState::WaitingForInitialTask => {
                            first_waiting = match first_waiting {
                                None => Some(remote_name),
                                first_waiting => {
                                    other_waiting_count += 1;
                                    first_waiting
                                }
                            };
                        }
                        ImportState::Ongoing { import_index_state, total_indexes } => {
                            first_ongoing = match first_ongoing {
                                None => {
                                    ongoing_total_indexes = *total_indexes;
                                    Some(remote_name)
                                }
                                first_ongoing => {
                                    other_ongoing_count += 1;
                                    first_ongoing
                                }
                            };
                            for import_state in import_index_state.values() {
                                match import_state {
                                    ImportIndexState::Ongoing {
                                        total_documents,
                                        processed_documents,
                                        received_documents,
                                        task_keys: _,
                                    } => {
                                        ongoing_total_documents += total_documents;
                                        ongoing_processed_documents += processed_documents;
                                        ongoing_missing_documents +=
                                            total_documents.saturating_sub(*received_documents);
                                    }
                                    ImportIndexState::Finished { total_documents } => {
                                        ongoing_total_documents += total_documents;
                                        ongoing_processed_documents += total_documents;
                                    }
                                }
                            }
                        }
                        ImportState::Finished { total_indexes, total_documents } => {
                            finished_count += 1;
                            ongoing_total_indexes = *total_indexes;
                            ongoing_total_documents += *total_documents;
                            ongoing_processed_documents += *total_documents;
                        }
                    }
                }
                format!(
                    "Importing documents from {total} remotes{waiting}{ongoing}{finished}",
                    total = self.in_remotes.len(),
                    waiting = if let Some(first_waiting) = first_waiting {
                        format!(
                            ", waiting on first task from `{}`{others}",
                            first_waiting,
                            others = if other_waiting_count > 0 {
                                format!(" and {other_waiting_count} other remotes")
                            } else {
                                "".into()
                            }
                        )
                    } else {
                        "".into()
                    },
                    ongoing = if let Some(first_ongoing) = first_ongoing {
                        format!(", awaiting {ongoing_missing_documents} and processed {ongoing_processed_documents} out of {ongoing_total_documents} documents in {ongoing_total_indexes} indexes from `{first_ongoing}`{others}",
                others=if other_ongoing_count > 0 {format!(" and {other_ongoing_count} other remotes")} else {"".into()})
                    } else {
                        "".into()
                    },
                    finished = if finished_count >= 0 {
                        format!(", {finished_count} remotes finished processing")
                    } else {
                        "".into()
                    }
                )
            }
            NetworkTopologyState::Finished => "Finished".into(),
        };
        Details::NetworkTopologyChange { moved_documents: self.stats.moved_documents, message }
    }

    pub fn merge(&mut self, other: NetworkTopologyChange) {
        // The topology change has a guarantee of forward progress, so for each field we're going to keep the "most advanced" values.
        let Self { state, in_name: _, out_name: _, out_remotes: _, in_remotes, stats } = self;

        *state = Ord::max(*state, other.state);
        *stats = Ord::max(*stats, other.stats);

        for (old_value, new_value) in other.in_remotes.into_values().zip(in_remotes.values_mut()) {
            new_value.import_state = match (old_value.import_state, std::mem::take(&mut new_value.import_state)) {
                    // waiting for initial task is always older
                    (ImportState::WaitingForInitialTask, newer)
                    | (newer, ImportState::WaitingForInitialTask)

                    // finished is always newer
                    | (_, newer @ ImportState::Finished { .. })
                    | (newer @ ImportState::Finished { .. }, _) => newer,
                    (
                        ImportState::Ongoing { import_index_state: left_import, total_indexes: left_total_indexes },
                        ImportState::Ongoing { import_index_state: right_import, total_indexes: right_total_indexes },
                    ) => {
                        let import_index_state = left_import.into_iter().merge_join_by(right_import.into_iter(), |(k,_), (x, _)|k.cmp(x)).map(|eob|
                            match eob {
                                EitherOrBoth::Both((name, left), (_, right)) => {
                                    let newer = merge_import_index_state(left, right);
                                    (name, newer)
                                },
                                EitherOrBoth::Left(import) |
                                EitherOrBoth::Right(import) => import,
                            }
                        ).collect();

                        ImportState::Ongoing{ import_index_state, total_indexes : u64::max(left_total_indexes, right_total_indexes) }
                    },
                }
        }
    }
}

fn merge_import_index_state(left: ImportIndexState, right: ImportIndexState) -> ImportIndexState {
    match (left, right) {
        (_, newer @ ImportIndexState::Finished { .. }) => newer,
        (newer @ ImportIndexState::Finished { .. }, _) => newer,
        (
            ImportIndexState::Ongoing {
                total_documents: left_total_documents,
                received_documents: left_received_documents,
                processed_documents: left_processed_documents,
                task_keys: mut left_task_keys,
            },
            ImportIndexState::Ongoing {
                total_documents: right_total_documents,
                received_documents: right_received_documents,
                processed_documents: right_processed_documents,
                task_keys: right_task_keys,
            },
        ) => {
            let total_documents = u64::max(left_total_documents, right_total_documents);
            let received_documents = u64::max(left_received_documents, right_received_documents);
            let processed_documents = u64::max(left_processed_documents, right_processed_documents);
            left_task_keys.0 |= &right_task_keys.0;
            let task_keys = left_task_keys;

            ImportIndexState::Ongoing {
                total_documents,
                received_documents,
                processed_documents,
                task_keys,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum NetworkTopologyState {
    WaitingForOlderTasks,
    ExportingDocuments,
    ImportingDocuments,
    Finished,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct NetworkTopologyStats {
    #[serde(default)]
    pub moved_documents: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InRemote {
    import_state: ImportState,
}

impl InRemote {
    pub fn new() -> Self {
        Self { import_state: ImportState::WaitingForInitialTask }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ImportState {
    /// Initially Meilisearch doesn't know how many documents it should expect from a remote.
    /// Any task from each remote contains the information of how many indexes will be imported,
    /// and the number of documents to import for the index of the task.
    #[default]
    WaitingForInitialTask,
    Ongoing {
        import_index_state: BTreeMap<String, ImportIndexState>,
        total_indexes: u64,
    },
    Finished {
        total_indexes: u64,
        total_documents: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ImportIndexState {
    Ongoing {
        total_documents: u64,
        received_documents: u64,
        processed_documents: u64,
        task_keys: TaskKeys,
    },
    Finished {
        total_documents: u64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskKeys(pub RoaringBitmap);

impl Serialize for TaskKeys {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let TaskKeys(task_keys) = self;
        let mut bytes = Vec::new();
        CboRoaringBitmapCodec::serialize_into_vec(task_keys, &mut bytes);
        let encoded = base64::prelude::BASE64_STANDARD.encode(&bytes);
        serializer.serialize_str(&encoded)
    }
}

impl<'de> Deserialize<'de> for TaskKeys {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TaskKeysVisitor)
    }
}

struct TaskKeysVisitor;
impl<'de> serde::de::Visitor<'de> for TaskKeysVisitor {
    type Value = TaskKeys;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a base64 encoded cbo roaring bitmap")
    }

    fn visit_str<E>(self, encoded: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let decoded = base64::prelude::BASE64_STANDARD.decode(encoded).map_err(|_err| {
            E::invalid_value(serde::de::Unexpected::Str(encoded), &"a base64 string")
        })?;
        self.visit_bytes(&decoded)
    }

    fn visit_bytes<E>(self, decoded: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let task_keys = CboRoaringBitmapCodec::deserialize_from(decoded).map_err(|_err| {
            E::invalid_value(serde::de::Unexpected::Bytes(decoded), &"a cbo roaring bitmap")
        })?;
        Ok(TaskKeys(task_keys))
    }
}

pub enum ReceiveTaskError {
    UnknownRemote(String),
    DuplicateTask(DocumentId),
}

pub mod headers {
    pub const PROXY_ORIGIN_REMOTE_HEADER: &str = "X-Meili-Proxy-Origin-Remote";
    pub const PROXY_ORIGIN_TASK_UID_HEADER: &str = "X-Meili-Proxy-Origin-TaskUid";
    pub const PROXY_ORIGIN_NETWORK_VERSION_HEADER: &str = "X-Meili-Proxy-Origin-Network-Version";
    pub const PROXY_IMPORT_REMOTE_HEADER: &str = "X-Meili-Proxy-Import-Remote";
    pub const PROXY_IMPORT_INDEX_COUNT_HEADER: &str = "X-Meili-Proxy-Import-Index-Count";
    pub const PROXY_IMPORT_INDEX_HEADER: &str = "X-Meili-Proxy-Import-Index";
    pub const PROXY_IMPORT_TASK_KEY_HEADER: &str = "X-Meili-Proxy-Import-Task-Key";
    pub const PROXY_IMPORT_DOCS_HEADER: &str = "X-Meili-Proxy-Import-Docs";
    pub const PROXY_IMPORT_TOTAL_INDEX_DOCS_HEADER: &str = "X-Meili-Proxy-Import-Total-Index-Docs";
}

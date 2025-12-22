use core::fmt;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Display, Write};
use std::str::FromStr;

use byte_unit::Byte;
use enum_iterator::Sequence;
use milli::update::{IndexDocumentsMethod, MissingDocumentPolicy};
use milli::Object;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use time::{Duration, OffsetDateTime};
use utoipa::{schema, ToSchema};
use uuid::Uuid;

use crate::batches::BatchId;
use crate::error::ResponseError;
use crate::index_uid_pattern::IndexUidPattern;
use crate::keys::Key;
use crate::settings::{Settings, Unchecked};
use crate::{versioning, InstanceUid};

pub type TaskId = u32;

pub mod network;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub uid: TaskId,
    pub batch_uid: Option<BatchId>,

    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,

    pub error: Option<ResponseError>,
    pub canceled_by: Option<TaskId>,
    pub details: Option<Details>,

    pub status: Status,
    pub kind: KindWithContent,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<network::DbTaskNetwork>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_metadata: Option<String>,
}

impl Task {
    pub fn index_uid(&self) -> Option<&str> {
        use KindWithContent::*;

        match &self.kind {
            DumpCreation { .. }
            | SnapshotCreation
            | TaskCancelation { .. }
            | TaskDeletion { .. }
            | Export { .. }
            | UpgradeDatabase { .. }
            | NetworkTopologyChange { .. }
            | IndexSwap { .. } => None,
            DocumentAdditionOrUpdate { index_uid, .. }
            | DocumentEdition { index_uid, .. }
            | DocumentDeletion { index_uid, .. }
            | DocumentDeletionByFilter { index_uid, .. }
            | DocumentClear { index_uid }
            | SettingsUpdate { index_uid, .. }
            | IndexCreation { index_uid, .. }
            | IndexUpdate { index_uid, .. }
            | IndexDeletion { index_uid }
            | IndexCompaction { index_uid } => Some(index_uid),
        }
    }

    /// Return the list of indexes updated by this tasks.
    pub fn indexes(&self) -> Vec<&str> {
        self.kind.indexes()
    }

    /// Return the content-uuid if there is one
    pub fn content_uuid(&self) -> Option<Uuid> {
        match self.kind {
            KindWithContent::DocumentAdditionOrUpdate { content_file, .. } => Some(content_file),
            KindWithContent::DocumentEdition { .. }
            | KindWithContent::DocumentDeletion { .. }
            | KindWithContent::DocumentDeletionByFilter { .. }
            | KindWithContent::DocumentClear { .. }
            | KindWithContent::SettingsUpdate { .. }
            | KindWithContent::IndexDeletion { .. }
            | KindWithContent::IndexCreation { .. }
            | KindWithContent::IndexUpdate { .. }
            | KindWithContent::IndexSwap { .. }
            | KindWithContent::TaskCancelation { .. }
            | KindWithContent::TaskDeletion { .. }
            | KindWithContent::DumpCreation { .. }
            | KindWithContent::SnapshotCreation
            | KindWithContent::Export { .. }
            | KindWithContent::UpgradeDatabase { .. }
            | KindWithContent::NetworkTopologyChange { .. }
            | KindWithContent::IndexCompaction { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KindWithContent {
    DocumentAdditionOrUpdate {
        index_uid: String,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_file: Uuid,
        documents_count: u64,
        allow_index_creation: bool,
        #[serde(default)]
        on_missing_document: MissingDocumentPolicy,
    },
    DocumentDeletion {
        index_uid: String,
        documents_ids: Vec<String>,
    },
    DocumentDeletionByFilter {
        index_uid: String,
        filter_expr: Value,
    },
    DocumentEdition {
        index_uid: String,
        filter_expr: Option<Value>,
        context: Option<milli::Object>,
        function: String,
    },
    DocumentClear {
        index_uid: String,
    },
    SettingsUpdate {
        index_uid: String,
        new_settings: Box<Settings<Unchecked>>,
        is_deletion: bool,
        allow_index_creation: bool,
    },
    IndexDeletion {
        index_uid: String,
    },
    IndexCreation {
        index_uid: String,
        primary_key: Option<String>,
    },
    IndexUpdate {
        index_uid: String,
        primary_key: Option<String>,
        new_index_uid: Option<String>,
    },
    IndexSwap {
        swaps: Vec<IndexSwap>,
    },
    TaskCancelation {
        query: String,
        tasks: RoaringBitmap,
    },
    TaskDeletion {
        query: String,
        tasks: RoaringBitmap,
    },
    DumpCreation {
        keys: Vec<Key>,
        instance_uid: Option<InstanceUid>,
    },
    SnapshotCreation,
    Export {
        url: String,
        api_key: Option<String>,
        payload_size: Option<Byte>,
        indexes: BTreeMap<IndexUidPattern, ExportIndexSettings>,
    },
    UpgradeDatabase {
        from: (u32, u32, u32),
    },
    IndexCompaction {
        index_uid: String,
    },
    NetworkTopologyChange(network::NetworkTopologyChange),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IndexSwap {
    #[schema(value_type = Vec<String>, example = json!(["indexA", "indexB"]))]
    pub indexes: (String, String),
    #[serde(default)]
    pub rename: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExportIndexSettings {
    pub filter: Option<Value>,
    pub override_settings: bool,
}

impl KindWithContent {
    pub fn as_kind(&self) -> Kind {
        match self {
            KindWithContent::DocumentAdditionOrUpdate { .. } => Kind::DocumentAdditionOrUpdate,
            KindWithContent::DocumentEdition { .. } => Kind::DocumentEdition,
            KindWithContent::DocumentDeletion { .. } => Kind::DocumentDeletion,
            KindWithContent::DocumentDeletionByFilter { .. } => Kind::DocumentDeletion,
            KindWithContent::DocumentClear { .. } => Kind::DocumentDeletion,
            KindWithContent::SettingsUpdate { .. } => Kind::SettingsUpdate,
            KindWithContent::IndexCreation { .. } => Kind::IndexCreation,
            KindWithContent::IndexDeletion { .. } => Kind::IndexDeletion,
            KindWithContent::IndexUpdate { .. } => Kind::IndexUpdate,
            KindWithContent::IndexSwap { .. } => Kind::IndexSwap,
            KindWithContent::TaskCancelation { .. } => Kind::TaskCancelation,
            KindWithContent::TaskDeletion { .. } => Kind::TaskDeletion,
            KindWithContent::DumpCreation { .. } => Kind::DumpCreation,
            KindWithContent::SnapshotCreation => Kind::SnapshotCreation,
            KindWithContent::Export { .. } => Kind::Export,
            KindWithContent::UpgradeDatabase { .. } => Kind::UpgradeDatabase,
            KindWithContent::IndexCompaction { .. } => Kind::IndexCompaction,
            KindWithContent::NetworkTopologyChange { .. } => Kind::NetworkTopologyChange,
        }
    }

    pub fn indexes(&self) -> Vec<&str> {
        use KindWithContent::*;

        match self {
            DumpCreation { .. }
            | SnapshotCreation
            | TaskCancelation { .. }
            | TaskDeletion { .. }
            | Export { .. }
            | NetworkTopologyChange { .. }
            | UpgradeDatabase { .. } => vec![],
            DocumentAdditionOrUpdate { index_uid, .. }
            | DocumentEdition { index_uid, .. }
            | DocumentDeletion { index_uid, .. }
            | DocumentDeletionByFilter { index_uid, .. }
            | DocumentClear { index_uid }
            | SettingsUpdate { index_uid, .. }
            | IndexCreation { index_uid, .. }
            | IndexDeletion { index_uid }
            | IndexCompaction { index_uid } => vec![index_uid],
            IndexUpdate { index_uid, new_index_uid, .. } => {
                let mut indexes = vec![index_uid.as_str()];
                if let Some(new_uid) = new_index_uid {
                    indexes.push(new_uid.as_str());
                }
                indexes
            }
            IndexSwap { swaps } => {
                let mut indexes = HashSet::<&str>::default();
                for swap in swaps {
                    indexes.insert(swap.indexes.0.as_str());
                    indexes.insert(swap.indexes.1.as_str());
                }
                indexes.into_iter().collect()
            }
        }
    }

    /// Returns the default `Details` that correspond to this `KindWithContent`,
    /// `None` if it cannot be generated.
    pub fn default_details(&self) -> Option<Details> {
        match self {
            KindWithContent::DocumentAdditionOrUpdate { documents_count, .. } => {
                Some(Details::DocumentAdditionOrUpdate {
                    received_documents: *documents_count,
                    indexed_documents: None,
                })
            }
            KindWithContent::DocumentEdition { index_uid: _, filter_expr, context, function } => {
                Some(Details::DocumentEdition {
                    deleted_documents: None,
                    edited_documents: None,
                    original_filter: filter_expr.as_ref().map(|v| v.to_string()),
                    context: context.clone(),
                    function: function.clone(),
                })
            }
            KindWithContent::DocumentDeletion { index_uid: _, documents_ids } => {
                Some(Details::DocumentDeletion {
                    provided_ids: documents_ids.len(),
                    deleted_documents: None,
                })
            }
            KindWithContent::DocumentDeletionByFilter { index_uid: _, filter_expr } => {
                Some(Details::DocumentDeletionByFilter {
                    original_filter: filter_expr.to_string(),
                    deleted_documents: None,
                })
            }
            KindWithContent::DocumentClear { .. } | KindWithContent::IndexDeletion { .. } => {
                Some(Details::ClearAll { deleted_documents: None })
            }
            KindWithContent::SettingsUpdate { new_settings, .. } => {
                Some(Details::SettingsUpdate { settings: new_settings.clone() })
            }
            KindWithContent::IndexCreation { primary_key, .. } => Some(Details::IndexInfo {
                primary_key: primary_key.clone(),
                old_index_uid: None,
                new_index_uid: None,
            }),
            KindWithContent::IndexUpdate { primary_key, new_index_uid, index_uid } => {
                Some(Details::IndexInfo {
                    primary_key: primary_key.clone(),
                    old_index_uid: new_index_uid.as_ref().map(|_| index_uid.clone()),
                    new_index_uid: new_index_uid.clone(),
                })
            }
            KindWithContent::IndexSwap { swaps } => {
                Some(Details::IndexSwap { swaps: swaps.clone() })
            }
            KindWithContent::TaskCancelation { query, tasks } => Some(Details::TaskCancelation {
                matched_tasks: tasks.len(),
                canceled_tasks: None,
                original_filter: query.clone(),
            }),
            KindWithContent::TaskDeletion { query, tasks } => Some(Details::TaskDeletion {
                matched_tasks: tasks.len(),
                deleted_tasks: None,
                original_filter: query.clone(),
            }),
            KindWithContent::DumpCreation { .. } => Some(Details::Dump { dump_uid: None }),
            KindWithContent::SnapshotCreation => None,
            KindWithContent::Export { url, api_key, payload_size, indexes } => {
                Some(Details::Export {
                    url: url.clone(),
                    api_key: api_key.clone(),
                    payload_size: *payload_size,
                    indexes: indexes.iter().map(|(p, s)| (p.clone(), s.clone().into())).collect(),
                })
            }
            KindWithContent::UpgradeDatabase { from } => Some(Details::UpgradeDatabase {
                from: (from.0, from.1, from.2),
                to: (
                    versioning::VERSION_MAJOR,
                    versioning::VERSION_MINOR,
                    versioning::VERSION_PATCH,
                ),
            }),
            KindWithContent::IndexCompaction { index_uid } => Some(Details::IndexCompaction {
                index_uid: index_uid.clone(),
                pre_compaction_size: None,
                post_compaction_size: None,
            }),
            KindWithContent::NetworkTopologyChange { .. } => Some(Details::NetworkTopologyChange {
                moved_documents: 0,
                message: "processing tasks for previous network versions".into(),
            }),
        }
    }

    pub fn default_finished_details(&self) -> Option<Details> {
        match self {
            KindWithContent::DocumentAdditionOrUpdate { documents_count, .. } => {
                Some(Details::DocumentAdditionOrUpdate {
                    received_documents: *documents_count,
                    indexed_documents: Some(0),
                })
            }
            KindWithContent::DocumentEdition { index_uid: _, filter_expr, context, function } => {
                Some(Details::DocumentEdition {
                    deleted_documents: Some(0),
                    edited_documents: Some(0),
                    original_filter: filter_expr.as_ref().map(|v| v.to_string()),
                    context: context.clone(),
                    function: function.clone(),
                })
            }
            KindWithContent::DocumentDeletion { index_uid: _, documents_ids } => {
                Some(Details::DocumentDeletion {
                    provided_ids: documents_ids.len(),
                    deleted_documents: Some(0),
                })
            }
            KindWithContent::DocumentDeletionByFilter { index_uid: _, filter_expr } => {
                Some(Details::DocumentDeletionByFilter {
                    original_filter: filter_expr.to_string(),
                    deleted_documents: Some(0),
                })
            }
            KindWithContent::DocumentClear { .. } => {
                Some(Details::ClearAll { deleted_documents: None })
            }
            KindWithContent::SettingsUpdate { new_settings, .. } => {
                Some(Details::SettingsUpdate { settings: new_settings.clone() })
            }
            KindWithContent::IndexDeletion { .. } => None,
            KindWithContent::IndexCreation { primary_key, .. } => Some(Details::IndexInfo {
                primary_key: primary_key.clone(),
                old_index_uid: None,
                new_index_uid: None,
            }),
            KindWithContent::IndexUpdate { primary_key, new_index_uid, index_uid } => {
                Some(Details::IndexInfo {
                    primary_key: primary_key.clone(),
                    old_index_uid: new_index_uid.as_ref().map(|_| index_uid.clone()),
                    new_index_uid: new_index_uid.clone(),
                })
            }
            KindWithContent::IndexSwap { .. } => {
                unimplemented!("do not call `default_finished_details` for `IndexSwap` tasks")
            }
            KindWithContent::TaskCancelation { query, tasks } => Some(Details::TaskCancelation {
                matched_tasks: tasks.len(),
                canceled_tasks: Some(0),
                original_filter: query.clone(),
            }),
            KindWithContent::TaskDeletion { query, tasks } => Some(Details::TaskDeletion {
                matched_tasks: tasks.len(),
                deleted_tasks: Some(0),
                original_filter: query.clone(),
            }),
            KindWithContent::DumpCreation { .. } => Some(Details::Dump { dump_uid: None }),
            KindWithContent::SnapshotCreation => None,
            KindWithContent::Export { url, api_key, payload_size, indexes } => {
                Some(Details::Export {
                    url: url.clone(),
                    api_key: api_key.clone(),
                    payload_size: *payload_size,
                    indexes: indexes.iter().map(|(p, s)| (p.clone(), s.clone().into())).collect(),
                })
            }
            KindWithContent::UpgradeDatabase { from } => Some(Details::UpgradeDatabase {
                from: *from,
                to: (
                    versioning::VERSION_MAJOR,
                    versioning::VERSION_MINOR,
                    versioning::VERSION_PATCH,
                ),
            }),
            KindWithContent::IndexCompaction { index_uid } => Some(Details::IndexCompaction {
                index_uid: index_uid.clone(),
                pre_compaction_size: None,
                post_compaction_size: None,
            }),
            KindWithContent::NetworkTopologyChange(network_topology_change) => {
                Some(network_topology_change.to_details())
            }
        }
    }
}

impl From<&KindWithContent> for Option<Details> {
    fn from(kind: &KindWithContent) -> Self {
        match kind {
            KindWithContent::DocumentAdditionOrUpdate { documents_count, .. } => {
                Some(Details::DocumentAdditionOrUpdate {
                    received_documents: *documents_count,
                    indexed_documents: None,
                })
            }
            KindWithContent::DocumentEdition { .. } => None,
            KindWithContent::DocumentDeletion { .. } => None,
            KindWithContent::DocumentDeletionByFilter { .. } => None,
            KindWithContent::DocumentClear { .. } => None,
            KindWithContent::SettingsUpdate { new_settings, .. } => {
                Some(Details::SettingsUpdate { settings: new_settings.clone() })
            }
            KindWithContent::IndexDeletion { .. } => None,
            KindWithContent::IndexCreation { primary_key, .. } => Some(Details::IndexInfo {
                primary_key: primary_key.clone(),
                new_index_uid: None,
                old_index_uid: None,
            }),
            KindWithContent::IndexUpdate { primary_key, new_index_uid, index_uid } => {
                Some(Details::IndexInfo {
                    primary_key: primary_key.clone(),
                    old_index_uid: new_index_uid.as_ref().map(|_| index_uid.clone()),
                    new_index_uid: new_index_uid.clone(),
                })
            }
            KindWithContent::IndexSwap { .. } => None,
            KindWithContent::TaskCancelation { query, tasks } => Some(Details::TaskCancelation {
                matched_tasks: tasks.len(),
                canceled_tasks: None,
                original_filter: query.clone(),
            }),
            KindWithContent::TaskDeletion { query, tasks } => Some(Details::TaskDeletion {
                matched_tasks: tasks.len(),
                deleted_tasks: None,
                original_filter: query.clone(),
            }),
            KindWithContent::DumpCreation { .. } => Some(Details::Dump { dump_uid: None }),
            KindWithContent::SnapshotCreation => None,
            KindWithContent::Export { url, api_key, payload_size, indexes } => {
                Some(Details::Export {
                    url: url.clone(),
                    api_key: api_key.clone(),
                    payload_size: *payload_size,
                    indexes: indexes.iter().map(|(p, s)| (p.clone(), s.clone().into())).collect(),
                })
            }
            KindWithContent::UpgradeDatabase { from } => Some(Details::UpgradeDatabase {
                from: *from,
                to: (
                    versioning::VERSION_MAJOR,
                    versioning::VERSION_MINOR,
                    versioning::VERSION_PATCH,
                ),
            }),
            KindWithContent::IndexCompaction { index_uid } => Some(Details::IndexCompaction {
                index_uid: index_uid.clone(),
                pre_compaction_size: None,
                post_compaction_size: None,
            }),
            KindWithContent::NetworkTopologyChange(network_topology_change) => {
                Some(network_topology_change.to_details())
            }
        }
    }
}

/// The status of a task.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Sequence,
    PartialOrd,
    Ord,
    ToSchema,
)]
#[schema(example = json!(Status::Processing))]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
    Canceled,
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Enqueued => write!(f, "enqueued"),
            Status::Processing => write!(f, "processing"),
            Status::Succeeded => write!(f, "succeeded"),
            Status::Failed => write!(f, "failed"),
            Status::Canceled => write!(f, "canceled"),
        }
    }
}

impl FromStr for Status {
    type Err = ParseTaskStatusError;

    fn from_str(status: &str) -> Result<Self, Self::Err> {
        if status.eq_ignore_ascii_case("enqueued") {
            Ok(Status::Enqueued)
        } else if status.eq_ignore_ascii_case("processing") {
            Ok(Status::Processing)
        } else if status.eq_ignore_ascii_case("succeeded") {
            Ok(Status::Succeeded)
        } else if status.eq_ignore_ascii_case("failed") {
            Ok(Status::Failed)
        } else if status.eq_ignore_ascii_case("canceled") {
            Ok(Status::Canceled)
        } else {
            Err(ParseTaskStatusError(status.to_owned()))
        }
    }
}

#[derive(Debug)]
pub struct ParseTaskStatusError(pub String);
impl fmt::Display for ParseTaskStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a valid task status. Available statuses are {}.",
            self.0,
            enum_iterator::all::<Status>()
                .map(|s| format!("`{s}`"))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
impl std::error::Error for ParseTaskStatusError {}

/// The type of the task.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Sequence,
    PartialOrd,
    Ord,
    ToSchema,
)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase", example = "documentAdditionOrUpdate")]
pub enum Kind {
    DocumentAdditionOrUpdate,
    DocumentEdition,
    DocumentDeletion,
    SettingsUpdate,
    IndexCreation,
    IndexDeletion,
    IndexUpdate,
    IndexSwap,
    TaskCancelation,
    TaskDeletion,
    DumpCreation,
    SnapshotCreation,
    Export,
    UpgradeDatabase,
    IndexCompaction,
    NetworkTopologyChange,
}

impl Kind {
    pub fn related_to_one_index(&self) -> bool {
        match self {
            Kind::DocumentAdditionOrUpdate
            | Kind::DocumentEdition
            | Kind::DocumentDeletion
            | Kind::SettingsUpdate
            | Kind::IndexCreation
            | Kind::IndexDeletion
            | Kind::IndexUpdate
            | Kind::IndexCompaction => true,
            Kind::IndexSwap
            | Kind::TaskCancelation
            | Kind::TaskDeletion
            | Kind::DumpCreation
            | Kind::Export
            | Kind::UpgradeDatabase
            | Kind::NetworkTopologyChange
            | Kind::SnapshotCreation => false,
        }
    }
}
impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Kind::DocumentAdditionOrUpdate => write!(f, "documentAdditionOrUpdate"),
            Kind::DocumentEdition => write!(f, "documentEdition"),
            Kind::DocumentDeletion => write!(f, "documentDeletion"),
            Kind::SettingsUpdate => write!(f, "settingsUpdate"),
            Kind::IndexCreation => write!(f, "indexCreation"),
            Kind::IndexDeletion => write!(f, "indexDeletion"),
            Kind::IndexUpdate => write!(f, "indexUpdate"),
            Kind::IndexSwap => write!(f, "indexSwap"),
            Kind::TaskCancelation => write!(f, "taskCancelation"),
            Kind::TaskDeletion => write!(f, "taskDeletion"),
            Kind::DumpCreation => write!(f, "dumpCreation"),
            Kind::SnapshotCreation => write!(f, "snapshotCreation"),
            Kind::Export => write!(f, "export"),
            Kind::UpgradeDatabase => write!(f, "upgradeDatabase"),
            Kind::IndexCompaction => write!(f, "indexCompaction"),
            Kind::NetworkTopologyChange => write!(f, "networkTopologyChange"),
        }
    }
}
impl FromStr for Kind {
    type Err = ParseTaskKindError;

    fn from_str(kind: &str) -> Result<Self, Self::Err> {
        if kind.eq_ignore_ascii_case("indexCreation") {
            Ok(Kind::IndexCreation)
        } else if kind.eq_ignore_ascii_case("indexUpdate") {
            Ok(Kind::IndexUpdate)
        } else if kind.eq_ignore_ascii_case("indexSwap") {
            Ok(Kind::IndexSwap)
        } else if kind.eq_ignore_ascii_case("indexDeletion") {
            Ok(Kind::IndexDeletion)
        } else if kind.eq_ignore_ascii_case("documentAdditionOrUpdate") {
            Ok(Kind::DocumentAdditionOrUpdate)
        } else if kind.eq_ignore_ascii_case("documentEdition") {
            Ok(Kind::DocumentEdition)
        } else if kind.eq_ignore_ascii_case("documentDeletion") {
            Ok(Kind::DocumentDeletion)
        } else if kind.eq_ignore_ascii_case("settingsUpdate") {
            Ok(Kind::SettingsUpdate)
        } else if kind.eq_ignore_ascii_case("taskCancelation") {
            Ok(Kind::TaskCancelation)
        } else if kind.eq_ignore_ascii_case("taskDeletion") {
            Ok(Kind::TaskDeletion)
        } else if kind.eq_ignore_ascii_case("dumpCreation") {
            Ok(Kind::DumpCreation)
        } else if kind.eq_ignore_ascii_case("snapshotCreation") {
            Ok(Kind::SnapshotCreation)
        } else if kind.eq_ignore_ascii_case("export") {
            Ok(Kind::Export)
        } else if kind.eq_ignore_ascii_case("upgradeDatabase") {
            Ok(Kind::UpgradeDatabase)
        } else if kind.eq_ignore_ascii_case("indexCompaction") {
            Ok(Kind::IndexCompaction)
        } else if kind.eq_ignore_ascii_case("networkTopologyChange") {
            Ok(Kind::NetworkTopologyChange)
        } else {
            Err(ParseTaskKindError(kind.to_owned()))
        }
    }
}

#[derive(Debug)]
pub struct ParseTaskKindError(pub String);
impl fmt::Display for ParseTaskKindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a valid task type. Available types are {}.",
            self.0,
            enum_iterator::all::<Kind>()
                .map(|k| format!(
                    "`{}`",
                    // by default serde is going to insert `"` around the value.
                    serde_json::to_string(&k).unwrap().trim_matches('"')
                ))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
impl std::error::Error for ParseTaskKindError {}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Details {
    DocumentAdditionOrUpdate {
        received_documents: u64,
        indexed_documents: Option<u64>,
    },
    SettingsUpdate {
        settings: Box<Settings<Unchecked>>,
    },
    IndexInfo {
        primary_key: Option<String>,
        new_index_uid: Option<String>,
        old_index_uid: Option<String>,
    },
    DocumentDeletion {
        provided_ids: usize,
        deleted_documents: Option<u64>,
    },
    DocumentDeletionByFilter {
        original_filter: String,
        deleted_documents: Option<u64>,
    },
    DocumentEdition {
        deleted_documents: Option<u64>,
        edited_documents: Option<u64>,
        original_filter: Option<String>,
        context: Option<Object>,
        function: String,
    },
    ClearAll {
        deleted_documents: Option<u64>,
    },
    TaskCancelation {
        matched_tasks: u64,
        canceled_tasks: Option<u64>,
        original_filter: String,
    },
    TaskDeletion {
        matched_tasks: u64,
        deleted_tasks: Option<u64>,
        original_filter: String,
    },
    Dump {
        dump_uid: Option<String>,
    },
    IndexSwap {
        swaps: Vec<IndexSwap>,
    },
    Export {
        url: String,
        api_key: Option<String>,
        payload_size: Option<Byte>,
        indexes: BTreeMap<IndexUidPattern, DetailsExportIndexSettings>,
    },
    UpgradeDatabase {
        from: (u32, u32, u32),
        to: (u32, u32, u32),
    },
    IndexCompaction {
        index_uid: String,
        pre_compaction_size: Option<Byte>,
        post_compaction_size: Option<Byte>,
    },
    NetworkTopologyChange {
        moved_documents: u64,
        message: String,
    },
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, ToSchema)]
#[schema(rename_all = "camelCase")]
pub struct DetailsExportIndexSettings {
    #[serde(flatten)]
    pub settings: ExportIndexSettings,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_documents: Option<u64>,
}

impl From<ExportIndexSettings> for DetailsExportIndexSettings {
    fn from(settings: ExportIndexSettings) -> Self {
        DetailsExportIndexSettings { settings, matched_documents: None }
    }
}

impl Details {
    pub fn to_failed(&self) -> Self {
        let mut details = self.clone();
        match &mut details {
            Self::DocumentAdditionOrUpdate { indexed_documents, .. } => {
                *indexed_documents = Some(0)
            }
            Self::DocumentEdition { edited_documents, .. } => *edited_documents = Some(0),
            Self::DocumentDeletion { deleted_documents, .. } => *deleted_documents = Some(0),
            Self::DocumentDeletionByFilter { deleted_documents, .. } => {
                *deleted_documents = Some(0)
            }
            Self::ClearAll { deleted_documents } => *deleted_documents = Some(0),
            Self::TaskCancelation { canceled_tasks, .. } => *canceled_tasks = Some(0),
            Self::TaskDeletion { deleted_tasks, .. } => *deleted_tasks = Some(0),
            Self::IndexCompaction { pre_compaction_size, post_compaction_size, .. } => {
                *pre_compaction_size = None;
                *post_compaction_size = None;
            }
            Self::SettingsUpdate { .. }
            | Self::IndexInfo { .. }
            | Self::Dump { .. }
            | Self::Export { .. }
            | Self::UpgradeDatabase { .. }
            | Self::IndexSwap { .. } => (),
            Self::NetworkTopologyChange { moved_documents: _, message } => {
                *message = format!("Failed. Previous status: {}", message);
            }
        }

        details
    }
}

#[derive(Default, Debug, Clone)]
pub enum BatchStopReason {
    #[default]
    Unspecified,
    TaskCannotBeBatched {
        kind: Kind,
        id: TaskId,
    },
    TaskKindCannotBeBatched {
        kind: Kind,
    },
    ExhaustedEnqueuedTasks,
    ExhaustedEnqueuedTasksForIndex {
        index: String,
    },
    ReachedTaskLimit {
        task_limit: usize,
    },
    ReachedSizeLimit {
        size_limit: u64,
        size: u64,
    },
    PrimaryKeyIndexMismatch {
        id: TaskId,
        in_index: String,
        in_task: String,
    },
    IndexCreationMismatch {
        id: TaskId,
    },
    PrimaryKeyMismatch {
        id: TaskId,
        reason: PrimaryKeyMismatchReason,
    },
    IndexDeletion {
        id: TaskId,
    },
    DocumentOperationWithSettings {
        id: TaskId,
    },
    DocumentOperationWithDeletionByFilter {
        id: TaskId,
    },
    DeletionByFilterWithDocumentOperation {
        id: TaskId,
    },
    SettingsWithDocumentOperation {
        id: TaskId,
    },
    NetworkTask {
        id: TaskId,
    },
    NetworkTaskOlderTasks {
        id: TaskId,
        inner_reason: String,
    },
    NetworkTaskImportTasks {
        id: TaskId,
        inner_reason: String,
    },
}

impl BatchStopReason {
    pub fn replace_unspecified(&mut self, new: BatchStopReason) {
        if let BatchStopReason::Unspecified = self {
            *self = new;
        }
    }
}

#[derive(Debug, Clone)]
pub enum PrimaryKeyMismatchReason {
    TaskPrimaryKeyDifferFromIndexPrimaryKey { task_pk: String, index_pk: String },
    TaskPrimaryKeyDifferFromCurrentBatchPrimaryKey { task_pk: String, batch_pk: String },
    CannotInterfereWithPrimaryKeyGuessing { task_pk: String },
}

impl Display for BatchStopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchStopReason::Unspecified => f.write_str("unspecified"),
            BatchStopReason::TaskKindCannotBeBatched { kind } => {
                write!(f, "stopped after the last task of type `{kind}` because they cannot be batched with tasks of any other type.")
            }
            BatchStopReason::TaskCannotBeBatched { kind, id } => {
                write!(f, "created batch containing only task with id {id} of type `{kind}` that cannot be batched with any other task.")
            }
            BatchStopReason::ExhaustedEnqueuedTasks => f.write_str("batched all enqueued tasks"),
            BatchStopReason::ExhaustedEnqueuedTasksForIndex { index } => {
                write!(f, "batched all enqueued tasks for index `{index}`")
            }
            BatchStopReason::ReachedTaskLimit { task_limit } => {
                write!(f, "batched up to configured batch limit of {task_limit} tasks")
            }
            BatchStopReason::ReachedSizeLimit { size_limit, size } => write!(
                f,
                "batched up to configured batch size limit of {size_limit}B with a total of {size}B",
            ),
            BatchStopReason::PrimaryKeyIndexMismatch { id, in_index, in_task } => {
                write!(f, "stopped batching before task with id {id} because its primary key `{in_task}` is different from the primary key of the index `{in_index}`")
            }
            BatchStopReason::IndexCreationMismatch { id } => {
                write!(f, "stopped batching before task with id {id} because its index creation rules differ from the ones from the batch")
            }
            BatchStopReason::PrimaryKeyMismatch { reason, id } => match reason {
                PrimaryKeyMismatchReason::TaskPrimaryKeyDifferFromIndexPrimaryKey {
                    task_pk,
                    index_pk,
                } => {
                    write!(f, "stopped batching before task with id {id} because its primary key `{task_pk}` is different from the primary key of the index `{index_pk}`")
                }
                PrimaryKeyMismatchReason::TaskPrimaryKeyDifferFromCurrentBatchPrimaryKey {
                    task_pk,
                    batch_pk,
                } => {
                    write!(f, "stopped batching before task with id {id} because its primary key `{task_pk}` is different from the primary key of the batch `{batch_pk}`")
                }
                PrimaryKeyMismatchReason::CannotInterfereWithPrimaryKeyGuessing { task_pk } => {
                    write!(f, "stopped batching before task with id {id} because it is setting the `{task_pk}` primary key and it would interfere with primary key guessing of the batch")
                }
            },
            BatchStopReason::IndexDeletion { id } => {
                write!(f, "stopped after task with id {id} because it deletes the index")
            }
            BatchStopReason::DocumentOperationWithSettings { id } => {
                write!(
                    f,
                    "stopped before task with id {id} because it is a settings change which cannot be batched with document operations"
                )
            }
            BatchStopReason::DocumentOperationWithDeletionByFilter { id } => {
                write!(
                    f,
                    "stopped before task with id {id} because it is a deletion by filter which cannot be batched with document operations"
                )
            }
            BatchStopReason::DeletionByFilterWithDocumentOperation { id } => {
                write!(
                    f,
                    "stopped before task with id {id} because it is a document operation which cannot be batched with deletions by filter"
                )
            }
            BatchStopReason::SettingsWithDocumentOperation { id } => {
                write!(
                    f,
                    "stopped before task with id {id} because it is a document operation which cannot be batched with settings changes"
                )
            }
            BatchStopReason::NetworkTask { id } => {
                write!(
                    f,
                    "stopped after task with id {id} because it is a network topology change task"
                )
            }
            BatchStopReason::NetworkTaskOlderTasks { id, inner_reason } => {
                write!(
                    f,
                    "stopped after batching network task with id {id} and a batch of older tasks: {inner_reason}"
                )
            }
            BatchStopReason::NetworkTaskImportTasks { id, inner_reason } => {
                write!(
                    f,
                    "stopped after batching network task with id {id} and a batch of import tasks: {inner_reason}"
                )
            }
        }
    }
}

/// Serialize a `time::Duration` as a best effort ISO 8601 while waiting for
/// https://github.com/time-rs/time/issues/378.
/// This code is a port of the old code of time that was removed in 0.2.
pub fn serialize_duration<S: Serializer>(
    duration: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match duration {
        Some(duration) => {
            // technically speaking, negative duration is not valid ISO 8601
            if duration.is_negative() {
                return serializer.serialize_none();
            }

            const SECS_PER_DAY: i64 = Duration::DAY.whole_seconds();
            let secs = duration.whole_seconds();
            let days = secs / SECS_PER_DAY;
            let secs = secs - days * SECS_PER_DAY;
            let hasdate = days != 0;
            let nanos = duration.subsec_nanoseconds();
            let hastime = (secs != 0 || nanos != 0) || !hasdate;

            // all the following unwrap can't fail
            let mut res = String::new();
            write!(&mut res, "P").unwrap();

            if hasdate {
                write!(&mut res, "{}D", days).unwrap();
            }

            const NANOS_PER_MILLI: i32 = Duration::MILLISECOND.subsec_nanoseconds();
            const NANOS_PER_MICRO: i32 = Duration::MICROSECOND.subsec_nanoseconds();

            if hastime {
                if nanos == 0 {
                    write!(&mut res, "T{}S", secs).unwrap();
                } else if nanos % NANOS_PER_MILLI == 0 {
                    write!(&mut res, "T{}.{:03}S", secs, nanos / NANOS_PER_MILLI).unwrap();
                } else if nanos % NANOS_PER_MICRO == 0 {
                    write!(&mut res, "T{}.{:06}S", secs, nanos / NANOS_PER_MICRO).unwrap();
                } else {
                    write!(&mut res, "T{}.{:09}S", secs, nanos).unwrap();
                }
            }

            serializer.serialize_str(&res)
        }
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{Details, Kind};
    use crate::heed::types::SerdeJson;
    use crate::heed::{BytesDecode, BytesEncode};

    #[test]
    fn bad_deser() {
        let details = Details::TaskDeletion {
            matched_tasks: 1,
            deleted_tasks: None,
            original_filter: "hello".to_owned(),
        };
        let serialised = SerdeJson::<Details>::bytes_encode(&details).unwrap();
        let deserialised = SerdeJson::<Details>::bytes_decode(&serialised).unwrap();
        meili_snap::snapshot!(format!("{:?}", details), @r###"TaskDeletion { matched_tasks: 1, deleted_tasks: None, original_filter: "hello" }"###);
        meili_snap::snapshot!(format!("{:?}", deserialised), @r###"TaskDeletion { matched_tasks: 1, deleted_tasks: None, original_filter: "hello" }"###);
    }

    #[test]
    fn all_kind_can_be_from_str() {
        for kind in enum_iterator::all::<Kind>() {
            let s = kind.to_string();
            let k = Kind::from_str(&s).map_err(|e| format!("Could not from_str {s}: {e}")).unwrap();
            assert_eq!(kind, k, "{kind}.to_string() returned {s} which was parsed as {k}");
        }
    }
}

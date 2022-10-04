use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use super::settings::{Settings, Unchecked};

pub type TaskId = u32;
pub type BatchId = u32;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Task {
    pub id: TaskId,
    /// The name of the index the task is targeting. If it isn't targeting any index (i.e Dump task)
    /// then this is None
    // TODO: when next forward breaking dumps, it would be a good idea to move this field inside of
    // the TaskContent.
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum TaskContent {
    DocumentAddition {
        index_uid: IndexUid,
        content_uuid: Uuid,
        merge_strategy: IndexDocumentsMethod,
        primary_key: Option<String>,
        documents_count: usize,
        allow_index_creation: bool,
    },
    DocumentDeletion {
        index_uid: IndexUid,
        deletion: DocumentDeletion,
    },
    SettingsUpdate {
        index_uid: IndexUid,
        settings: Settings<Unchecked>,
        /// Indicates whether the task was a deletion
        is_deletion: bool,
        allow_index_creation: bool,
    },
    IndexDeletion {
        index_uid: IndexUid,
    },
    IndexCreation {
        index_uid: IndexUid,
        primary_key: Option<String>,
    },
    IndexUpdate {
        index_uid: IndexUid,
        primary_key: Option<String>,
    },
    Dump {
        uid: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexUid(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexDocumentsMethod {
    /// Replace the previous document with the new one,
    /// removing all the already known attributes.
    ReplaceDocuments,

    /// Merge the previous version of the document with the new version,
    /// replacing old attributes values with the new ones and add the new attributes.
    UpdateDocuments,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DocumentDeletion {
    Clear,
    Ids(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskEvent {
    Created(#[serde(with = "time::serde::rfc3339")] OffsetDateTime),
    Batched {
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
        batch_id: BatchId,
    },
    Processing(#[serde(with = "time::serde::rfc3339")] OffsetDateTime),
    Succeeded {
        result: TaskResult,
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
    },
    Failed {
        error: ResponseError,
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskResult {
    DocumentAddition { indexed_documents: u64 },
    DocumentDeletion { deleted_documents: u64 },
    ClearAll { deleted_documents: u64 },
    Other,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseError {
    message: String,
    #[serde(rename = "code")]
    error_code: String,
    #[serde(rename = "type")]
    error_type: String,
    #[serde(rename = "link")]
    error_link: String,
}

impl Task {
    /// Return true when a task is finished.
    /// A task is finished when its last state is either `Succeeded` or `Failed`.
    pub fn is_finished(&self) -> bool {
        self.events.last().map_or(false, |event| {
            matches!(
                event,
                TaskEvent::Succeeded { .. } | TaskEvent::Failed { .. }
            )
        })
    }

    /// Return the content_uuid of the `Task` if there is one.
    pub fn get_content_uuid(&self) -> Option<Uuid> {
        match self {
            Task {
                content: TaskContent::DocumentAddition { content_uuid, .. },
                ..
            } => Some(*content_uuid),
            _ => None,
        }
    }

    pub fn index_uid(&self) -> Option<&str> {
        match &self.content {
            TaskContent::DocumentAddition { index_uid, .. }
            | TaskContent::DocumentDeletion { index_uid, .. }
            | TaskContent::SettingsUpdate { index_uid, .. }
            | TaskContent::IndexDeletion { index_uid }
            | TaskContent::IndexCreation { index_uid, .. }
            | TaskContent::IndexUpdate { index_uid, .. } => Some(index_uid.as_str()),
            TaskContent::Dump { .. } => None,
        }
    }
}

impl IndexUid {
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Return a reference over the inner str.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for IndexUid {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

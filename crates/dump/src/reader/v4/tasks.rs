use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use super::errors::ResponseError;
use super::meta::IndexUid;
use super::settings::{Settings, Unchecked};

pub type TaskId = u32;
pub type BatchId = u32;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Task {
    pub id: TaskId,
    pub index_uid: IndexUid,
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
#[allow(clippy::large_enum_variant)]
pub enum TaskContent {
    DocumentAddition {
        content_uuid: Uuid,
        merge_strategy: IndexDocumentsMethod,
        primary_key: Option<String>,
        documents_count: usize,
        allow_index_creation: bool,
    },
    DocumentDeletion(DocumentDeletion),
    SettingsUpdate {
        settings: Settings<Unchecked>,
        /// Indicates whether the task was a deletion
        is_deletion: bool,
        allow_index_creation: bool,
    },
    IndexDeletion,
    IndexCreation {
        primary_key: Option<String>,
    },
    IndexUpdate {
        primary_key: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum IndexDocumentsMethod {
    /// Replace the previous document with the new one,
    /// removing all the already known attributes.
    ReplaceDocuments,

    /// Merge the previous version of the document with the new version,
    /// replacing old attributes values with the new ones and add the new attributes.
    UpdateDocuments,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum DocumentDeletion {
    Clear,
    Ids(Vec<String>),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum TaskEvent {
    Created(#[serde(with = "time::serde::rfc3339")] OffsetDateTime),
    Batched {
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
        batch_id: BatchId,
    },
    Processing(#[serde(with = "time::serde::rfc3339")] OffsetDateTime),
    Succeded {
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum TaskResult {
    DocumentAddition { indexed_documents: u64 },
    DocumentDeletion { deleted_documents: u64 },
    ClearAll { deleted_documents: u64 },
    Other,
}

impl Task {
    /// Return true when a task is finished.
    /// A task is finished when its last state is either `Succeeded` or `Failed`.
    pub fn is_finished(&self) -> bool {
        self.events.last().map_or(false, |event| {
            matches!(event, TaskEvent::Succeded { .. } | TaskEvent::Failed { .. })
        })
    }

    pub fn processed_at(&self) -> Option<OffsetDateTime> {
        match self.events.last() {
            Some(TaskEvent::Succeded { result: _, timestamp }) => Some(*timestamp),
            _ => None,
        }
    }

    pub fn created_at(&self) -> Option<OffsetDateTime> {
        match &self.content {
            TaskContent::IndexCreation { primary_key: _ } => match self.events.first() {
                Some(TaskEvent::Created(ts)) => Some(*ts),
                _ => None,
            },
            TaskContent::DocumentAddition {
                content_uuid: _,
                merge_strategy: _,
                primary_key: _,
                documents_count: _,
                allow_index_creation: _,
            } => match self.events.first() {
                Some(TaskEvent::Created(ts)) => Some(*ts),
                _ => None,
            },
            TaskContent::SettingsUpdate {
                settings: _,
                is_deletion: _,
                allow_index_creation: _,
            } => match self.events.first() {
                Some(TaskEvent::Created(ts)) => Some(*ts),
                _ => None,
            },
            _ => None,
        }
    }

    /// Return the content_uuid of the `Task` if there is one.
    pub fn get_content_uuid(&self) -> Option<Uuid> {
        match self {
            Task { content: TaskContent::DocumentAddition { content_uuid, .. }, .. } => {
                Some(*content_uuid)
            }
            _ => None,
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

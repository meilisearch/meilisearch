use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use milli::update::IndexDocumentsMethod;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::index::{Settings, Unchecked};
use crate::tasks::batch::BatchId;
use crate::tasks::task::{
    DocumentDeletion, TaskContent as NewTaskContent, TaskEvent as NewTaskEvent, TaskId, TaskResult,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub index_uid: IndexUid,
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

impl From<Task> for crate::tasks::task::Task {
    fn from(other: Task) -> Self {
        Self {
            id: other.id,
            content: NewTaskContent::from((other.index_uid, other.content)),
            events: other.events.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

impl From<TaskEvent> for NewTaskEvent {
    fn from(other: TaskEvent) -> Self {
        match other {
            TaskEvent::Created(x) => NewTaskEvent::Created(x),
            TaskEvent::Batched {
                timestamp,
                batch_id,
            } => NewTaskEvent::Batched {
                timestamp,
                batch_id,
            },
            TaskEvent::Processing(x) => NewTaskEvent::Processing(x),
            TaskEvent::Succeded { result, timestamp } => {
                NewTaskEvent::Succeeded { result, timestamp }
            }
            TaskEvent::Failed { error, timestamp } => NewTaskEvent::Failed { error, timestamp },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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
    Dump {
        uid: String,
    },
}

impl From<(IndexUid, TaskContent)> for NewTaskContent {
    fn from((index_uid, content): (IndexUid, TaskContent)) -> Self {
        match content {
            TaskContent::DocumentAddition {
                content_uuid,
                merge_strategy,
                primary_key,
                documents_count,
                allow_index_creation,
            } => NewTaskContent::DocumentAddition {
                index_uid,
                content_uuid,
                merge_strategy,
                primary_key,
                documents_count,
                allow_index_creation,
            },
            TaskContent::DocumentDeletion(deletion) => NewTaskContent::DocumentDeletion {
                index_uid,
                deletion,
            },
            TaskContent::SettingsUpdate {
                settings,
                is_deletion,
                allow_index_creation,
            } => NewTaskContent::SettingsUpdate {
                index_uid,
                settings,
                is_deletion,
                allow_index_creation,
            },
            TaskContent::IndexDeletion => NewTaskContent::IndexDeletion { index_uid },
            TaskContent::IndexCreation { primary_key } => NewTaskContent::IndexCreation {
                index_uid,
                primary_key,
            },
            TaskContent::IndexUpdate { primary_key } => NewTaskContent::IndexUpdate {
                index_uid,
                primary_key,
            },
            TaskContent::Dump { uid } => NewTaskContent::Dump { uid },
        }
    }
}

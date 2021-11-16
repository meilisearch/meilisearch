use chrono::{DateTime, Utc};
use meilisearch_error::ResponseError;
use milli::update::{DocumentAdditionResult, IndexDocumentsMethod};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{index::{Settings, Unchecked}, index_resolver::IndexUid};
use super::batch::BatchId;

pub type TaskId = u64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskResult {
    DocumentAddition {
        number_of_documents: usize,
    },
    DocumentDeletion {
        number_of_documents: u64,
    },
    Other,
}

impl From<DocumentAdditionResult> for TaskResult {
    fn from(other : DocumentAdditionResult) -> Self {
        Self::DocumentAddition {
            number_of_documents: other.nb_documents,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskEvent {
    Created(DateTime<Utc>),
    Batched {
        timestamp: DateTime<Utc>,
        batch_id: BatchId,
    },
    Processing(DateTime<Utc>),
    Succeded {
        result: TaskResult,
        timestamp: DateTime<Utc>,
    },
    Failed {
        error: ResponseError,
        timestamp: DateTime<Utc>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: TaskId,
    pub index_uid: IndexUid,
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum DocumentDeletion {
    Clear,
    Ids(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TaskContent {
    DocumentAddition {
        content_uuid: Uuid,
        merge_strategy: IndexDocumentsMethod,
        primary_key: Option<String>,
        documents_count: usize,
    },
    DocumentDeletion(DocumentDeletion),
    SettingsUpdate {
        settings: Settings<Unchecked>,
        /// Indicates whether the task was a deletion
        is_deletion: bool,
    },
    IndexDeletion,
    CreateIndex {
        primary_key: Option<String>,
    },
    UpdateIndex {
        primary_key: Option<String>,
    },
}

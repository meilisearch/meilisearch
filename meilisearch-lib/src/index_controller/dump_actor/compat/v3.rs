use chrono::{DateTime, Utc};
use meilisearch_error::{Code, ResponseError};
use milli::update::IndexDocumentsMethod;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::index::{Settings, Unchecked};
use crate::index_resolver::IndexUid;
use crate::tasks::task::{DocumentDeletion, Task, TaskContent, TaskEvent, TaskResult};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum UpdateStatus {
    Processing(Processing),
    Enqueued(Enqueued),
    Processed(Processed),
    Failed(Failed),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocumentAdditionResult {
    pub nb_documents: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateResult {
    DocumentsAddition(DocumentAdditionResult),
    DocumentDeletion { deleted: u64 },
    Other,
}

impl From<UpdateResult> for TaskResult {
    fn from(other: UpdateResult) -> Self {
        match other {
            UpdateResult::DocumentsAddition(result) => TaskResult::DocumentAddition {
                indexed_documents: result.nb_documents as u64,
            },
            UpdateResult::DocumentDeletion { deleted } => TaskResult::DocumentDeletion {
                deleted_documents: deleted,
            },
            UpdateResult::Other => TaskResult::Other,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Update {
    DeleteDocuments(Vec<String>),
    DocumentAddition {
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_uuid: Uuid,
    },
    Settings(Settings<Unchecked>),
    ClearDocuments,
}

impl From<Update> for TaskContent {
    fn from(other: Update) -> Self {
        match other {
            Update::DeleteDocuments(ids) => {
                TaskContent::DocumentDeletion(DocumentDeletion::Ids(ids))
            }
            Update::DocumentAddition {
                primary_key,
                method,
                ..
            } => TaskContent::DocumentAddition {
                content_uuid: Uuid::default(),
                merge_strategy: method,
                primary_key,
                // document count is unknown for legacy updates
                documents_count: 0,
            },
            Update::Settings(settings) => TaskContent::SettingsUpdate {
                settings,
                // There is no way to know now, so we assume it isn't
                is_deletion: false,
            },
            Update::ClearDocuments => TaskContent::DocumentDeletion(DocumentDeletion::Clear),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMeta {
    DocumentsAddition {
        method: IndexDocumentsMethod,
        primary_key: Option<String>,
    },
    ClearDocuments,
    DeleteDocuments {
        ids: Vec<String>,
    },
    Settings(Settings<Unchecked>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Enqueued {
    pub update_id: u64,
    pub meta: Update,
    pub enqueued_at: DateTime<Utc>,
}

impl Enqueued {
    fn update_task(self, task: &mut Task) {
        task.id = self.update_id;
        task.content = self.meta.into();
        task.events.push(TaskEvent::Created(self.enqueued_at));
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Processed {
    pub success: UpdateResult,
    pub processed_at: DateTime<Utc>,
    #[serde(flatten)]
    pub from: Processing,
}

impl Processed {
    fn update_task(self, task: &mut Task) {
        self.from.update_task(task);

        let event = TaskEvent::Succeded {
            result: TaskResult::from(self.success),
            timestamp: self.processed_at,
        };
        task.events.push(event);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Processing {
    #[serde(flatten)]
    pub from: Enqueued,
    pub started_processing_at: DateTime<Utc>,
}

impl Processing {
    fn update_task(self, task: &mut Task) {
        self.from.update_task(task);

        let event = TaskEvent::Processing(self.started_processing_at);
        task.events.push(event);
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Failed {
    #[serde(flatten)]
    pub from: Processing,
    pub msg: String,
    pub code: Code,
    pub failed_at: DateTime<Utc>,
}

impl Failed {
    fn update_task(self, task: &mut Task) {
        self.from.update_task(task);

        let event = TaskEvent::Failed {
            error: ResponseError::from_msg(self.msg, self.code),
            timestamp: self.failed_at,
        };
        task.events.push(event);
    }
}

impl From<(UpdateStatus, String)> for Task {
    fn from((update, uid): (UpdateStatus, String)) -> Self {
        // Dummy task
        let mut task = Task {
            id: 0,
            index_uid: IndexUid::new(uid).unwrap(),
            content: TaskContent::IndexDeletion,
            events: Vec::new(),
        };

        match update {
            UpdateStatus::Processing(u) => u.update_task(&mut task),
            UpdateStatus::Enqueued(u) => u.update_task(&mut task),
            UpdateStatus::Processed(u) => u.update_task(&mut task),
            UpdateStatus::Failed(u) => u.update_task(&mut task),
        }

        task
    }
}

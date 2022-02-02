use chrono::{DateTime, Duration, Utc};
use meilisearch_error::ResponseError;
use meilisearch_lib::index::{Settings, Unchecked};
use meilisearch_lib::milli::update::IndexDocumentsMethod;
use meilisearch_lib::tasks::batch::BatchId;
use meilisearch_lib::tasks::task::{
    DocumentDeletion, Task, TaskContent, TaskEvent, TaskId, TaskResult,
};
use serde::{Serialize, Serializer};

use crate::AUTOBATCHING_ENABLED;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum TaskType {
    IndexCreation,
    IndexUpdate,
    IndexDeletion,
    DocumentAddition,
    DocumentPartial,
    DocumentDeletion,
    SettingsUpdate,
    ClearAll,
}

impl From<TaskContent> for TaskType {
    fn from(other: TaskContent) -> Self {
        match other {
            TaskContent::DocumentAddition {
                merge_strategy: IndexDocumentsMethod::ReplaceDocuments,
                ..
            } => TaskType::DocumentAddition,
            TaskContent::DocumentAddition {
                merge_strategy: IndexDocumentsMethod::UpdateDocuments,
                ..
            } => TaskType::DocumentPartial,
            TaskContent::DocumentDeletion(DocumentDeletion::Clear) => TaskType::ClearAll,
            TaskContent::DocumentDeletion(DocumentDeletion::Ids(_)) => TaskType::DocumentDeletion,
            TaskContent::SettingsUpdate { .. } => TaskType::SettingsUpdate,
            TaskContent::IndexDeletion => TaskType::IndexDeletion,
            TaskContent::IndexCreation { .. } => TaskType::IndexCreation,
            TaskContent::IndexUpdate { .. } => TaskType::IndexUpdate,
            _ => unreachable!("unexpected task type"),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum TaskStatus {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
enum TaskDetails {
    #[serde(rename_all = "camelCase")]
    DocumentAddition {
        received_documents: usize,
        indexed_documents: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    Settings {
        #[serde(flatten)]
        settings: Settings<Unchecked>,
    },
    #[serde(rename_all = "camelCase")]
    IndexInfo { primary_key: Option<String> },
    #[serde(rename_all = "camelCase")]
    DocumentDeletion {
        received_document_ids: usize,
        deleted_documents: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    ClearAll { deleted_documents: Option<u64> },
}

fn serialize_duration<S: Serializer>(
    duration: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match duration {
        Some(duration) => {
            let duration_str = duration.to_string();
            serializer.serialize_str(&duration_str)
        }
        None => serializer.serialize_none(),
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskView {
    uid: TaskId,
    index_uid: String,
    status: TaskStatus,
    #[serde(rename = "type")]
    task_type: TaskType,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<TaskDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ResponseError>,
    #[serde(serialize_with = "serialize_duration")]
    duration: Option<Duration>,
    enqueued_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_uid: Option<Option<BatchId>>,
}

impl From<Task> for TaskView {
    fn from(task: Task) -> Self {
        let Task {
            id,
            index_uid,
            content,
            events,
        } = task;

        let (task_type, mut details) = match content {
            TaskContent::DocumentAddition {
                merge_strategy,
                documents_count,
                ..
            } => {
                let details = TaskDetails::DocumentAddition {
                    received_documents: documents_count,
                    indexed_documents: None,
                };

                let task_type = match merge_strategy {
                    IndexDocumentsMethod::UpdateDocuments => TaskType::DocumentPartial,
                    IndexDocumentsMethod::ReplaceDocuments => TaskType::DocumentAddition,
                    _ => unreachable!("Unexpected document merge strategy."),
                };

                (task_type, Some(details))
            }
            TaskContent::DocumentDeletion(DocumentDeletion::Ids(ids)) => (
                TaskType::DocumentDeletion,
                Some(TaskDetails::DocumentDeletion {
                    received_document_ids: ids.len(),
                    deleted_documents: None,
                }),
            ),
            TaskContent::DocumentDeletion(DocumentDeletion::Clear) => (
                TaskType::ClearAll,
                Some(TaskDetails::ClearAll {
                    deleted_documents: None,
                }),
            ),
            TaskContent::IndexDeletion => (
                TaskType::IndexDeletion,
                Some(TaskDetails::ClearAll {
                    deleted_documents: None,
                }),
            ),
            TaskContent::SettingsUpdate { settings, .. } => (
                TaskType::SettingsUpdate,
                Some(TaskDetails::Settings { settings }),
            ),
            TaskContent::IndexCreation { primary_key } => (
                TaskType::IndexCreation,
                Some(TaskDetails::IndexInfo { primary_key }),
            ),
            TaskContent::IndexUpdate { primary_key } => (
                TaskType::IndexUpdate,
                Some(TaskDetails::IndexInfo { primary_key }),
            ),
        };

        // An event always has at least one event: "Created"
        let (status, error, finished_at) = match events.last().unwrap() {
            TaskEvent::Created(_) => (TaskStatus::Enqueued, None, None),
            TaskEvent::Batched { .. } => (TaskStatus::Enqueued, None, None),
            TaskEvent::Processing(_) => (TaskStatus::Processing, None, None),
            TaskEvent::Succeded { timestamp, result } => {
                match (result, &mut details) {
                    (
                        TaskResult::DocumentAddition {
                            indexed_documents: num,
                            ..
                        },
                        Some(TaskDetails::DocumentAddition {
                            ref mut indexed_documents,
                            ..
                        }),
                    ) => {
                        indexed_documents.replace(*num);
                    }
                    (
                        TaskResult::DocumentDeletion {
                            deleted_documents: docs,
                            ..
                        },
                        Some(TaskDetails::DocumentDeletion {
                            ref mut deleted_documents,
                            ..
                        }),
                    ) => {
                        deleted_documents.replace(*docs);
                    }
                    (
                        TaskResult::ClearAll {
                            deleted_documents: docs,
                        },
                        Some(TaskDetails::ClearAll {
                            ref mut deleted_documents,
                        }),
                    ) => {
                        deleted_documents.replace(*docs);
                    }
                    _ => (),
                }
                (TaskStatus::Succeeded, None, Some(*timestamp))
            }
            TaskEvent::Failed { timestamp, error } => {
                match details {
                    Some(TaskDetails::DocumentDeletion {
                        ref mut deleted_documents,
                        ..
                    }) => {
                        deleted_documents.replace(0);
                    }
                    Some(TaskDetails::ClearAll {
                        ref mut deleted_documents,
                        ..
                    }) => {
                        deleted_documents.replace(0);
                    }
                    Some(TaskDetails::DocumentAddition {
                        ref mut indexed_documents,
                        ..
                    }) => {
                        indexed_documents.replace(0);
                    }
                    _ => (),
                }
                (TaskStatus::Failed, Some(error.clone()), Some(*timestamp))
            }
        };

        let enqueued_at = match events.first() {
            Some(TaskEvent::Created(ts)) => *ts,
            _ => unreachable!("A task must always have a creation event."),
        };

        let started_at = events.iter().find_map(|e| match e {
            TaskEvent::Processing(ts) => Some(*ts),
            _ => None,
        });

        let duration = finished_at.zip(started_at).map(|(tf, ts)| (tf - ts));

        let batch_uid = if AUTOBATCHING_ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
            let id = events.iter().find_map(|e| match e {
                TaskEvent::Batched { batch_id, .. } => Some(*batch_id),
                _ => None,
            });
            Some(id)
        } else {
            None
        };

        Self {
            uid: id,
            index_uid: index_uid.into_inner(),
            status,
            task_type,
            details,
            error,
            duration,
            enqueued_at,
            started_at,
            finished_at,
            batch_uid,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TaskListView {
    results: Vec<TaskView>,
}

impl From<Vec<TaskView>> for TaskListView {
    fn from(results: Vec<TaskView>) -> Self {
        Self { results }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SummarizedTaskView {
    uid: TaskId,
    index_uid: String,
    status: TaskStatus,
    #[serde(rename = "type")]
    task_type: TaskType,
    enqueued_at: DateTime<Utc>,
}

impl From<Task> for SummarizedTaskView {
    fn from(mut other: Task) -> Self {
        let created_event = other
            .events
            .drain(..1)
            .next()
            .expect("Task must have an enqueued event.");

        let enqueued_at = match created_event {
            TaskEvent::Created(ts) => ts,
            _ => unreachable!("The first event of a task must always be 'Created'"),
        };

        Self {
            uid: other.id,
            index_uid: other.index_uid.to_string(),
            status: TaskStatus::Enqueued,
            task_type: other.content.into(),
            enqueued_at,
        }
    }
}

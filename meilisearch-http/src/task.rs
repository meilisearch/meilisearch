use chrono::{DateTime, Duration, Utc};
use meilisearch_error::ResponseError;
use meilisearch_lib::index::{Settings, Unchecked};
use meilisearch_lib::milli::update::IndexDocumentsMethod;
use meilisearch_lib::tasks::task::{
    DocumentDeletion, Task, TaskContent, TaskEvent, TaskId, TaskResult,
};
use serde::{Serialize, Serializer};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum TaskType {
    IndexCreation,
    IndexUpdate,
    IndexDeletion,
    DocumentsAddition,
    DocumentsPartial,
    DocumentsDeletion,
    SettingsUpdate,
    ClearAll,
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
enum TaskDetails {
    #[serde(rename_all = "camelCase")]
    DocumentsAddition {
        received_documents: usize,
        indexed_documents: Option<usize>,
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
pub struct TaskResponse {
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
}

impl From<Task> for TaskResponse {
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
                let details = TaskDetails::DocumentsAddition {
                    received_documents: documents_count,
                    indexed_documents: None,
                };

                let task_type = match merge_strategy {
                    IndexDocumentsMethod::UpdateDocuments => TaskType::DocumentsPartial,
                    IndexDocumentsMethod::ReplaceDocuments => TaskType::DocumentsAddition,
                    _ => unreachable!("Unexpected document merge strategy."),
                };

                (task_type, Some(details))
            }
            TaskContent::DocumentDeletion(DocumentDeletion::Ids(ids)) => (
                TaskType::DocumentsDeletion,
                Some(TaskDetails::DocumentDeletion {
                    received_document_ids: ids.len(),
                    deleted_documents: None,
                }),
            ),
            TaskContent::DocumentDeletion(DocumentDeletion::Clear) => (TaskType::ClearAll, None),
            TaskContent::IndexDeletion => (TaskType::IndexDeletion, None),
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
                            number_of_documents,
                        },
                        Some(TaskDetails::DocumentsAddition {
                            ref mut indexed_documents,
                            ..
                        }),
                    ) => {
                        indexed_documents.replace(*number_of_documents);
                    }
                    (
                        TaskResult::DocumentDeletion {
                            number_of_documents,
                        },
                        Some(TaskDetails::DocumentDeletion {
                            ref mut deleted_documents,
                            ..
                        }),
                    ) => {
                        deleted_documents.replace(*number_of_documents);
                    }
                    _ => (),
                }
                (TaskStatus::Succeeded, None, Some(*timestamp))
            }
            TaskEvent::Failed { timestamp, error } => {
                (TaskStatus::Failed, Some(error.clone()), Some(*timestamp))
            }
        };

        let enqueued_at = match events.first() {
            Some(TaskEvent::Created(ts)) => *ts,
            _ => unreachable!("A task must always have a creation event."),
        };

        let duration = finished_at.map(|ts| (ts - enqueued_at));

        let started_at = events.iter().find_map(|e| match e {
            TaskEvent::Processing(ts) => Some(*ts),
            _ => None,
        });

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
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TaskListResponse {
    results: Vec<TaskResponse>,
}

impl From<Vec<TaskResponse>> for TaskListResponse {
    fn from(results: Vec<TaskResponse>) -> Self {
        Self { results }
    }
}

use std::error::Error;
use std::fmt::{self, Write};
use std::str::FromStr;
use std::write;

use meilisearch_lib::index::{Settings, Unchecked};
use meilisearch_lib::tasks::task::{
    DocumentDeletion, Task, TaskContent, TaskEvent, TaskId, TaskResult,
};
use meilisearch_types::error::ResponseError;
use serde::{Deserialize, Serialize, Serializer};
use time::{Duration, OffsetDateTime};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TaskType {
    IndexCreation,
    IndexUpdate,
    IndexDeletion,
    DocumentAdditionOrUpdate,
    DocumentDeletion,
    SettingsUpdate,
    DumpCreation,
}

impl From<TaskContent> for TaskType {
    fn from(other: TaskContent) -> Self {
        match other {
            TaskContent::IndexCreation { .. } => TaskType::IndexCreation,
            TaskContent::IndexUpdate { .. } => TaskType::IndexUpdate,
            TaskContent::IndexDeletion { .. } => TaskType::IndexDeletion,
            TaskContent::DocumentAddition { .. } => TaskType::DocumentAdditionOrUpdate,
            TaskContent::DocumentDeletion { .. } => TaskType::DocumentDeletion,
            TaskContent::SettingsUpdate { .. } => TaskType::SettingsUpdate,
            TaskContent::Dump { .. } => TaskType::DumpCreation,
        }
    }
}

#[derive(Debug)]
pub struct TaskTypeError {
    invalid_type: String,
}

impl fmt::Display for TaskTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid task type `{}`, expecting one of: \
            indexCreation, indexUpdate, indexDeletion, documentAdditionOrUpdate, \
            documentDeletion, settingsUpdate, dumpCreation",
            self.invalid_type
        )
    }
}

impl Error for TaskTypeError {}

impl FromStr for TaskType {
    type Err = TaskTypeError;

    fn from_str(type_: &str) -> Result<Self, TaskTypeError> {
        if type_.eq_ignore_ascii_case("indexCreation") {
            Ok(TaskType::IndexCreation)
        } else if type_.eq_ignore_ascii_case("indexUpdate") {
            Ok(TaskType::IndexUpdate)
        } else if type_.eq_ignore_ascii_case("indexDeletion") {
            Ok(TaskType::IndexDeletion)
        } else if type_.eq_ignore_ascii_case("documentAdditionOrUpdate") {
            Ok(TaskType::DocumentAdditionOrUpdate)
        } else if type_.eq_ignore_ascii_case("documentDeletion") {
            Ok(TaskType::DocumentDeletion)
        } else if type_.eq_ignore_ascii_case("settingsUpdate") {
            Ok(TaskType::SettingsUpdate)
        } else if type_.eq_ignore_ascii_case("dumpCreation") {
            Ok(TaskType::DumpCreation)
        } else {
            Err(TaskTypeError {
                invalid_type: type_.to_string(),
            })
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TaskStatus {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
}

#[derive(Debug)]
pub struct TaskStatusError {
    invalid_status: String,
}

impl fmt::Display for TaskStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid task status `{}`, expecting one of: \
            enqueued, processing, succeeded, or failed",
            self.invalid_status,
        )
    }
}

impl Error for TaskStatusError {}

impl FromStr for TaskStatus {
    type Err = TaskStatusError;

    fn from_str(status: &str) -> Result<Self, TaskStatusError> {
        if status.eq_ignore_ascii_case("enqueued") {
            Ok(TaskStatus::Enqueued)
        } else if status.eq_ignore_ascii_case("processing") {
            Ok(TaskStatus::Processing)
        } else if status.eq_ignore_ascii_case("succeeded") {
            Ok(TaskStatus::Succeeded)
        } else if status.eq_ignore_ascii_case("failed") {
            Ok(TaskStatus::Failed)
        } else {
            Err(TaskStatusError {
                invalid_status: status.to_string(),
            })
        }
    }
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
    #[serde(rename_all = "camelCase")]
    Dump { dump_uid: String },
}

/// Serialize a `time::Duration` as a best effort ISO 8601 while waiting for
/// https://github.com/time-rs/time/issues/378.
/// This code is a port of the old code of time that was removed in 0.2.
fn serialize_duration<S: Serializer>(
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskView {
    pub uid: TaskId,
    index_uid: Option<String>,
    status: TaskStatus,
    #[serde(rename = "type")]
    task_type: TaskType,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<TaskDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ResponseError>,
    #[serde(serialize_with = "serialize_duration")]
    duration: Option<Duration>,
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    enqueued_at: OffsetDateTime,
    #[serde(serialize_with = "time::serde::rfc3339::option::serialize")]
    started_at: Option<OffsetDateTime>,
    #[serde(serialize_with = "time::serde::rfc3339::option::serialize")]
    finished_at: Option<OffsetDateTime>,
}

impl From<Task> for TaskView {
    fn from(task: Task) -> Self {
        let index_uid = task.index_uid().map(String::from);
        let Task {
            id,
            content,
            events,
        } = task;

        let (task_type, mut details) = match content {
            TaskContent::DocumentAddition {
                documents_count, ..
            } => {
                let details = TaskDetails::DocumentAddition {
                    received_documents: documents_count,
                    indexed_documents: None,
                };

                (TaskType::DocumentAdditionOrUpdate, Some(details))
            }
            TaskContent::DocumentDeletion {
                deletion: DocumentDeletion::Ids(ids),
                ..
            } => (
                TaskType::DocumentDeletion,
                Some(TaskDetails::DocumentDeletion {
                    received_document_ids: ids.len(),
                    deleted_documents: None,
                }),
            ),
            TaskContent::DocumentDeletion {
                deletion: DocumentDeletion::Clear,
                ..
            } => (
                TaskType::DocumentDeletion,
                Some(TaskDetails::ClearAll {
                    deleted_documents: None,
                }),
            ),
            TaskContent::IndexDeletion { .. } => (
                TaskType::IndexDeletion,
                Some(TaskDetails::ClearAll {
                    deleted_documents: None,
                }),
            ),
            TaskContent::SettingsUpdate { settings, .. } => (
                TaskType::SettingsUpdate,
                Some(TaskDetails::Settings { settings }),
            ),
            TaskContent::IndexCreation { primary_key, .. } => (
                TaskType::IndexCreation,
                Some(TaskDetails::IndexInfo { primary_key }),
            ),
            TaskContent::IndexUpdate { primary_key, .. } => (
                TaskType::IndexUpdate,
                Some(TaskDetails::IndexInfo { primary_key }),
            ),
            TaskContent::Dump { uid } => (
                TaskType::DumpCreation,
                Some(TaskDetails::Dump { dump_uid: uid }),
            ),
        };

        // An event always has at least one event: "Created"
        let (status, error, finished_at) = match events.last().unwrap() {
            TaskEvent::Created(_) => (TaskStatus::Enqueued, None, None),
            TaskEvent::Batched { .. } => (TaskStatus::Enqueued, None, None),
            TaskEvent::Processing(_) => (TaskStatus::Processing, None, None),
            TaskEvent::Succeeded { timestamp, result } => {
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

        Self {
            uid: id,
            index_uid,
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
pub struct TaskListView {
    pub results: Vec<TaskView>,
    pub limit: usize,
    pub from: Option<TaskId>,
    pub next: Option<TaskId>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SummarizedTaskView {
    task_uid: TaskId,
    index_uid: Option<String>,
    status: TaskStatus,
    #[serde(rename = "type")]
    task_type: TaskType,
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    enqueued_at: OffsetDateTime,
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
            task_uid: other.id,
            index_uid: other.index_uid().map(String::from),
            status: TaskStatus::Enqueued,
            task_type: other.content.into(),
            enqueued_at,
        }
    }
}

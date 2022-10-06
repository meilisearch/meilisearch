use std::fmt::Write;

use serde::{Deserialize, Serializer};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use super::{
    meta::IndexUid,
    settings::{Settings, Unchecked},
};

pub type TaskId = u32;
pub type BatchId = u32;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Task {
    pub id: TaskId,
    pub index_uid: IndexUid,
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
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

#[derive(Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
#[cfg_attr(test, serde(untagged))]
#[allow(clippy::large_enum_variant)]
enum TaskDetails {
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    DocumentAddition {
        received_documents: usize,
        indexed_documents: Option<u64>,
    },
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    Settings {
        #[cfg_attr(test, serde(flatten))]
        settings: Settings<Unchecked>,
    },
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    IndexInfo { primary_key: Option<String> },
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    DocumentDeletion {
        received_document_ids: usize,
        deleted_documents: Option<u64>,
    },
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    ClearAll { deleted_documents: Option<u64> },
}

#[derive(Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
#[cfg_attr(test, serde(rename_all = "camelCase"))]
enum TaskStatus {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
}

#[derive(Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
#[cfg_attr(test, serde(rename_all = "camelCase"))]
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

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct ResponseError {
    pub message: String,
    #[serde(rename = "code")]
    pub error_code: String,
    #[serde(rename = "type")]
    pub error_type: String,
    #[serde(rename = "link")]
    pub error_link: String,
}

impl Task {
    /// Return true when a task is finished.
    /// A task is finished when its last state is either `Succeeded` or `Failed`.
    pub fn is_finished(&self) -> bool {
        self.events.last().map_or(false, |event| {
            matches!(event, TaskEvent::Succeded { .. } | TaskEvent::Failed { .. })
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

#[derive(Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
#[cfg_attr(test, serde(rename_all = "camelCase"))]
pub struct TaskView {
    uid: TaskId,
    index_uid: String,
    status: TaskStatus,
    #[cfg_attr(test, serde(rename = "type"))]
    task_type: TaskType,
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    details: Option<TaskDetails>,
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    error: Option<ResponseError>,
    #[cfg_attr(test, serde(serialize_with = "serialize_duration"))]
    duration: Option<Duration>,
    #[cfg_attr(test, serde(serialize_with = "time::serde::rfc3339::serialize"))]
    enqueued_at: OffsetDateTime,
    #[cfg_attr(
        test,
        serde(serialize_with = "time::serde::rfc3339::option::serialize")
    )]
    started_at: Option<OffsetDateTime>,
    #[cfg_attr(
        test,
        serde(serialize_with = "time::serde::rfc3339::option::serialize")
    )]
    finished_at: Option<OffsetDateTime>,
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
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

        let batch_uid = if true {
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

use serde::Deserialize;
use time::{Duration, OffsetDateTime};
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
    /// The name of the index the task is targeting. If it isn't targeting any index (i.e Dump task)
    /// then this is None
    // TODO: when next forward breaking dumps, it would be a good idea to move this field inside of
    // the TaskContent.
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
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
            matches!(event, TaskEvent::Succeeded { .. } | TaskEvent::Failed { .. })
        })
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

    pub fn processed_at(&self) -> Option<OffsetDateTime> {
        match self.events.last() {
            Some(TaskEvent::Succeeded { result: _, timestamp }) => Some(*timestamp),
            _ => None,
        }
    }

    pub fn created_at(&self) -> Option<OffsetDateTime> {
        match &self.content {
            TaskContent::IndexCreation { index_uid: _, primary_key: _ } => {
                match self.events.first() {
                    Some(TaskEvent::Created(ts)) => Some(*ts),
                    _ => None,
                }
            }
            TaskContent::DocumentAddition {
                index_uid: _,
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
                index_uid: _,
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

#[allow(dead_code)] // otherwise rustc complains that the fields go unused
#[derive(Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
#[cfg_attr(test, serde(rename_all = "camelCase"))]
pub struct TaskView {
    pub uid: TaskId,
    pub index_uid: Option<String>,
    pub status: TaskStatus,
    #[cfg_attr(test, serde(rename = "type"))]
    pub task_type: TaskType,
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub details: Option<TaskDetails>,
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub error: Option<ResponseError>,
    #[cfg_attr(test, serde(serialize_with = "serialize_duration"))]
    pub duration: Option<Duration>,
    #[cfg_attr(test, serde(serialize_with = "time::serde::rfc3339::serialize"))]
    pub enqueued_at: OffsetDateTime,
    #[cfg_attr(test, serde(serialize_with = "time::serde::rfc3339::option::serialize"))]
    pub started_at: Option<OffsetDateTime>,
    #[cfg_attr(test, serde(serialize_with = "time::serde::rfc3339::option::serialize"))]
    pub finished_at: Option<OffsetDateTime>,
}

impl From<Task> for TaskView {
    fn from(task: Task) -> Self {
        let index_uid = task.index_uid().map(String::from);
        let Task { id, content, events } = task;

        let (task_type, mut details) = match content {
            TaskContent::DocumentAddition { documents_count, .. } => {
                let details = TaskDetails::DocumentAddition {
                    received_documents: documents_count,
                    indexed_documents: None,
                };

                (TaskType::DocumentAdditionOrUpdate, Some(details))
            }
            TaskContent::DocumentDeletion { deletion: DocumentDeletion::Ids(ids), .. } => (
                TaskType::DocumentDeletion,
                Some(TaskDetails::DocumentDeletion {
                    received_document_ids: ids.len(),
                    deleted_documents: None,
                }),
            ),
            TaskContent::DocumentDeletion { deletion: DocumentDeletion::Clear, .. } => (
                TaskType::DocumentDeletion,
                Some(TaskDetails::ClearAll { deleted_documents: None }),
            ),
            TaskContent::IndexDeletion { .. } => {
                (TaskType::IndexDeletion, Some(TaskDetails::ClearAll { deleted_documents: None }))
            }
            TaskContent::SettingsUpdate { settings, .. } => {
                (TaskType::SettingsUpdate, Some(TaskDetails::Settings { settings }))
            }
            TaskContent::IndexCreation { primary_key, .. } => {
                (TaskType::IndexCreation, Some(TaskDetails::IndexInfo { primary_key }))
            }
            TaskContent::IndexUpdate { primary_key, .. } => {
                (TaskType::IndexUpdate, Some(TaskDetails::IndexInfo { primary_key }))
            }
            TaskContent::Dump { uid } => {
                (TaskType::DumpCreation, Some(TaskDetails::Dump { dump_uid: uid }))
            }
        };

        // An event always has at least one event: "Created"
        let (status, error, finished_at) = match events.last().unwrap() {
            TaskEvent::Created(_) => (TaskStatus::Enqueued, None, None),
            TaskEvent::Batched { .. } => (TaskStatus::Enqueued, None, None),
            TaskEvent::Processing(_) => (TaskStatus::Processing, None, None),
            TaskEvent::Succeeded { timestamp, result } => {
                match (result, &mut details) {
                    (
                        TaskResult::DocumentAddition { indexed_documents: num, .. },
                        Some(TaskDetails::DocumentAddition { ref mut indexed_documents, .. }),
                    ) => {
                        indexed_documents.replace(*num);
                    }
                    (
                        TaskResult::DocumentDeletion { deleted_documents: docs, .. },
                        Some(TaskDetails::DocumentDeletion { ref mut deleted_documents, .. }),
                    ) => {
                        deleted_documents.replace(*docs);
                    }
                    (
                        TaskResult::ClearAll { deleted_documents: docs },
                        Some(TaskDetails::ClearAll { ref mut deleted_documents }),
                    ) => {
                        deleted_documents.replace(*docs);
                    }
                    _ => (),
                }
                (TaskStatus::Succeeded, None, Some(*timestamp))
            }
            TaskEvent::Failed { timestamp, error } => {
                match details {
                    Some(TaskDetails::DocumentDeletion { ref mut deleted_documents, .. }) => {
                        deleted_documents.replace(0);
                    }
                    Some(TaskDetails::ClearAll { ref mut deleted_documents, .. }) => {
                        deleted_documents.replace(0);
                    }
                    Some(TaskDetails::DocumentAddition { ref mut indexed_documents, .. }) => {
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

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
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

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub enum TaskStatus {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
}

#[derive(Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
#[cfg_attr(test, serde(untagged))]
#[allow(clippy::large_enum_variant)]
pub enum TaskDetails {
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    DocumentAddition { received_documents: usize, indexed_documents: Option<u64> },
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    Settings {
        #[cfg_attr(test, serde(flatten))]
        settings: Settings<Unchecked>,
    },
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    IndexInfo { primary_key: Option<String> },
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    DocumentDeletion { received_document_ids: usize, deleted_documents: Option<u64> },
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    ClearAll { deleted_documents: Option<u64> },
    #[cfg_attr(test, serde(rename_all = "camelCase"))]
    Dump { dump_uid: String },
}

/// Serialize a `time::Duration` as a best effort ISO 8601 while waiting for
/// https://github.com/time-rs/time/issues/378.
/// This code is a port of the old code of time that was removed in 0.2.
#[cfg(test)]
fn serialize_duration<S: serde::Serializer>(
    duration: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use std::fmt::Write;

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

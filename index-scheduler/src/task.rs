use anyhow::Result;
use meilisearch_types::error::ResponseError;
use meilisearch_types::milli::update::IndexDocumentsMethod;
use meilisearch_types::settings::{Settings, Unchecked};

use serde::{Deserialize, Serialize, Serializer};
use std::{
    fmt::{Display, Write},
    path::PathBuf,
    str::FromStr,
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::{Error, TaskId};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskView {
    pub uid: TaskId,
    #[serde(default)]
    pub index_uid: Option<String>,
    pub status: Status,
    // TODO use our own Kind for the user
    #[serde(rename = "type")]
    pub kind: Kind,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<DetailsView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,

    #[serde(
        serialize_with = "serialize_duration",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub duration: Option<Duration>,
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub started_at: Option<OffsetDateTime>,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub finished_at: Option<OffsetDateTime>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub uid: TaskId,

    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,

    pub error: Option<ResponseError>,
    pub details: Option<Details>,

    pub status: Status,
    pub kind: KindWithContent,
}

impl Task {
    /// Persist all the temp files associated with the task.
    pub fn persist(&self) -> Result<()> {
        self.kind.persist()
    }

    /// Delete all the files associated with the task.
    pub fn remove_data(&self) -> Result<()> {
        self.kind.remove_data()
    }

    /// Return the list of indexes updated by this tasks.
    pub fn indexes(&self) -> Option<Vec<&str>> {
        self.kind.indexes()
    }

    /// Convert a Task to a TaskView
    pub fn as_task_view(&self) -> TaskView {
        TaskView {
            uid: self.uid,
            index_uid: self
                .indexes()
                .and_then(|vec| vec.first().map(|i| i.to_string())),
            status: self.status,
            kind: self.kind.as_kind(),
            details: self.details.as_ref().map(Details::as_details_view),
            error: self.error.clone(),
            duration: self
                .started_at
                .zip(self.finished_at)
                .map(|(start, end)| end - start),
            enqueued_at: self.enqueued_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
}
impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Enqueued => write!(f, "enqueued"),
            Status::Processing => write!(f, "processing"),
            Status::Succeeded => write!(f, "succeeded"),
            Status::Failed => write!(f, "failed"),
        }
    }
}
impl FromStr for Status {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "enqueued" => Ok(Status::Enqueued),
            "processing" => Ok(Status::Processing),
            "succeeded" => Ok(Status::Succeeded),
            "failed" => Ok(Status::Failed),
            s => Err(Error::InvalidStatus(s.to_string())),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KindWithContent {
    DocumentImport {
        index_uid: String,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_file: Uuid,
        documents_count: u64,
        allow_index_creation: bool,
    },
    DocumentDeletion {
        index_uid: String,
        documents_ids: Vec<String>,
    },
    DocumentClear {
        index_uid: String,
    },
    Settings {
        index_uid: String,
        new_settings: Settings<Unchecked>,
        is_deletion: bool,
        allow_index_creation: bool,
    },
    IndexDeletion {
        index_uid: String,
    },
    IndexCreation {
        index_uid: String,
        primary_key: Option<String>,
    },
    IndexUpdate {
        index_uid: String,
        primary_key: Option<String>,
    },
    IndexSwap {
        lhs: String,
        rhs: String,
    },
    CancelTask {
        tasks: Vec<TaskId>,
    },
    DeleteTasks {
        query: String,
        tasks: Vec<TaskId>,
    },
    DumpExport {
        output: PathBuf,
    },
    Snapshot,
}

impl KindWithContent {
    pub fn as_kind(&self) -> Kind {
        match self {
            KindWithContent::DocumentImport {
                method,
                allow_index_creation,
                ..
            } => Kind::DocumentImport {
                method: *method,
                allow_index_creation: *allow_index_creation,
            },
            KindWithContent::DocumentDeletion { .. } => Kind::DocumentDeletion,
            KindWithContent::DocumentClear { .. } => Kind::DocumentClear,
            KindWithContent::Settings {
                allow_index_creation,
                ..
            } => Kind::Settings {
                allow_index_creation: *allow_index_creation,
            },
            KindWithContent::IndexCreation { .. } => Kind::IndexCreation,
            KindWithContent::IndexDeletion { .. } => Kind::IndexDeletion,
            KindWithContent::IndexUpdate { .. } => Kind::IndexUpdate,
            KindWithContent::IndexSwap { .. } => Kind::IndexSwap,
            KindWithContent::CancelTask { .. } => Kind::CancelTask,
            KindWithContent::DeleteTasks { .. } => Kind::DeleteTasks,
            KindWithContent::DumpExport { .. } => Kind::DumpExport,
            KindWithContent::Snapshot => Kind::Snapshot,
        }
    }

    pub fn persist(&self) -> Result<()> {
        use KindWithContent::*;

        match self {
            DocumentImport { .. } => {
                // TODO: TAMO: persist the file
                // content_file.persist();
                Ok(())
            }
            DocumentDeletion { .. }
            | DocumentClear { .. }
            | Settings { .. }
            | IndexCreation { .. }
            | IndexDeletion { .. }
            | IndexUpdate { .. }
            | IndexSwap { .. }
            | CancelTask { .. }
            | DeleteTasks { .. }
            | DumpExport { .. }
            | Snapshot => Ok(()), // There is nothing to persist for all these tasks
        }
    }

    pub fn remove_data(&self) -> Result<()> {
        use KindWithContent::*;

        match self {
            DocumentImport { .. } => {
                // TODO: TAMO: delete the file
                // content_file.delete();
                Ok(())
            }
            IndexCreation { .. }
            | DocumentDeletion { .. }
            | DocumentClear { .. }
            | Settings { .. }
            | IndexDeletion { .. }
            | IndexUpdate { .. }
            | IndexSwap { .. }
            | CancelTask { .. }
            | DeleteTasks { .. }
            | DumpExport { .. }
            | Snapshot => Ok(()), // There is no data associated with all these tasks
        }
    }

    pub fn indexes(&self) -> Option<Vec<&str>> {
        use KindWithContent::*;

        match self {
            DumpExport { .. } | Snapshot | CancelTask { .. } | DeleteTasks { .. } => None,
            DocumentImport { index_uid, .. }
            | DocumentDeletion { index_uid, .. }
            | DocumentClear { index_uid }
            | Settings { index_uid, .. }
            | IndexCreation { index_uid, .. }
            | IndexUpdate { index_uid, .. }
            | IndexDeletion { index_uid } => Some(vec![index_uid]),
            IndexSwap { lhs, rhs } => Some(vec![lhs, rhs]),
        }
    }

    /// Returns the default `Details` that correspond to this `KindWithContent`,
    /// `None` if it cannot be generated.
    pub fn default_details(&self) -> Option<Details> {
        match self {
            KindWithContent::DocumentImport {
                documents_count, ..
            } => Some(Details::DocumentAddition {
                received_documents: *documents_count,
                indexed_documents: 0,
            }),
            KindWithContent::DocumentDeletion {
                index_uid: _,
                documents_ids,
            } => Some(Details::DocumentDeletion {
                received_document_ids: documents_ids.len(),
                deleted_documents: None,
            }),
            KindWithContent::DocumentClear { .. } => Some(Details::ClearAll {
                deleted_documents: None,
            }),
            KindWithContent::Settings { new_settings, .. } => Some(Details::Settings {
                settings: new_settings.clone(),
            }),
            KindWithContent::IndexDeletion { .. } => None,
            KindWithContent::IndexCreation { primary_key, .. }
            | KindWithContent::IndexUpdate { primary_key, .. } => Some(Details::IndexInfo {
                primary_key: primary_key.clone(),
            }),
            KindWithContent::IndexSwap { .. } => {
                todo!()
            }
            KindWithContent::CancelTask { .. } => {
                None // TODO: check correctness of this return value
            }
            KindWithContent::DeleteTasks { query, tasks } => Some(Details::DeleteTasks {
                matched_tasks: tasks.len(),
                deleted_tasks: None,
                original_query: query.clone(),
            }),
            KindWithContent::DumpExport { .. } => None,
            KindWithContent::Snapshot => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Kind {
    DocumentImport {
        method: IndexDocumentsMethod,
        allow_index_creation: bool,
    },
    DocumentDeletion,
    DocumentClear,
    Settings {
        allow_index_creation: bool,
    },
    IndexCreation,
    IndexDeletion,
    IndexUpdate,
    IndexSwap,
    CancelTask,
    DeleteTasks,
    DumpExport,
    Snapshot,
}

impl FromStr for Kind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "document_addition" => Ok(Kind::DocumentImport {
                method: IndexDocumentsMethod::ReplaceDocuments,
                // TODO this doesn't make sense
                allow_index_creation: false,
            }),
            "document_update" => Ok(Kind::DocumentImport {
                method: IndexDocumentsMethod::UpdateDocuments,
                // TODO this doesn't make sense
                allow_index_creation: false,
            }),
            "document_deletion" => Ok(Kind::DocumentDeletion),
            "document_clear" => Ok(Kind::DocumentClear),
            "settings" => Ok(Kind::Settings {
                // TODO this doesn't make sense
                allow_index_creation: false,
            }),
            "index_creation" => Ok(Kind::IndexCreation),
            "index_deletion" => Ok(Kind::IndexDeletion),
            "index_update" => Ok(Kind::IndexUpdate),
            "index_swap" => Ok(Kind::IndexSwap),
            "cancel_task" => Ok(Kind::CancelTask),
            "delete_tasks" => Ok(Kind::DeleteTasks),
            "dump_export" => Ok(Kind::DumpExport),
            "snapshot" => Ok(Kind::Snapshot),
            s => Err(Error::InvalidKind(s.to_string())),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum Details {
    DocumentAddition {
        received_documents: u64,
        indexed_documents: u64,
    },
    Settings {
        settings: Settings<Unchecked>,
    },
    IndexInfo {
        primary_key: Option<String>,
    },
    DocumentDeletion {
        received_document_ids: usize,
        // TODO why is this optional?
        deleted_documents: Option<u64>,
    },
    ClearAll {
        deleted_documents: Option<u64>,
    },
    DeleteTasks {
        matched_tasks: usize,
        deleted_tasks: Option<usize>,
        original_query: String,
    },
    Dump {
        dump_uid: String,
    },
}
#[derive(Default, Debug, PartialEq, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailsView {
    #[serde(skip_serializing_if = "Option::is_none")]
    received_documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexed_documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_key: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    received_document_ids: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_documents: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_tasks: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_tasks: Option<Option<usize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dump_uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    settings: Option<Settings<Unchecked>>,
}
impl Details {
    fn as_details_view(&self) -> DetailsView {
        match self.clone() {
            Details::DocumentAddition {
                received_documents,
                indexed_documents,
            } => DetailsView {
                received_documents: Some(received_documents),
                indexed_documents: Some(indexed_documents),
                ..DetailsView::default()
            },
            Details::Settings { settings } => DetailsView {
                settings: Some(settings),
                ..DetailsView::default()
            },
            Details::IndexInfo { primary_key } => DetailsView {
                primary_key: Some(primary_key),
                ..DetailsView::default()
            },
            Details::DocumentDeletion {
                received_document_ids,
                deleted_documents,
            } => DetailsView {
                received_document_ids: Some(received_document_ids),
                deleted_documents: Some(deleted_documents),
                ..DetailsView::default()
            },
            Details::ClearAll { deleted_documents } => DetailsView {
                deleted_documents: Some(deleted_documents),
                ..DetailsView::default()
            },
            Details::DeleteTasks {
                matched_tasks,
                deleted_tasks,
                original_query,
            } => DetailsView {
                matched_tasks: Some(matched_tasks),
                deleted_tasks: Some(deleted_tasks),
                original_query: Some(original_query),
                ..DetailsView::default()
            },
            Details::Dump { dump_uid } => DetailsView {
                dump_uid: Some(dump_uid),
                ..DetailsView::default()
            },
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

#[cfg(test)]
mod tests {
    use meilisearch_types::heed::{types::SerdeJson, BytesDecode, BytesEncode};

    use crate::assert_smol_debug_snapshot;

    use super::Details;

    #[test]
    fn bad_deser() {
        let details = Details::DeleteTasks {
            matched_tasks: 1,
            deleted_tasks: None,
            original_query: "hello".to_owned(),
        };
        let serialised = SerdeJson::<Details>::bytes_encode(&details).unwrap();
        let deserialised = SerdeJson::<Details>::bytes_decode(&serialised).unwrap();
        assert_smol_debug_snapshot!(details, @r###"DeleteTasks { matched_tasks: 1, deleted_tasks: None, original_query: "hello" }"###);
        assert_smol_debug_snapshot!(deserialised, @r###"DeleteTasks { matched_tasks: 1, deleted_tasks: None, original_query: "hello" }"###);
    }
}

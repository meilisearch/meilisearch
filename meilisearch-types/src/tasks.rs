use milli::update::IndexDocumentsMethod;
use serde::{Deserialize, Serialize, Serializer};
use std::{
    fmt::{Display, Write},
    str::FromStr,
};
use time::{Duration, OffsetDateTime};

use crate::{
    error::{Code, ResponseError},
    settings::{Settings, Unchecked},
};

pub type TaskId = u32;

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDump {
    pub uid: TaskId,
    #[serde(default)]
    pub index_uid: Option<String>,
    pub status: Status,
    // TODO use our own Kind for the user
    #[serde(rename = "type")]
    pub kind: Kind,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Details>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,

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
    type Err = ResponseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "enqueued" => Ok(Status::Enqueued),
            "processing" => Ok(Status::Processing),
            "succeeded" => Ok(Status::Succeeded),
            "failed" => Ok(Status::Failed),
            s => Err(ResponseError::from_msg(
                format!("`{}` is not a status. Available types are", s),
                Code::BadRequest,
            )),
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
    type Err = ResponseError;

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
            s => Err(ResponseError::from_msg(
                format!("`{}` is not a type. Available status are ", s),
                Code::BadRequest,
            )),
        }
    }
}

#[derive(Default, Debug, PartialEq, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailsView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_document_ids: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_documents: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_tasks: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_tasks: Option<Option<usize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dump_uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub settings: Option<Settings<Unchecked>>,
}

// A `Kind` specific version made for the dump. If modified you may break the dump.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KindDump {
    DocumentImport {
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        documents_count: u64,
        allow_index_creation: bool,
    },
    DocumentDeletion {
        documents_ids: Vec<String>,
    },
    DocumentClear,
    Settings {
        new_settings: Settings<Unchecked>,
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
    DumpExport,
    Snapshot,
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

impl Details {
    pub fn as_details_view(&self) -> DetailsView {
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

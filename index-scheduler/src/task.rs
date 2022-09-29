use anyhow::Result;
use index::{Settings, Unchecked};
use meilisearch_types::error::ResponseError;

use serde::{Deserialize, Serialize, Serializer};
use std::{fmt::Write, path::PathBuf, str::FromStr};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::{Error, TaskId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskView {
    pub uid: TaskId,
    pub index_uid: Option<String>,
    pub status: Status,
    #[serde(rename = "type")]
    pub kind: Kind,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Details>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,

    #[serde(
        serialize_with = "serialize_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub duration: Option<Duration>,
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub started_at: Option<OffsetDateTime>,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
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
            details: self.details.clone(),
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
    DocumentAddition {
        index_uid: String,
        primary_key: Option<String>,
        content_file: Uuid,
        documents_count: usize,
        allow_index_creation: bool,
    },
    DocumentUpdate {
        index_uid: String,
        primary_key: Option<String>,
        content_file: Uuid,
        documents_count: usize,
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
    IndexRename {
        index_uid: String,
        new_name: String,
    },
    IndexSwap {
        lhs: String,
        rhs: String,
    },
    CancelTask {
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
            KindWithContent::DocumentAddition { .. } => Kind::DocumentAddition,
            KindWithContent::DocumentUpdate { .. } => Kind::DocumentUpdate,
            KindWithContent::DocumentDeletion { .. } => Kind::DocumentDeletion,
            KindWithContent::DocumentClear { .. } => Kind::DocumentClear,
            KindWithContent::Settings { .. } => Kind::Settings,
            KindWithContent::IndexCreation { .. } => Kind::IndexCreation,
            KindWithContent::IndexDeletion { .. } => Kind::IndexDeletion,
            KindWithContent::IndexUpdate { .. } => Kind::IndexUpdate,
            KindWithContent::IndexRename { .. } => Kind::IndexRename,
            KindWithContent::IndexSwap { .. } => Kind::IndexSwap,
            KindWithContent::CancelTask { .. } => Kind::CancelTask,
            KindWithContent::DumpExport { .. } => Kind::DumpExport,
            KindWithContent::Snapshot => Kind::Snapshot,
        }
    }

    pub fn persist(&self) -> Result<()> {
        use KindWithContent::*;

        match self {
            DocumentAddition { .. } | DocumentUpdate { .. } => {
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
            | IndexRename { .. }
            | IndexSwap { .. }
            | CancelTask { .. }
            | DumpExport { .. }
            | Snapshot => Ok(()), // There is nothing to persist for all these tasks
        }
    }

    pub fn remove_data(&self) -> Result<()> {
        use KindWithContent::*;

        match self {
            DocumentAddition { .. } | DocumentUpdate { .. } => {
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
            | IndexRename { .. }
            | IndexSwap { .. }
            | CancelTask { .. }
            | DumpExport { .. }
            | Snapshot => Ok(()), // There is no data associated with all these tasks
        }
    }

    pub fn indexes(&self) -> Option<Vec<&str>> {
        use KindWithContent::*;

        match self {
            DumpExport { .. } | Snapshot | CancelTask { .. } => None,
            DocumentAddition { index_uid, .. }
            | DocumentUpdate { index_uid, .. }
            | DocumentDeletion { index_uid, .. }
            | DocumentClear { index_uid }
            | Settings { index_uid, .. }
            | IndexCreation { index_uid, .. }
            | IndexUpdate { index_uid, .. }
            | IndexDeletion { index_uid } => Some(vec![index_uid]),
            IndexRename {
                index_uid: lhs,
                new_name: rhs,
            }
            | IndexSwap { lhs, rhs } => Some(vec![lhs, rhs]),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Kind {
    DocumentAddition,
    DocumentUpdate,
    DocumentDeletion,
    DocumentClear,
    Settings,
    IndexCreation,
    IndexDeletion,
    IndexUpdate,
    IndexRename,
    IndexSwap,
    CancelTask,
    DumpExport,
    Snapshot,
}

impl FromStr for Kind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "document_addition" => Ok(Kind::DocumentAddition),
            "document_update" => Ok(Kind::DocumentUpdate),
            "document_deletion" => Ok(Kind::DocumentDeletion),
            "document_clear" => Ok(Kind::DocumentClear),
            "settings" => Ok(Kind::Settings),
            "index_creation" => Ok(Kind::IndexCreation),
            "index_deletion" => Ok(Kind::IndexDeletion),
            "index_update" => Ok(Kind::IndexUpdate),
            "index_rename" => Ok(Kind::IndexRename),
            "index_swap" => Ok(Kind::IndexSwap),
            "cancel_task" => Ok(Kind::CancelTask),
            "dump_export" => Ok(Kind::DumpExport),
            "snapshot" => Ok(Kind::Snapshot),
            s => Err(Error::InvalidKind(s.to_string())),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum Details {
    #[serde(rename_all = "camelCase")]
    DocumentAddition {
        received_documents: u64,
        indexed_documents: u64,
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
        // TODO why is this optional?
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

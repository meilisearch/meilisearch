use anyhow::Result;
use index::{Settings, Unchecked};
use milli::update::IndexDocumentsMethod;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::TaskId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub uid: TaskId,

    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,

    pub error: Option<String>,
    pub info: Option<String>,

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
}

#[derive(Debug, Serialize, Deserialize)]
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

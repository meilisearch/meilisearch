use anyhow::Result;
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

    #[serde(with = "time::serde::rfc3339::option")]
    pub enqueued_at: Option<OffsetDateTime>,
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KindWithContent {
    DumpExport {
        output: PathBuf,
    },
    Snapshot,
    DocumentAddition {
        index_name: String,
        content_file: Uuid,
    },
    DocumentDeletion {
        index_name: String,
        documents_ids: Vec<String>,
    },
    ClearAllDocuments {
        index_name: String,
    },
    // TODO: TAMO: uncomment the settings
    // Settings {
    //     index_name: String,
    //     new_settings: Settings,
    // },
    RenameIndex {
        index_name: String,
        new_name: String,
    },
    CreateIndex {
        index_name: String,
        primary_key: Option<String>,
    },
    DeleteIndex {
        index_name: String,
    },
    SwapIndex {
        lhs: String,
        rhs: String,
    },
    CancelTask {
        tasks: Vec<TaskId>,
    },
}

impl KindWithContent {
    pub fn as_kind(&self) -> Kind {
        match self {
            KindWithContent::DumpExport { .. } => Kind::DumpExport,
            KindWithContent::DocumentAddition { .. } => Kind::DocumentAddition,
            KindWithContent::DocumentDeletion { .. } => Kind::DocumentDeletion,
            KindWithContent::ClearAllDocuments { .. } => Kind::ClearAllDocuments,
            KindWithContent::RenameIndex { .. } => Kind::RenameIndex,
            KindWithContent::CreateIndex { .. } => Kind::CreateIndex,
            KindWithContent::DeleteIndex { .. } => Kind::DeleteIndex,
            KindWithContent::SwapIndex { .. } => Kind::SwapIndex,
            KindWithContent::CancelTask { .. } => Kind::CancelTask,
            KindWithContent::Snapshot => Kind::Snapshot,
        }
    }

    pub fn persist(&self) -> Result<()> {
        use KindWithContent::*;

        match self {
            DocumentAddition {
                index_name: _,
                content_file: _,
            } => {
                // TODO: TAMO: persist the file
                // content_file.persist();
                Ok(())
            }
            // There is nothing to persist for all these tasks
            DumpExport { .. }
            | DocumentDeletion { .. }
            | ClearAllDocuments { .. }
            | RenameIndex { .. }
            | CreateIndex { .. }
            | DeleteIndex { .. }
            | SwapIndex { .. }
            | CancelTask { .. }
            | Snapshot => Ok(()),
        }
    }

    pub fn remove_data(&self) -> Result<()> {
        use KindWithContent::*;

        match self {
            DocumentAddition {
                index_name: _,
                content_file: _,
            } => {
                // TODO: TAMO: delete the file
                // content_file.delete();
                Ok(())
            }
            // There is no data associated with all these tasks
            DumpExport { .. }
            | DocumentDeletion { .. }
            | ClearAllDocuments { .. }
            | RenameIndex { .. }
            | CreateIndex { .. }
            | DeleteIndex { .. }
            | SwapIndex { .. }
            | CancelTask { .. }
            | Snapshot => Ok(()),
        }
    }

    pub fn indexes(&self) -> Option<Vec<&str>> {
        use KindWithContent::*;

        match self {
            DumpExport { .. } | Snapshot | CancelTask { .. } => None,
            DocumentAddition { index_name, .. }
            | DocumentDeletion { index_name, .. }
            | ClearAllDocuments { index_name }
            | CreateIndex { index_name, .. }
            | DeleteIndex { index_name } => Some(vec![index_name]),
            RenameIndex {
                index_name: lhs,
                new_name: rhs,
            }
            | SwapIndex { lhs, rhs } => Some(vec![lhs, rhs]),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Kind {
    CancelTask,
    ClearAllDocuments,
    CreateIndex,
    DeleteIndex,
    DocumentAddition,
    DocumentDeletion,
    DumpExport,
    RenameIndex,
    Settings,
    Snapshot,
    SwapIndex,
}

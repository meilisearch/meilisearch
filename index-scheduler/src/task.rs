use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use time::OffsetDateTime;

use crate::TaskId;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
    #[serde(with = "time::serde::rfc3339::option")]
    pub enqueued_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,

    pub status: Status,
    pub kind: KindWithContent,
}

impl Task {
    pub fn persist(&self) -> Result<()> {
        self.kind.persist()
    }

    pub fn remove_data(&self) -> Result<()> {
        self.kind.remove_data()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KindWithContent {
    DumpExport {
        output: PathBuf,
    },
    Snapshot,
    DocumentAddition {
        index_name: String,
        content_file: String,
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
        match self {
            KindWithContent::DocumentAddition {
                index_name: _,
                content_file: _,
            } => {
                // TODO: TAMO: persist the file
                // content_file.persist();
                Ok(())
            }
            // There is nothing to persist for all these tasks
            KindWithContent::DumpExport { .. }
            | KindWithContent::DocumentDeletion { .. }
            | KindWithContent::ClearAllDocuments { .. }
            | KindWithContent::RenameIndex { .. }
            | KindWithContent::CreateIndex { .. }
            | KindWithContent::DeleteIndex { .. }
            | KindWithContent::SwapIndex { .. }
            | KindWithContent::CancelTask { .. }
            | KindWithContent::Snapshot => Ok(()),
        }
    }

    pub fn remove_data(&self) -> Result<()> {
        match self {
            KindWithContent::DocumentAddition {
                index_name: _,
                content_file: _,
            } => {
                // TODO: TAMO: delete the file
                // content_file.delete();
                Ok(())
            }
            // There is no data associated with all these tasks
            KindWithContent::DumpExport { .. }
            | KindWithContent::DocumentDeletion { .. }
            | KindWithContent::ClearAllDocuments { .. }
            | KindWithContent::RenameIndex { .. }
            | KindWithContent::CreateIndex { .. }
            | KindWithContent::DeleteIndex { .. }
            | KindWithContent::SwapIndex { .. }
            | KindWithContent::CancelTask { .. }
            | KindWithContent::Snapshot => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

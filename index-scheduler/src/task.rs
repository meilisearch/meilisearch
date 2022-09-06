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
    pub kind: Kind,
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
pub enum Kind {
    DumpExport {
        output: PathBuf,
    },
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

impl Kind {
    pub fn persist(&self) -> Result<()> {
        match self {
            Kind::DocumentAddition {
                index_name,
                content_file,
            } => {
                // TODO: TAMO: persist the file
                // content_file.persist();
                Ok(())
            }
            // There is nothing to persist for all these tasks
            Kind::DumpExport { .. }
            | Kind::DocumentDeletion { .. }
            | Kind::ClearAllDocuments { .. }
            | Kind::RenameIndex { .. }
            | Kind::CreateIndex { .. }
            | Kind::DeleteIndex { .. }
            | Kind::SwapIndex { .. }
            | Kind::CancelTask { .. } => Ok(()),
        }
    }

    pub fn remove_data(&self) -> Result<()> {
        match self {
            Kind::DocumentAddition {
                index_name,
                content_file,
            } => {
                // TODO: TAMO: delete the file
                // content_file.delete();
                Ok(())
            }
            // There is no data associated with all these tasks
            Kind::DumpExport { .. }
            | Kind::DocumentDeletion { .. }
            | Kind::ClearAllDocuments { .. }
            | Kind::RenameIndex { .. }
            | Kind::CreateIndex { .. }
            | Kind::DeleteIndex { .. }
            | Kind::SwapIndex { .. }
            | Kind::CancelTask { .. } => Ok(()),
        }
    }

    pub fn to_u32(&self) -> u32 {
        match self {
            Kind::DumpExport { .. } => 0,
            Kind::DocumentAddition { .. } => 1,
            Kind::DocumentDeletion { .. } => 2,
            Kind::ClearAllDocuments { .. } => 3,
            Kind::RenameIndex { .. } => 4,
            Kind::CreateIndex { .. } => 5,
            Kind::DeleteIndex { .. } => 6,
            Kind::SwapIndex { .. } => 7,
            Kind::CancelTask { .. } => 8,
        }
    }
}

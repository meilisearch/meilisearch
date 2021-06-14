use chrono::{DateTime, Utc};
use milli::update::{DocumentAdditionResult, IndexDocumentsMethod, UpdateFormat};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::index::{Settings, Unchecked};

pub type UpdateError = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateResult {
    DocumentsAddition(DocumentAdditionResult),
    DocumentDeletion { deleted: u64 },
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMeta {
    DocumentsAddition {
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        primary_key: Option<String>,
    },
    ClearDocuments,
    DeleteDocuments {
        ids: Vec<String>
    },
    Settings(Settings<Unchecked>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Enqueued {
    pub update_id: u64,
    pub meta: UpdateMeta,
    pub enqueued_at: DateTime<Utc>,
    pub content: Option<Uuid>,
}

impl Enqueued {
    pub fn new(meta: UpdateMeta, update_id: u64, content: Option<Uuid>) -> Self {
        Self {
            enqueued_at: Utc::now(),
            meta,
            update_id,
            content,
        }
    }

    pub fn processing(self) -> Processing {
        Processing {
            from: self,
            started_processing_at: Utc::now(),
        }
    }

    pub fn abort(self) -> Aborted {
        Aborted {
            from: self,
            aborted_at: Utc::now(),
        }
    }

    pub fn meta(&self) -> &UpdateMeta {
        &self.meta
    }

    pub fn id(&self) -> u64 {
        self.update_id
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Processed {
    pub success: UpdateResult,
    pub processed_at: DateTime<Utc>,
    #[serde(flatten)]
    pub from: Processing,
}

impl Processed {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &UpdateMeta {
        self.from.meta()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Processing {
    #[serde(flatten)]
    pub from: Enqueued,
    pub started_processing_at: DateTime<Utc>,
}

impl Processing {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &UpdateMeta {
        self.from.meta()
    }

    pub fn process(self, success: UpdateResult) -> Processed {
        Processed {
            success,
            from: self,
            processed_at: Utc::now(),
        }
    }

    pub fn fail(self, error: UpdateError) -> Failed {
        Failed {
            from: self,
            error,
            failed_at: Utc::now(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Aborted {
    #[serde(flatten)]
    from: Enqueued,
    aborted_at: DateTime<Utc>,
}

impl Aborted {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &UpdateMeta {
        self.from.meta()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Failed {
    #[serde(flatten)]
    pub from: Processing,
    pub error: UpdateError,
    pub failed_at: DateTime<Utc>,
}

impl Failed {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &UpdateMeta {
        self.from.meta()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum UpdateStatus {
    Processing(Processing),
    Enqueued(Enqueued),
    Processed(Processed),
    Aborted(Aborted),
    Failed(Failed),
}

impl UpdateStatus {
    pub fn id(&self) -> u64 {
        match self {
            UpdateStatus::Processing(u) => u.id(),
            UpdateStatus::Enqueued(u) => u.id(),
            UpdateStatus::Processed(u) => u.id(),
            UpdateStatus::Aborted(u) => u.id(),
            UpdateStatus::Failed(u) => u.id(),
        }
    }

    pub fn meta(&self) -> &UpdateMeta {
        match self {
            UpdateStatus::Processing(u) => u.meta(),
            UpdateStatus::Enqueued(u) => u.meta(),
            UpdateStatus::Processed(u) => u.meta(),
            UpdateStatus::Aborted(u) => u.meta(),
            UpdateStatus::Failed(u) => u.meta(),
        }
    }

    pub fn processed(&self) -> Option<&Processed> {
        match self {
            UpdateStatus::Processed(p) => Some(p),
            _ => None,
        }
    }
}

impl From<Enqueued> for UpdateStatus {
    fn from(other: Enqueued) -> Self {
        Self::Enqueued(other)
    }
}

impl From<Aborted> for UpdateStatus {
    fn from(other: Aborted) -> Self {
        Self::Aborted(other)
    }
}

impl From<Processed> for UpdateStatus {
    fn from(other: Processed) -> Self {
        Self::Processed(other)
    }
}

impl From<Processing> for UpdateStatus {
    fn from(other: Processing) -> Self {
        Self::Processing(other)
    }
}

impl From<Failed> for UpdateStatus {
    fn from(other: Failed) -> Self {
        Self::Failed(other)
    }
}

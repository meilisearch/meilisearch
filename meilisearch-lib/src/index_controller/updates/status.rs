use std::{error::Error, fmt::Display};

use chrono::{DateTime, Utc};

use meilisearch_error::{Code, ErrorCode};
use milli::update::{DocumentAdditionResult, IndexDocumentsMethod};
use serde::{Deserialize, Serialize};

use crate::{
    index::{Settings, Unchecked},
    Update,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateResult {
    DocumentsAddition(DocumentAdditionResult),
    DocumentDeletion { deleted: u64 },
    Other,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMeta {
    DocumentsAddition {
        method: IndexDocumentsMethod,
        primary_key: Option<String>,
    },
    ClearDocuments,
    DeleteDocuments {
        ids: Vec<String>,
    },
    Settings(Settings<Unchecked>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Enqueued {
    pub update_id: u64,
    pub meta: Update,
    pub enqueued_at: DateTime<Utc>,
}

impl Enqueued {
    pub fn new(meta: Update, update_id: u64) -> Self {
        Self {
            enqueued_at: Utc::now(),
            meta,
            update_id,
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

    pub fn meta(&self) -> &Update {
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

    pub fn meta(&self) -> &Update {
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

    pub fn meta(&self) -> &Update {
        self.from.meta()
    }

    pub fn process(self, success: UpdateResult) -> Processed {
        Processed {
            success,
            from: self,
            processed_at: Utc::now(),
        }
    }

    pub fn fail(self, error: impl ErrorCode) -> Failed {
        let msg = error.to_string();
        let code = error.error_code();
        Failed {
            from: self,
            msg,
            code,
            failed_at: Utc::now(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Aborted {
    #[serde(flatten)]
    pub from: Enqueued,
    pub aborted_at: DateTime<Utc>,
}

impl Aborted {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &Update {
        self.from.meta()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Failed {
    #[serde(flatten)]
    pub from: Processing,
    pub msg: String,
    pub code: Code,
    pub failed_at: DateTime<Utc>,
}

impl Display for Failed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.msg.fmt(f)
    }
}

impl Error for Failed {}

impl ErrorCode for Failed {
    fn error_code(&self) -> Code {
        self.code
    }
}

impl Failed {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &Update {
        self.from.meta()
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

    pub fn meta(&self) -> &Update {
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

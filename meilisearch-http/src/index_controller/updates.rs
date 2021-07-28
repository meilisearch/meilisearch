use chrono::{DateTime, Utc};
use milli::update::{DocumentAdditionResult, IndexDocumentsMethod, UpdateFormat};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::ResponseError,
    index::{Settings, Unchecked},
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
        format: UpdateFormat,
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

/// This state indicate that we were able to process the update successfully. Now we are waiting
/// for the user to `commit` his change
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

    /// The commit was made successfully and we can move to our last state
    pub fn commit(self) -> Done {
        Done {
            success: self.success,
            from: self.from,
            processed_at: Utc::now(),
        }
    }

    /// The commit failed
    pub fn fail(self, error: ResponseError) -> Failed {
        Failed {
            from: self.from, // MARIN: maybe we should update Failed so it can fail from the processed state?
            error,
            failed_at: Utc::now(),
        }
    }

    /// The update was aborted
    pub fn abort(self) -> Aborted {
        Aborted {
            from: self.from.from, // MARIN: maybe we should update Aborted so it can fail from the processed state?
            aborted_at: Utc::now(),
        }
    }
}

/// Final state: everything went well
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Done {
    pub success: UpdateResult,
    pub processed_at: DateTime<Utc>,
    #[serde(flatten)]
    pub from: Processing,
}

impl Done {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &UpdateMeta {
        self.from.meta()
    }
}

/// The update is being handled by milli. It can fail but not be aborted.
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

    pub fn fail(self, error: ResponseError) -> Failed {
        Failed {
            from: self,
            error,
            failed_at: Utc::now(),
        }
    }
}

/// Final state: The update has been aborted.
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

/// Final state: The update failed to process or commit correctly.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Failed {
    #[serde(flatten)]
    pub from: Processing,
    pub error: ResponseError,
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum UpdateStatus {
    Processing(Processing),
    Enqueued(Enqueued),
    Processed(Processed),
    Done(Done),
    Aborted(Aborted),
    Failed(Failed),
}

impl UpdateStatus {
    pub fn id(&self) -> u64 {
        match self {
            UpdateStatus::Processing(u) => u.id(),
            UpdateStatus::Enqueued(u) => u.id(),
            UpdateStatus::Processed(u) => u.id(),
            UpdateStatus::Done(u) => u.id(),
            UpdateStatus::Aborted(u) => u.id(),
            UpdateStatus::Failed(u) => u.id(),
        }
    }

    pub fn meta(&self) -> &UpdateMeta {
        match self {
            UpdateStatus::Processing(u) => u.meta(),
            UpdateStatus::Enqueued(u) => u.meta(),
            UpdateStatus::Processed(u) => u.meta(),
            UpdateStatus::Done(u) => u.meta(),
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

impl From<Processing> for UpdateStatus {
    fn from(other: Processing) -> Self {
        Self::Processing(other)
    }
}

impl From<Processed> for UpdateStatus {
    fn from(other: Processed) -> Self {
        Self::Processed(other)
    }
}

impl From<Done> for UpdateStatus {
    fn from(other: Done) -> Self {
        Self::Done(other)
    }
}

impl From<Failed> for UpdateStatus {
    fn from(other: Failed) -> Self {
        Self::Failed(other)
    }
}

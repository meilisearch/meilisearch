use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use super::{ResponseError, Settings, Unchecked};

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct UpdateEntry {
    pub uuid: Uuid,
    pub update: UpdateStatus,
}

impl UpdateEntry {
    pub fn is_finished(&self) -> bool {
        match self.update {
            UpdateStatus::Processing(_) | UpdateStatus::Enqueued(_) => false,
            UpdateStatus::Processed(_) | UpdateStatus::Aborted(_) | UpdateStatus::Failed(_) => true,
        }
    }

    pub fn get_content_uuid(&self) -> Option<&Uuid> {
        match &self.update {
            UpdateStatus::Enqueued(enqueued) => enqueued.content.as_ref(),
            UpdateStatus::Processing(processing) => processing.from.content.as_ref(),
            UpdateStatus::Processed(processed) => processed.from.from.content.as_ref(),
            UpdateStatus::Aborted(aborted) => aborted.from.content.as_ref(),
            UpdateStatus::Failed(failed) => failed.from.from.content.as_ref(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum UpdateResult {
    DocumentsAddition(DocumentAdditionResult),
    DocumentDeletion { deleted: u64 },
    Other,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct DocumentAdditionResult {
    pub nb_documents: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[non_exhaustive]
pub enum IndexDocumentsMethod {
    /// Replace the previous document with the new one,
    /// removing all the already known attributes.
    ReplaceDocuments,

    /// Merge the previous version of the document with the new version,
    /// replacing old attributes values with the new ones and add the new attributes.
    UpdateDocuments,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[non_exhaustive]
pub enum UpdateFormat {
    /// The given update is a real **comma separated** CSV with headers on the first line.
    Csv,
    /// The given update is a JSON array with documents inside.
    Json,
    /// The given update is a JSON stream with a document on each line.
    JsonStream,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
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

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct Enqueued {
    pub update_id: u64,
    pub meta: UpdateMeta,
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    pub content: Option<Uuid>,
}

impl Enqueued {
    pub fn meta(&self) -> &UpdateMeta {
        &self.meta
    }

    pub fn id(&self) -> u64 {
        self.update_id
    }
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct Processed {
    pub success: UpdateResult,
    #[serde(with = "time::serde::rfc3339")]
    pub processed_at: OffsetDateTime,
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

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct Processing {
    #[serde(flatten)]
    pub from: Enqueued,
    #[serde(with = "time::serde::rfc3339")]
    pub started_processing_at: OffsetDateTime,
}

impl Processing {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &UpdateMeta {
        self.from.meta()
    }
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct Aborted {
    #[serde(flatten)]
    pub from: Enqueued,
    #[serde(with = "time::serde::rfc3339")]
    pub aborted_at: OffsetDateTime,
}

impl Aborted {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &UpdateMeta {
        self.from.meta()
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct Failed {
    #[serde(flatten)]
    pub from: Processing,
    pub error: ResponseError,
    #[serde(with = "time::serde::rfc3339")]
    pub failed_at: OffsetDateTime,
}

impl Failed {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &UpdateMeta {
        self.from.meta()
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
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

    pub fn finished_at(&self) -> Option<OffsetDateTime> {
        match self {
            UpdateStatus::Processing(_) => None,
            UpdateStatus::Enqueued(_) => None,
            UpdateStatus::Processed(u) => Some(u.processed_at),
            UpdateStatus::Aborted(_) => None,
            UpdateStatus::Failed(u) => Some(u.failed_at),
        }
    }
}

use std::fmt::Display;

use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use super::{Code, Settings, Unchecked};

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct UpdateEntry {
    pub uuid: Uuid,
    pub update: UpdateStatus,
}

impl UpdateEntry {
    pub fn is_finished(&self) -> bool {
        match self.update {
            UpdateStatus::Processed(_) | UpdateStatus::Aborted(_) | UpdateStatus::Failed(_) => true,
            UpdateStatus::Processing(_) | UpdateStatus::Enqueued(_) => false,
        }
    }

    pub fn get_content_uuid(&self) -> Option<&Uuid> {
        match self.update.meta() {
            Update::DocumentAddition { content_uuid, .. } => Some(content_uuid),
            Update::DeleteDocuments(_) | Update::Settings(_) | Update::ClearDocuments => None,
        }
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

    pub fn meta(&self) -> &Update {
        match self {
            UpdateStatus::Processing(u) => u.meta(),
            UpdateStatus::Enqueued(u) => u.meta(),
            UpdateStatus::Processed(u) => u.meta(),
            UpdateStatus::Aborted(u) => u.meta(),
            UpdateStatus::Failed(u) => u.meta(),
        }
    }

    pub fn is_finished(&self) -> bool {
        match self {
            UpdateStatus::Processing(_) | UpdateStatus::Enqueued(_) => false,
            UpdateStatus::Aborted(_) | UpdateStatus::Failed(_) | UpdateStatus::Processed(_) => true,
        }
    }

    pub fn processed(&self) -> Option<&Processed> {
        match self {
            UpdateStatus::Processed(p) => Some(p),
            _ => None,
        }
    }

    pub fn enqueued_at(&self) -> Option<OffsetDateTime> {
        match self {
            UpdateStatus::Processing(u) => Some(u.from.enqueued_at),
            UpdateStatus::Enqueued(u) => Some(u.enqueued_at),
            UpdateStatus::Processed(u) => Some(u.from.from.enqueued_at),
            UpdateStatus::Aborted(u) => Some(u.from.enqueued_at),
            UpdateStatus::Failed(u) => Some(u.from.from.enqueued_at),
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

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct Enqueued {
    pub update_id: u64,
    pub meta: Update,
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
}

impl Enqueued {
    pub fn meta(&self) -> &Update {
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

    pub fn meta(&self) -> &Update {
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

    pub fn meta(&self) -> &Update {
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

    pub fn meta(&self) -> &Update {
        self.from.meta()
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct Failed {
    #[serde(flatten)]
    pub from: Processing,
    pub msg: String,
    pub code: Code,
    #[serde(with = "time::serde::rfc3339")]
    pub failed_at: OffsetDateTime,
}

impl Display for Failed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.msg.fmt(f)
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

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum Update {
    DeleteDocuments(Vec<String>),
    DocumentAddition {
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_uuid: Uuid,
    },
    Settings(Settings<Unchecked>),
    ClearDocuments,
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

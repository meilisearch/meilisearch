use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use milli::update::{DocumentAdditionResult, IndexDocumentsMethod, UpdateFormat};
use serde::{Deserialize, Serialize};

use crate::index::{Checked, Settings};

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
    DeleteDocuments,
    Settings(Settings<Checked>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Enqueued {
    pub update_id: u64,
    pub meta: UpdateMeta,
    pub enqueued_at: DateTime<Utc>,
    pub content: Option<PathBuf>,
}

impl Enqueued {
    pub fn new(meta: UpdateMeta, update_id: u64, content: Option<PathBuf>) -> Self {
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

    pub fn content_path(&self) -> Option<&Path> {
        self.content.as_deref()
    }

    pub fn content_path_mut(&mut self) -> Option<&mut PathBuf> {
        self.content.as_mut()
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

    pub fn content_path(&self) -> Option<&Path> {
        self.from.content_path()
    }

    pub fn content_path_mut(&mut self) -> Option<&mut PathBuf> {
        self.from.content_path_mut()
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

    pub fn content_path(&self) -> Option<&Path> {
        self.from.content_path()
    }

    pub fn content_path_mut(&mut self) -> Option<&mut PathBuf> {
        self.from.content_path_mut()
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

    pub fn content_path(&self) -> Option<&Path> {
        self.from.content_path()
    }

    pub fn content_path_mut(&mut self) -> Option<&mut PathBuf> {
        self.from.content_path_mut()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Failed {
    #[serde(flatten)]
    from: Processing,
    error: UpdateError,
    failed_at: DateTime<Utc>,
}

impl Failed {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn content_path(&self) -> Option<&Path> {
        self.from.content_path()
    }

    pub fn content_path_mut(&mut self) -> Option<&mut PathBuf> {
        self.from.content_path_mut()
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

    pub fn processed(&self) -> Option<&Processed> {
        match self {
            UpdateStatus::Processed(p) => Some(p),
            _ => None,
        }
    }

    pub fn content_path(&self) -> Option<&Path> {
        match self {
            UpdateStatus::Processing(u) => u.content_path(),
            UpdateStatus::Processed(u) => u.content_path(),
            UpdateStatus::Aborted(u) => u.content_path(),
            UpdateStatus::Failed(u) => u.content_path(),
            UpdateStatus::Enqueued(u) => u.content_path(),
        }
    }

    pub fn content_path_mut(&mut self) -> Option<&mut PathBuf> {
        match self {
            UpdateStatus::Processing(u) => u.content_path_mut(),
            UpdateStatus::Processed(u) => u.content_path_mut(),
            UpdateStatus::Aborted(u) => u.content_path_mut(),
            UpdateStatus::Failed(u) => u.content_path_mut(),
            UpdateStatus::Enqueued(u) => u.content_path_mut(),
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

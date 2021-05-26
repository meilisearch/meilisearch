mod actor;
mod handle_impl;
mod message;
mod loaders;

use std::{fs::File, path::Path};

use log::error;
#[cfg(test)]
use mockall::automock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use loaders::v1::MetadataV1;
use loaders::v2::MetadataV2;

pub use actor::DumpActor;
pub use handle_impl::*;
pub use message::DumpMsg;

pub type DumpResult<T> = std::result::Result<T, DumpError>;

#[derive(Error, Debug)]
pub enum DumpError {
    #[error("error with index: {0}")]
    Error(#[from] anyhow::Error),
    #[error("Heed error: {0}")]
    HeedError(#[from] heed::Error),
    #[error("dump already running")]
    DumpAlreadyRunning,
    #[error("dump `{0}` does not exist")]
    DumpDoesNotExist(String),
}

#[async_trait::async_trait]
#[cfg_attr(test, automock)]
pub trait DumpActorHandle {
    /// Start the creation of a dump
    /// Implementation: [handle_impl::DumpActorHandleImpl::create_dump]
    async fn create_dump(&self) -> DumpResult<DumpInfo>;

    /// Return the status of an already created dump
    /// Implementation: [handle_impl::DumpActorHandleImpl::dump_status]
    async fn dump_info(&self, uid: String) -> DumpResult<DumpInfo>;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "dump_version")]
pub enum Metadata {
    V1 {
        #[serde(flatten)]
        meta: MetadataV1,
    },
    V2 {
        #[serde(flatten)]
        meta: MetadataV2,
    },
}

impl Metadata {
    /// Extract Metadata from `metadata.json` file present at provided `dir_path`
    fn from_path(dir_path: &Path) -> anyhow::Result<Self> {
        let path = dir_path.join("metadata.json");
        let file = File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let metadata = serde_json::from_reader(reader)?;

        Ok(metadata)
    }

    /// Write Metadata in `metadata.json` file at provided `dir_path`
    pub async fn to_path(&self, dir_path: &Path) -> anyhow::Result<()> {
        let path = dir_path.join("metadata.json");
        tokio::fs::write(path, serde_json::to_string(self)?).await?;

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DumpStatus {
    Done,
    InProgress,
    Failed,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DumpInfo {
    pub uid: String,
    pub status: DumpStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl DumpInfo {
    pub fn new(uid: String, status: DumpStatus) -> Self {
        Self {
            uid,
            status,
            error: None,
        }
    }

    pub fn with_error(&mut self, error: String) {
        self.status = DumpStatus::Failed;
        self.error = Some(error);
    }

    pub fn done(&mut self) {
        self.status = DumpStatus::Done;
    }

    pub fn dump_already_in_progress(&self) -> bool {
        self.status == DumpStatus::InProgress
    }
}

pub fn load_dump(
    dst_path: impl AsRef<Path>,
    src_path: impl AsRef<Path>,
    _index_db_size: u64,
    _update_db_size: u64,
) -> anyhow::Result<()> {
    let meta_path = src_path.as_ref().join("metadat.json");
    let mut meta_file = File::open(&meta_path)?;
    let meta: Metadata = serde_json::from_reader(&mut meta_file)?;

    match meta {
        Metadata::V1 { meta } => meta.load_dump(src_path, dst_path)?,
        Metadata::V2 { meta } => meta.load_dump(src_path, dst_path)?,
    }

    Ok(())
}

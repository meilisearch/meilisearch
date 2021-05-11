mod actor;
mod handle_impl;
mod message;
mod v1;
mod v2;

use std::{fs::File, path::Path, sync::Arc};

use anyhow::bail;
use heed::EnvOpenOptions;
use log::{error, info};
use milli::update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat};
#[cfg(test)]
use mockall::automock;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;
use thiserror::Error;
use uuid::Uuid;

use super::IndexMetadata;
use crate::helpers::compression;
use crate::index::Index;
use crate::index_controller::uuid_resolver;

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

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum DumpVersion {
    V1,
    V2,
}

impl DumpVersion {
    const CURRENT: Self = Self::V2;

    /// Select the good importation function from the `DumpVersion` of metadata
    pub fn import_index(
        self,
        size: usize,
        uuid: Uuid,
        dump_path: &Path,
        db_path: &Path,
        primary_key: Option<&str>,
    ) -> anyhow::Result<()> {
        match self {
            Self::V1 => v1::import_index(size, uuid, dump_path, db_path, primary_key),
            Self::V2 => v2::import_index(size, uuid, dump_path, db_path, primary_key),
        }
    }
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
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    indexes: Vec<IndexMetadata>,
    db_version: String,
    dump_version: DumpVersion,
}

impl Metadata {
    /// Create a Metadata with the current dump version of meilisearch.
    pub fn new(indexes: Vec<IndexMetadata>, db_version: String) -> Self {
        Metadata {
            indexes,
            db_version,
            dump_version: DumpVersion::CURRENT,
        }
    }

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
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub error: Option<serde_json::Value>,
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
        self.error = Some(json!(error));
    }

    pub fn done(&mut self) {
        self.status = DumpStatus::Done;
    }

    pub fn dump_already_in_progress(&self) -> bool {
        self.status == DumpStatus::InProgress
    }
}

pub fn load_dump(
    db_path: impl AsRef<Path>,
    dump_path: impl AsRef<Path>,
    size: usize,
) -> anyhow::Result<()> {
    info!("Importing dump from {}...", dump_path.as_ref().display());
    let db_path = db_path.as_ref();
    let dump_path = dump_path.as_ref();
    let uuid_resolver = uuid_resolver::HeedUuidStore::new(&db_path)?;

    // extract the dump in a temporary directory
    let tmp_dir = TempDir::new_in(db_path)?;
    let tmp_dir_path = tmp_dir.path();
    compression::from_tar_gz(dump_path, tmp_dir_path)?;

    // read dump metadata
    let metadata = Metadata::from_path(&tmp_dir_path)?;

    // remove indexes which have same `uuid` than indexes to import and create empty indexes
    let existing_index_uids = uuid_resolver.list()?;

    info!("Deleting indexes already present in the db and provided in the dump...");
    for idx in &metadata.indexes {
        if let Some((_, uuid)) = existing_index_uids.iter().find(|(s, _)| s == &idx.uid) {
            // if we find the index in the `uuid_resolver` it's supposed to exist on the file system
            // and we want to delete it
            let path = db_path.join(&format!("indexes/index-{}", uuid));
            info!("Deleting {}", path.display());
            use std::io::ErrorKind::*;
            match std::fs::remove_dir_all(path) {
                Ok(()) => (),
                // if an index was present in the metadata but missing of the fs we can ignore the
                // problem because we are going to create it later
                Err(e) if e.kind() == NotFound => (),
                Err(e) => bail!(e),
            }
        } else {
            // if the index does not exist in the `uuid_resolver` we create it
            uuid_resolver.create_uuid(idx.uid.clone(), false)?;
        }
    }

    // import each indexes content
    for idx in metadata.indexes {
        let dump_path = tmp_dir_path.join(&idx.uid);
        // this cannot fail since we created all the missing uuid in the previous loop
        let uuid = uuid_resolver.get_uuid(idx.uid)?.unwrap();

        info!(
            "Importing dump from {} into {}...",
            dump_path.display(),
            db_path.display()
        );
        metadata.dump_version.import_index(
            size,
            uuid,
            &dump_path,
            &db_path,
            idx.meta.primary_key.as_ref().map(|s| s.as_ref()),
        )?;
        info!("Dump importation from {} succeed", dump_path.display());
    }

    // finally we can move all the unprocessed update file into our new DB
    // this directory may not exists
    let update_path = tmp_dir_path.join("update_files");
    let db_update_path = db_path.join("updates/update_files");
    if update_path.exists() {
        let _ = std::fs::remove_dir_all(db_update_path);
        std::fs::rename(
            tmp_dir_path.join("update_files"),
            db_path.join("updates/update_files"),
        )?;
    }

    info!("Dump importation from {} succeed", dump_path.display());
    Ok(())
}

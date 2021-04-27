mod v1;
mod v2;

use std::{fs::File, path::{Path, PathBuf}, sync::Arc};

use anyhow::bail;
use heed::EnvOpenOptions;
use log::{error, info};
use milli::update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat};
use serde::{de::Deserializer, Deserialize, Serialize};
use tempfile::TempDir;
use tokio::fs;
use tokio::task::spawn_blocking;

use super::update_actor::UpdateActorHandle;
use super::uuid_resolver::UuidResolverHandle;
use super::IndexMetadata;
use crate::index::Index;
use crate::index_controller::uuid_resolver;
use crate::{helpers::compression, index::Settings};

pub (super) fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum DumpVersion {
    V1,
    V2,
}

impl DumpVersion {
    const CURRENT: Self = Self::V2;

    /// Select the good importation function from the `DumpVersion` of metadata
    pub fn import_index(self, size: usize, dump_path: &Path, index_path: &Path) -> anyhow::Result<()> {
        match self {
            Self::V1 => v1::import_index(size, dump_path, index_path),
            Self::V2 => v2::import_index(size, dump_path, index_path),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DumpMetadata {
    indexes: Vec<IndexMetadata>,
    db_version: String,
    dump_version: DumpVersion,
}

impl DumpMetadata {
    /// Create a DumpMetadata with the current dump version of meilisearch.
    pub fn new(indexes: Vec<IndexMetadata>, db_version: String) -> Self {
        DumpMetadata {
            indexes,
            db_version,
            dump_version: DumpVersion::CURRENT,
        }
    }

    /// Extract DumpMetadata from `metadata.json` file present at provided `dir_path`
    fn from_path(dir_path: &Path) -> anyhow::Result<Self> {
        let path = dir_path.join("metadata.json");
        let file = File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let metadata = serde_json::from_reader(reader)?;

        Ok(metadata)
    }

    /// Write DumpMetadata in `metadata.json` file at provided `dir_path`
    fn to_path(&self, dir_path: &Path) -> anyhow::Result<()> {
        let path = dir_path.join("metadata.json");
        let file = File::create(path)?;

        serde_json::to_writer(file, &self)?;

        Ok(())
    }
}

pub struct DumpService<U, R> {
    uuid_resolver_handle: R,
    update_handle: U,
    dump_path: PathBuf,
    db_name: String,
}

impl<U, R> DumpService<U, R>
where
    U: UpdateActorHandle,
    R: UuidResolverHandle,
{
    pub fn new(
        uuid_resolver_handle: R,
        update_handle: U,
        dump_path: PathBuf,
        db_name: String,
    ) -> Self {
        Self {
            uuid_resolver_handle,
            update_handle,
            dump_path,
            db_name,
        }
    }

    pub async fn run(self) {
        if let Err(e) = self.perform_dump().await {
            error!("{}", e);
        }
    }

    async fn perform_dump(&self) -> anyhow::Result<()> {
        info!("Performing dump.");

        let dump_dir = self.dump_path.clone();
        fs::create_dir_all(&dump_dir).await?;
        let temp_dump_dir = spawn_blocking(move || tempfile::tempdir_in(dump_dir)).await??;
        let temp_dump_path = temp_dump_dir.path().to_owned();

        let uuids = self
            .uuid_resolver_handle
            .dump(temp_dump_path.clone())
            .await?;

        if uuids.is_empty() {
            return Ok(());
        }

        let tasks = uuids
            .iter()
            .map(|&uuid| self.update_handle.dump(uuid, temp_dump_path.clone()))
            .collect::<Vec<_>>();

        futures::future::try_join_all(tasks).await?;

        let dump_dir = self.dump_path.clone();
        let dump_path = self.dump_path.join(format!("{}.dump", self.db_name));
        let dump_path = spawn_blocking(move || -> anyhow::Result<PathBuf> {
            let temp_dump_file = tempfile::NamedTempFile::new_in(dump_dir)?;
            let temp_dump_file_path = temp_dump_file.path().to_owned();
            compression::to_tar_gz(temp_dump_path, temp_dump_file_path)?;
            temp_dump_file.persist(&dump_path)?;
            Ok(dump_path)
        })
        .await??;

        info!("Created dump in {:?}.", dump_path);

        Ok(())
    }
}

/// Write Settings in `settings.json` file at provided `dir_path`
fn settings_to_path(settings: &Settings, dir_path: &Path) -> anyhow::Result<()> {
    let path = dir_path.join("settings.json");
    let file = File::create(path)?;

    serde_json::to_writer(file, settings)?;

    Ok(())
}

pub async fn load_dump(
    db_path: impl AsRef<Path>,
    dump_path: impl AsRef<Path>,
    size: usize,
) -> anyhow::Result<()> {
    info!("Importing dump from {}...", dump_path.as_ref().display());
    let db_path = db_path.as_ref();
    let dump_path = dump_path.as_ref();
    let uuid_resolver = uuid_resolver::UuidResolverHandleImpl::new(&db_path)?;

    // extract the dump in a temporary directory
    let tmp_dir = TempDir::new()?;
    let tmp_dir_path = tmp_dir.path();
    compression::from_tar_gz(dump_path, tmp_dir_path)?;

    // read dump metadata
    let metadata = DumpMetadata::from_path(&tmp_dir_path)?;

    // remove indexes which have same `uuid` than indexes to import and create empty indexes
    let existing_index_uids = uuid_resolver.list().await?;

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
            uuid_resolver.create(idx.uid.clone()).await?;
        }
    }

    // import each indexes content
    for idx in metadata.indexes {
        let dump_path = tmp_dir_path.join(&idx.uid);
        let uuid = uuid_resolver.get(idx.uid).await?;
        let index_path = db_path.join(&format!("indexes/index-{}", uuid));
        let update_path = db_path.join(&format!("updates/updates-{}", uuid)); // TODO: add the update db

        info!("Importing dump from {} into {}...", dump_path.display(), index_path.display());
        metadata.dump_version.import_index(size, &dump_path, &index_path).unwrap();
        info!("Dump importation from {} succeed", dump_path.display());
    }


    info!("Dump importation from {} succeed", dump_path.display());
    Ok(())
}

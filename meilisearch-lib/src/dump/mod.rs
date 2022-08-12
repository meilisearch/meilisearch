use std::fs::File;
use std::path::Path;

use anyhow::bail;
use log::info;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use tempfile::TempDir;

use crate::compression::from_tar_gz;
use crate::options::IndexerOpts;

use self::loaders::{v2, v3, v4, v5};

pub use handler::{generate_uid, DumpHandler};

mod compat;
pub mod error;
mod handler;
mod loaders;

const META_FILE_NAME: &str = "metadata.json";

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    db_version: String,
    index_db_size: usize,
    update_db_size: usize,
    #[serde(with = "time::serde::rfc3339")]
    dump_date: OffsetDateTime,
}

impl Metadata {
    pub fn new(index_db_size: usize, update_db_size: usize) -> Self {
        Self {
            db_version: env!("CARGO_PKG_VERSION").to_string(),
            index_db_size,
            update_db_size,
            dump_date: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetadataV1 {
    pub db_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "dumpVersion")]
pub enum MetadataVersion {
    V1(MetadataV1),
    V2(Metadata),
    V3(Metadata),
    V4(Metadata),
    // V5 is forward compatible with V4 but not backward compatible.
    V5(Metadata),
}

impl MetadataVersion {
    pub fn load_dump(
        self,
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        index_db_size: usize,
        meta_env_size: usize,
        indexing_options: &IndexerOpts,
    ) -> anyhow::Result<()> {
        match self {
            MetadataVersion::V1(_meta) => {
                anyhow::bail!("The version 1 of the dumps is not supported anymore. You can re-export your dump from a version between 0.21 and 0.24, or start fresh from a version 0.25 onwards.")
            }
            MetadataVersion::V2(meta) => v2::load_dump(
                meta,
                src,
                dst,
                index_db_size,
                meta_env_size,
                indexing_options,
            )?,
            MetadataVersion::V3(meta) => v3::load_dump(
                meta,
                src,
                dst,
                index_db_size,
                meta_env_size,
                indexing_options,
            )?,
            MetadataVersion::V4(meta) => v4::load_dump(
                meta,
                src,
                dst,
                index_db_size,
                meta_env_size,
                indexing_options,
            )?,
            MetadataVersion::V5(meta) => v5::load_dump(
                meta,
                src,
                dst,
                index_db_size,
                meta_env_size,
                indexing_options,
            )?,
        }

        Ok(())
    }

    pub fn new_v5(index_db_size: usize, update_db_size: usize) -> Self {
        let meta = Metadata::new(index_db_size, update_db_size);
        Self::V5(meta)
    }

    pub fn db_version(&self) -> &str {
        match self {
            Self::V1(meta) => &meta.db_version,
            Self::V2(meta) | Self::V3(meta) | Self::V4(meta) | Self::V5(meta) => &meta.db_version,
        }
    }

    pub fn version(&self) -> &'static str {
        match self {
            MetadataVersion::V1(_) => "V1",
            MetadataVersion::V2(_) => "V2",
            MetadataVersion::V3(_) => "V3",
            MetadataVersion::V4(_) => "V4",
            MetadataVersion::V5(_) => "V5",
        }
    }

    pub fn dump_date(&self) -> Option<&OffsetDateTime> {
        match self {
            MetadataVersion::V1(_) => None,
            MetadataVersion::V2(meta)
            | MetadataVersion::V3(meta)
            | MetadataVersion::V4(meta)
            | MetadataVersion::V5(meta) => Some(&meta.dump_date),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DumpStatus {
    Done,
    InProgress,
    Failed,
}

pub fn load_dump(
    dst_path: impl AsRef<Path>,
    src_path: impl AsRef<Path>,
    ignore_dump_if_db_exists: bool,
    ignore_missing_dump: bool,
    index_db_size: usize,
    update_db_size: usize,
    indexer_opts: &IndexerOpts,
) -> anyhow::Result<()> {
    let empty_db = crate::is_empty_db(&dst_path);
    let src_path_exists = src_path.as_ref().exists();

    if empty_db && src_path_exists {
        let (tmp_src, tmp_dst, meta) = extract_dump(&dst_path, &src_path)?;
        meta.load_dump(
            tmp_src.path(),
            tmp_dst.path(),
            index_db_size,
            update_db_size,
            indexer_opts,
        )?;
        persist_dump(&dst_path, tmp_dst)?;
        Ok(())
    } else if !empty_db && !ignore_dump_if_db_exists {
        bail!(
            "database already exists at {:?}, try to delete it or rename it",
            dst_path
                .as_ref()
                .canonicalize()
                .unwrap_or_else(|_| dst_path.as_ref().to_owned())
        )
    } else if !src_path_exists && !ignore_missing_dump {
        bail!("dump doesn't exist at {:?}", src_path.as_ref())
    } else {
        // there is nothing to do
        Ok(())
    }
}

fn extract_dump(
    dst_path: impl AsRef<Path>,
    src_path: impl AsRef<Path>,
) -> anyhow::Result<(TempDir, TempDir, MetadataVersion)> {
    // Setup a temp directory path in the same path as the database, to prevent cross devices
    // references.
    let temp_path = dst_path
        .as_ref()
        .parent()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| ".".into());

    let tmp_src = tempfile::tempdir_in(temp_path)?;
    let tmp_src_path = tmp_src.path();

    from_tar_gz(&src_path, tmp_src_path)?;

    let meta_path = tmp_src_path.join(META_FILE_NAME);
    let mut meta_file = File::open(&meta_path)?;
    let meta: MetadataVersion = serde_json::from_reader(&mut meta_file)?;

    if !dst_path.as_ref().exists() {
        std::fs::create_dir_all(dst_path.as_ref())?;
    }

    let tmp_dst = tempfile::tempdir_in(dst_path.as_ref())?;

    info!(
        "Loading dump {}, dump database version: {}, dump version: {}",
        meta.dump_date()
            .map(|t| format!("from {}", t))
            .unwrap_or_else(String::new),
        meta.db_version(),
        meta.version()
    );

    Ok((tmp_src, tmp_dst, meta))
}

fn persist_dump(dst_path: impl AsRef<Path>, tmp_dst: TempDir) -> anyhow::Result<()> {
    let persisted_dump = tmp_dst.into_path();

    // Delete everything in the `data.ms` except the tempdir.
    if dst_path.as_ref().exists() {
        for file in dst_path.as_ref().read_dir().unwrap() {
            let file = file.unwrap().path();
            if file.file_name() == persisted_dump.file_name() {
                continue;
            }

            if file.is_file() {
                std::fs::remove_file(&file)?;
            } else {
                std::fs::remove_dir_all(&file)?;
            }
        }
    }

    // Move the whole content of the tempdir into the `data.ms`.
    for file in persisted_dump.read_dir().unwrap() {
        let file = file.unwrap().path();

        std::fs::rename(&file, &dst_path.as_ref().join(file.file_name().unwrap()))?;
    }

    // Delete the empty tempdir.
    std::fs::remove_dir_all(&persisted_dump)?;

    Ok(())
}

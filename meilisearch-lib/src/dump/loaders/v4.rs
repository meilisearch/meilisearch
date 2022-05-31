use std::fs;
use std::path::Path;

use fs_extra::dir::{self, CopyOptions};
use log::info;
use tempfile::tempdir;

use crate::dump::Metadata;
use crate::options::IndexerOpts;

pub fn load_dump(
    meta: Metadata,
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    index_db_size: usize,
    meta_env_size: usize,
    indexing_options: &IndexerOpts,
) -> anyhow::Result<()> {
    info!("Patching dump V4 to dump V5...");

    let patched_dir = tempdir()?;
    let options = CopyOptions::default();

    // Indexes
    dir::copy(src.as_ref().join("indexes"), patched_dir.path(), &options)?;

    // Index uuids
    dir::copy(
        src.as_ref().join("index_uuids"),
        patched_dir.path(),
        &options,
    )?;

    // Metadata
    fs::copy(
        src.as_ref().join("metadata.json"),
        patched_dir.path().join("metadata.json"),
    )?;

    // Updates
    dir::copy(src.as_ref().join("updates"), patched_dir.path(), &options)?;

    // Keys
    if src.as_ref().join("keys").exists() {
        fs::copy(src.as_ref().join("keys"), patched_dir.path().join("keys"))?;
    }

    super::v5::load_dump(
        meta,
        patched_dir.path(),
        dst,
        index_db_size,
        meta_env_size,
        indexing_options,
    )
}

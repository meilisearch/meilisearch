use std::fs::{self, create_dir_all, File};
use std::io::Write;
use std::path::Path;

use fs_extra::dir::{self, CopyOptions};
use log::info;
use tempfile::tempdir;

use meilisearch_auth::AuthController;

use crate::dump::{compat, Metadata};
use crate::options::IndexerOpts;
use crate::tasks::task::Task;

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
    patch_updates(&src, &patched_dir)?;

    // Keys
    AuthController::patch_dump_v4(&src, patched_dir.path())?;

    super::v5::load_dump(
        meta,
        patched_dir.path(),
        dst,
        index_db_size,
        meta_env_size,
        indexing_options,
    )
}

fn patch_updates(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> anyhow::Result<()> {
    let updates_path = src.as_ref().join("updates/data.jsonl");
    let output_updates_path = dst.as_ref().join("updates/data.jsonl");
    create_dir_all(output_updates_path.parent().unwrap())?;
    let udpates_file = File::open(updates_path)?;
    let mut output_update_file = File::create(output_updates_path)?;

    serde_json::Deserializer::from_reader(udpates_file)
        .into_iter::<compat::v4::Task>()
        .try_for_each(|task| -> anyhow::Result<()> {
            let task: Task = task?.into();

            serde_json::to_writer(&mut output_update_file, &task)?;
            output_update_file.write_all(b"\n")?;

            Ok(())
        })?;

    output_update_file.flush()?;

    Ok(())
}

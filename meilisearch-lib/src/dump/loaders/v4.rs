use std::fs::{self, create_dir_all, File};
use std::io::{BufReader, Write};
use std::path::Path;

use fs_extra::dir::{self, CopyOptions};
use log::info;
use serde_json::{Deserializer, Map, Value};
use tempfile::tempdir;
use uuid::Uuid;

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
    dir::copy(src.as_ref().join("indexes"), &patched_dir, &options)?;

    // Index uuids
    dir::copy(src.as_ref().join("index_uuids"), &patched_dir, &options)?;

    // Metadata
    fs::copy(
        src.as_ref().join("metadata.json"),
        patched_dir.path().join("metadata.json"),
    )?;

    // Updates
    patch_updates(&src, &patched_dir)?;

    // Keys
    patch_keys(&src, &patched_dir)?;

    super::v5::load_dump(
        meta,
        &patched_dir,
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

fn patch_keys(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> anyhow::Result<()> {
    let keys_file_src = src.as_ref().join("keys");

    if !keys_file_src.exists() {
        return Ok(());
    }

    fs::create_dir_all(&dst)?;
    let keys_file_dst = dst.as_ref().join("keys");
    let mut writer = File::create(&keys_file_dst)?;

    let reader = BufReader::new(File::open(&keys_file_src)?);
    for key in Deserializer::from_reader(reader).into_iter() {
        let mut key: Map<String, Value> = key?;

        // generate a new uuid v4 and insert it in the key.
        let uid = serde_json::to_value(Uuid::new_v4()).unwrap();
        key.insert("uid".to_string(), uid);

        serde_json::to_writer(&mut writer, &key)?;
        writer.write_all(b"\n")?;
    }

    Ok(())
}

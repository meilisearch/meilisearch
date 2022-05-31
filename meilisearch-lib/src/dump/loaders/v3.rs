use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::Context;
use fs_extra::dir::{self, CopyOptions};
use log::info;
use tempfile::tempdir;
use uuid::Uuid;

use crate::dump::compat::v3;
use crate::dump::Metadata;
use crate::index_resolver::meta_store::{DumpEntry, IndexMeta};
use crate::options::IndexerOpts;
use crate::tasks::task::{Task, TaskId};

/// dump structure for V3:
/// .
/// ├── indexes
/// │   └── 25f10bb8-6ea8-42f0-bd48-ad5857f77648
/// │       ├── documents.jsonl
/// │       └── meta.json
/// ├── index_uuids
/// │   └── data.jsonl
/// ├── metadata.json
/// └── updates
///     └── data.jsonl

pub fn load_dump(
    meta: Metadata,
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    index_db_size: usize,
    meta_env_size: usize,
    indexing_options: &IndexerOpts,
) -> anyhow::Result<()> {
    info!("Patching dump V3 to dump V4...");

    let patched_dir = tempdir()?;

    let options = CopyOptions::default();
    dir::copy(src.as_ref().join("indexes"), patched_dir.path(), &options)?;
    dir::copy(
        src.as_ref().join("index_uuids"),
        patched_dir.path(),
        &options,
    )?;

    let uuid_map = patch_index_meta(
        src.as_ref().join("index_uuids/data.jsonl"),
        patched_dir.path(),
    )?;

    fs::copy(
        src.as_ref().join("metadata.json"),
        patched_dir.path().join("metadata.json"),
    )?;

    patch_updates(&src, patched_dir.path(), uuid_map)?;

    super::v4::load_dump(
        meta,
        patched_dir.path(),
        dst,
        index_db_size,
        meta_env_size,
        indexing_options,
    )
}

fn patch_index_meta(
    path: impl AsRef<Path>,
    dst: impl AsRef<Path>,
) -> anyhow::Result<HashMap<Uuid, String>> {
    let file = BufReader::new(File::open(path)?);
    let dst = dst.as_ref().join("index_uuids");
    fs::create_dir_all(&dst)?;
    let mut dst_file = File::create(dst.join("data.jsonl"))?;

    let map = serde_json::Deserializer::from_reader(file)
        .into_iter::<v3::DumpEntry>()
        .try_fold(HashMap::new(), |mut map, entry| -> anyhow::Result<_> {
            let entry = entry?;
            map.insert(entry.uuid, entry.uid.clone());
            let meta = IndexMeta {
                uuid: entry.uuid,
                // This is lost information, we patch it to 0;
                creation_task_id: 0,
            };
            let entry = DumpEntry {
                uid: entry.uid,
                index_meta: meta,
            };
            serde_json::to_writer(&mut dst_file, &entry)?;
            dst_file.write_all(b"\n")?;
            Ok(map)
        })?;

    dst_file.flush()?;

    Ok(map)
}

fn patch_updates(
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    uuid_map: HashMap<Uuid, String>,
) -> anyhow::Result<()> {
    let dst = dst.as_ref().join("updates");
    fs::create_dir_all(&dst)?;

    let mut dst_file = BufWriter::new(File::create(dst.join("data.jsonl"))?);
    let src_file = BufReader::new(File::open(src.as_ref().join("updates/data.jsonl"))?);

    serde_json::Deserializer::from_reader(src_file)
        .into_iter::<v3::UpdateEntry>()
        .enumerate()
        .try_for_each(|(task_id, entry)| -> anyhow::Result<()> {
            let entry = entry?;
            let name = uuid_map
                .get(&entry.uuid)
                .with_context(|| format!("Unknown index uuid: {}", entry.uuid))?
                .clone();
            serde_json::to_writer(
                &mut dst_file,
                &Task::from((entry.update, name, task_id as TaskId)),
            )?;
            dst_file.write_all(b"\n")?;
            Ok(())
        })?;

    dst_file.flush()?;

    Ok(())
}

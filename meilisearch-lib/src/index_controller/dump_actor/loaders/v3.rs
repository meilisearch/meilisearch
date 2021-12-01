use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use fs_extra::dir::{self, CopyOptions};
use log::info;
use serde::{Deserialize, Serialize};
use tempfile::tempdir;
use uuid::Uuid;

use crate::index_controller::dump_actor::compat::v3::UpdateStatus;
use crate::index_controller::dump_actor::Metadata;
use crate::options::IndexerOpts;
use crate::tasks::task::Task;

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

#[allow(dead_code)]
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
    fs::copy(
        src.as_ref().join("metadata.json"),
        patched_dir.path().join("metadata.json"),
    )?;

    patch_updates(&src, patched_dir.path())?;

    super::v4::load_dump(
        meta,
        patched_dir.path(),
        dst,
        index_db_size,
        meta_env_size,
        indexing_options,
    )
}

fn patch_updates(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> anyhow::Result<()> {
    let dst = dst.as_ref().join("updates");
    fs::create_dir_all(&dst)?;

    let mut dst_file = BufWriter::new(File::create(dst.join("data.jsonl"))?);
    let src_file = BufReader::new(File::open(src.as_ref().join("updates/data.jsonl"))?);

    #[derive(Serialize, Deserialize)]
    pub struct UpdateEntry {
        pub uuid: Uuid,
        pub update: UpdateStatus,
    }
    serde_json::Deserializer::from_reader(src_file)
        .into_iter::<UpdateEntry>()
        .try_for_each(|update| -> anyhow::Result<()> {
            serde_json::to_writer(&mut dst_file, &Task::from(update?.update))?;
            dst_file.write_all(b"\n")?;
            Ok(())
        })?;

    dst_file.flush()?;

    Ok(())
}

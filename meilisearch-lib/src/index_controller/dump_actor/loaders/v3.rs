use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use fs_extra::dir::{self, CopyOptions};
use log::info;
use tempfile::tempdir;

use crate::index_controller::dump_actor::Metadata;
use crate::options::IndexerOpts;
use crate::tasks::task::Task;

use super::super::compat::v3;

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
    dir::copy(
        src.as_ref().join("indexes"),
        patched_dir.path().join("indexes"),
        &options,
    )?;
    dir::copy(
        src.as_ref().join("index_uuids"),
        patched_dir.path().join("index_uuids"),
        &options,
    )?;

    fs::copy(
        src.as_ref().join("metadata.json"),
        patched_dir.path().join("metadata.json"),
    )?;

    patch_updates(&src, patched_dir.path())?;

    todo!("call import dumpv4");
}

fn patch_updates(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> anyhow::Result<()> {
    let dst = dst.as_ref().join("updates");
    fs::create_dir_all(&dst)?;

    let mut dst_file = BufWriter::new(File::open(dst.join("data.jsonl"))?);
    let src_file = BufReader::new(File::open(src.as_ref().join("updates/data.jsonl"))?);

    serde_json::Deserializer::from_reader(src_file)
        .into_iter::<v3::UpdateStatus>()
        .try_for_each(|update| -> anyhow::Result<()> {
            serde_json::to_writer(&mut dst_file, &Task::from(update?))?;
            dst_file.write_all(b"\n")?;
            Ok(())
        })?;

    dst_file.flush()?;

    Ok(())
}

//! The breaking changes that happened between the v1.11 and the v1.12 are:
//! - The new indexer changed the update files format from OBKV to ndjson. https://github.com/meilisearch/meilisearch/pull/4900

use std::borrow::Cow;
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use anyhow::Context;
use file_store::FileStore;
use indexmap::IndexMap;
use meilisearch_types::milli::documents::DocumentsBatchReader;
use milli::heed::types::Str;
use milli::heed::{Database, EnvOpenOptions};
use milli::progress::Step;
use serde_json::value::RawValue;
use tempfile::NamedTempFile;

use crate::try_opening_database;
use crate::uuid_codec::UuidCodec;

pub fn v1_11_to_v1_12(db_path: &Path) -> anyhow::Result<()> {
    println!("Upgrading from v1.11.0 to v1.12.0");

    convert_update_files(db_path)?;

    Ok(())
}

pub fn v1_12_to_v1_12_3(db_path: &Path) -> anyhow::Result<()> {
    println!("Upgrading from v1.12.{{0, 1, 2}} to v1.12.3");

    rebuild_field_distribution(db_path)?;

    Ok(())
}

/// Convert the update files from OBKV to ndjson format.
///
/// 1) List all the update files using the file store.
/// 2) For each update file, read the update file into a DocumentsBatchReader.
/// 3) For each document in the update file, convert the document to a JSON object.
/// 4) Write the JSON object to a tmp file in the update files directory.
/// 5) Persist the tmp file replacing the old update file.
fn convert_update_files(db_path: &Path) -> anyhow::Result<()> {
    let update_files_dir_path = db_path.join("update_files");
    let file_store = FileStore::new(&update_files_dir_path).with_context(|| {
        format!("while creating file store for update files dir {update_files_dir_path:?}")
    })?;

    for uuid in file_store.all_uuids().context("while retrieving uuids from file store")? {
        let uuid = uuid.context("while retrieving uuid from file store")?;
        let update_file_path = file_store.get_update_path(uuid);
        let update_file = file_store
            .get_update(uuid)
            .with_context(|| format!("while getting update file for uuid {uuid:?}"))?;

        let mut file =
            NamedTempFile::new_in(&update_files_dir_path).map(BufWriter::new).with_context(
                || format!("while creating bufwriter for update file {update_file_path:?}"),
            )?;

        let reader = DocumentsBatchReader::from_reader(update_file).with_context(|| {
            format!("while creating documents batch reader for update file {update_file_path:?}")
        })?;
        let (mut cursor, index) = reader.into_cursor_and_fields_index();

        while let Some(document) = cursor.next_document().with_context(|| {
            format!(
                "while reading documents from batch reader for update file {update_file_path:?}"
            )
        })? {
            let mut json_document = IndexMap::new();
            for (fid, value) in document {
                let field_name = index
                    .name(fid)
                    .with_context(|| format!("while getting field name for fid {fid} for update file {update_file_path:?}"))?;
                let value: &RawValue = serde_json::from_slice(value)?;
                json_document.insert(field_name, value);
            }

            serde_json::to_writer(&mut file, &json_document)?;
        }

        let file = file.into_inner().map_err(|e| e.into_error()).context(format!(
            "while flushing update file bufwriter for update file {update_file_path:?}"
        ))?;
        let _ = file
            // atomically replace the obkv file with the rewritten NDJSON file
            .persist(&update_file_path)
            .with_context(|| format!("while persisting update file {update_file_path:?}"))?;
    }

    Ok(())
}

/// Rebuild field distribution as it was wrongly computed in v1.12.x if x < 3
fn rebuild_field_distribution(db_path: &Path) -> anyhow::Result<()> {
    let index_scheduler_path = db_path.join("tasks");
    let env = unsafe { EnvOpenOptions::new().max_dbs(100).open(&index_scheduler_path) }
        .with_context(|| format!("While trying to open {:?}", index_scheduler_path.display()))?;

    let sched_rtxn = env.read_txn()?;

    let index_mapping: Database<Str, UuidCodec> =
        try_opening_database(&env, &sched_rtxn, "index-mapping")?;

    let index_count =
        index_mapping.len(&sched_rtxn).context("while reading the number of indexes")?;

    let progress = milli::progress::Progress::default();
    let finished = AtomicBool::new(false);

    std::thread::scope(|scope| {
        let indexes = index_mapping.iter(&sched_rtxn)?;

        let display_progress = std::thread::Builder::new()
            .name("display_progress".into())
            .spawn_scoped(scope, || {
                while !finished.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_secs(5));
                    let view = progress.as_progress_view();
                    let Ok(view) = serde_json::to_string(&view) else {
                        continue;
                    };
                    println!("{view}");
                }
            })
            .unwrap();

        for (index_index, result) in indexes.enumerate() {
            let (uid, uuid) = result?;
            progress.update_progress(VariableNameStep::new(
                uid,
                index_index as u32,
                index_count as u32,
            ));
            let index_path = db_path.join("indexes").join(uuid.to_string());

            println!(
                "[{}/{index_count}]Updating index `{uid}` at `{}`",
                index_index + 1,
                index_path.display()
            );

            println!("\t- Rebuilding field distribution");

            let index =
                milli::Index::new(EnvOpenOptions::new(), &index_path).with_context(|| {
                    format!("while opening index {uid} at '{}'", index_path.display())
                })?;

            let mut index_txn = index.write_txn()?;

            milli::update::new::reindex::field_distribution(&index, &mut index_txn, &progress)
                .context("while rebuilding field distribution")?;

            index_txn.commit().context("while committing the write txn for the updated index")?;
        }

        sched_rtxn.commit().context("while committing the write txn for the index-scheduler")?;

        finished.store(true, std::sync::atomic::Ordering::Relaxed);

        if let Err(panic) = display_progress.join() {
            let msg = match panic.downcast_ref::<&'static str>() {
                Some(s) => *s,
                None => match panic.downcast_ref::<String>() {
                    Some(s) => &s[..],
                    None => "Box<dyn Any>",
                },
            };
            eprintln!("WARN: the display thread panicked with {msg}");
        }

        println!("Upgrading database succeeded");
        Ok(())
    })
}

pub struct VariableNameStep {
    name: String,
    current: u32,
    total: u32,
}

impl VariableNameStep {
    pub fn new(name: impl Into<String>, current: u32, total: u32) -> Self {
        Self { name: name.into(), current, total }
    }
}

impl Step for VariableNameStep {
    fn name(&self) -> Cow<'static, str> {
        self.name.clone().into()
    }

    fn current(&self) -> u32 {
        self.current
    }

    fn total(&self) -> u32 {
        self.total
    }
}

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
use meilisearch_types::milli::heed::types::{SerdeJson, Str};
use meilisearch_types::milli::heed::{Database, EnvOpenOptions, RoTxn, RwTxn};
use meilisearch_types::milli::progress::Step;
use meilisearch_types::milli::{FieldDistribution, Index};
use serde::Serialize;
use serde_json::value::RawValue;
use tempfile::NamedTempFile;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::try_opening_database;
use crate::uuid_codec::UuidCodec;

pub fn v1_11_to_v1_12(
    db_path: &Path,
    _origin_major: u32,
    _origin_minor: u32,
    _origin_patch: u32,
) -> anyhow::Result<()> {
    println!("Upgrading from v1.11.0 to v1.12.0");

    convert_update_files(db_path)?;

    Ok(())
}

pub fn v1_12_to_v1_12_3(
    db_path: &Path,
    origin_major: u32,
    origin_minor: u32,
    origin_patch: u32,
) -> anyhow::Result<()> {
    println!("Upgrading from v1.12.{{0, 1, 2}} to v1.12.3");

    if origin_minor == 12 {
        rebuild_field_distribution(db_path)?;
    } else {
        println!("Not rebuilding field distribution as it wasn't corrupted coming from v{origin_major}.{origin_minor}.{origin_patch}");
    }

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

    let mut sched_wtxn = env.write_txn()?;

    let index_mapping: Database<Str, UuidCodec> =
        try_opening_database(&env, &sched_wtxn, "index-mapping")?;
    let stats_db: Database<UuidCodec, SerdeJson<IndexStats>> =
        try_opening_database(&env, &sched_wtxn, "index-stats").with_context(|| {
            format!("While trying to open {:?}", index_scheduler_path.display())
        })?;

    let index_count =
        index_mapping.len(&sched_wtxn).context("while reading the number of indexes")?;

    // FIXME: not ideal, we have to pre-populate all indexes to prevent double borrow of sched_wtxn
    // 1. immutably for the iteration
    // 2. mutably for updating index stats
    let indexes: Vec<_> = index_mapping
        .iter(&sched_wtxn)?
        .map(|res| res.map(|(uid, uuid)| (uid.to_owned(), uuid)))
        .collect();

    let progress = meilisearch_types::milli::progress::Progress::default();
    let finished = AtomicBool::new(false);

    std::thread::scope(|scope| {
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

        for (index_index, result) in indexes.into_iter().enumerate() {
            let (uid, uuid) = result?;
            progress.update_progress(VariableNameStep::new(
                &uid,
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
                meilisearch_types::milli::Index::new(EnvOpenOptions::new(), &index_path, false)
                    .with_context(|| {
                        format!("while opening index {uid} at '{}'", index_path.display())
                    })?;

            let mut index_txn = index.write_txn()?;

            meilisearch_types::milli::update::new::reindex::field_distribution(
                &index,
                &mut index_txn,
                &progress,
            )
            .context("while rebuilding field distribution")?;

            let stats = IndexStats::new(&index, &index_txn)
                .with_context(|| format!("computing stats for index `{uid}`"))?;
            store_stats_of(stats_db, uuid, &mut sched_wtxn, &uid, &stats)?;

            index_txn.commit().context("while committing the write txn for the updated index")?;
        }

        sched_wtxn.commit().context("while committing the write txn for the index-scheduler")?;

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

pub fn store_stats_of(
    stats_db: Database<UuidCodec, SerdeJson<IndexStats>>,
    index_uuid: Uuid,
    sched_wtxn: &mut RwTxn,
    index_uid: &str,
    stats: &IndexStats,
) -> anyhow::Result<()> {
    stats_db
        .put(sched_wtxn, &index_uuid, stats)
        .with_context(|| format!("storing stats for index `{index_uid}`"))?;
    Ok(())
}

/// The statistics that can be computed from an `Index` object.
#[derive(Serialize, Debug)]
pub struct IndexStats {
    /// Number of documents in the index.
    pub number_of_documents: u64,
    /// Size taken up by the index' DB, in bytes.
    ///
    /// This includes the size taken by both the used and free pages of the DB, and as the free pages
    /// are not returned to the disk after a deletion, this number is typically larger than
    /// `used_database_size` that only includes the size of the used pages.
    pub database_size: u64,
    /// Size taken by the used pages of the index' DB, in bytes.
    ///
    /// As the DB backend does not return to the disk the pages that are not currently used by the DB,
    /// this value is typically smaller than `database_size`.
    pub used_database_size: u64,
    /// Association of every field name with the number of times it occurs in the documents.
    pub field_distribution: FieldDistribution,
    /// Creation date of the index.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// Date of the last update of the index.
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl IndexStats {
    /// Compute the stats of an index
    ///
    /// # Parameters
    ///
    /// - rtxn: a RO transaction for the index, obtained from `Index::read_txn()`.
    pub fn new(index: &Index, rtxn: &RoTxn) -> meilisearch_types::milli::Result<Self> {
        Ok(IndexStats {
            number_of_documents: index.number_of_documents(rtxn)?,
            database_size: index.on_disk_size()?,
            used_database_size: index.used_size()?,
            field_distribution: index.field_distribution(rtxn)?,
            created_at: index.created_at(rtxn)?,
            updated_at: index.updated_at(rtxn)?,
        })
    }
}

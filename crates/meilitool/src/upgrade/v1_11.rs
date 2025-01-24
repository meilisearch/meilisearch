//! The breaking changes that happened between the v1.10 and the v1.11 are:
//! - Arroy went from the v0.4.0 to the v0.5.0, see this release note to get the whole context: https://github.com/meilisearch/arroy/releases/tag/v0.5.0
//!   - The `angular` distance has been renamed to `cosine` => We only need to update the string in the metadata.
//!   - Reorganize the `NodeId` to make the appending of vectors work => We'll have to update the keys of almost all items in the DB.
//!   - Store the list of updated IDs directly in LMDB instead of a roaring bitmap => This shouldn't be an issue since we are never supposed to commit this roaring bitmap, but it's not forbidden by arroy so ensuring it works is probably better than anything.

use std::path::Path;

use anyhow::Context;
use meilisearch_types::heed::types::Str;
use meilisearch_types::heed::{Database, EnvOpenOptions};
use meilisearch_types::milli::index::db_name;

use crate::uuid_codec::UuidCodec;
use crate::{try_opening_database, try_opening_poly_database};

pub fn v1_10_to_v1_11(
    db_path: &Path,
    _origin_major: u32,
    _origin_minor: u32,
    _origin_patch: u32,
) -> anyhow::Result<()> {
    println!("Upgrading from v1.10.0 to v1.11.0");

    let index_scheduler_path = db_path.join("tasks");
    let env = unsafe { EnvOpenOptions::new().max_dbs(100).open(&index_scheduler_path) }
        .with_context(|| format!("While trying to open {:?}", index_scheduler_path.display()))?;

    let sched_rtxn = env.read_txn()?;

    let index_mapping: Database<Str, UuidCodec> =
        try_opening_database(&env, &sched_rtxn, "index-mapping")?;

    let index_count =
        index_mapping.len(&sched_rtxn).context("while reading the number of indexes")?;

    let indexes: Vec<_> = index_mapping
        .iter(&sched_rtxn)?
        .map(|res| res.map(|(uid, uuid)| (uid.to_owned(), uuid)))
        .collect();

    for (index_index, result) in indexes.into_iter().enumerate() {
        let (uid, uuid) = result?;
        let index_path = db_path.join("indexes").join(uuid.to_string());

        println!(
            "[{}/{index_count}]Updating embeddings for `{uid}` at `{}`",
            index_index + 1,
            index_path.display()
        );

        let index_env = unsafe {
            EnvOpenOptions::new().max_dbs(25).open(&index_path).with_context(|| {
                format!("while opening index {uid} at '{}'", index_path.display())
            })?
        };

        let index_rtxn = index_env.read_txn().with_context(|| {
            format!(
                "while obtaining a read transaction for index {uid} at {}",
                index_path.display()
            )
        })?;
        let index_read_database =
            try_opening_poly_database(&index_env, &index_rtxn, db_name::VECTOR_ARROY)
                .with_context(|| format!("while updating date format for index `{uid}`"))?;

        let mut index_wtxn = index_env.write_txn().with_context(|| {
            format!(
                "while obtaining a write transaction for index {uid} at {}",
                index_path.display()
            )
        })?;

        let index_write_database =
            try_opening_poly_database(&index_env, &index_wtxn, db_name::VECTOR_ARROY)
                .with_context(|| format!("while updating date format for index `{uid}`"))?;

        arroy_v04_to_v05::ugrade_from_prev_version(
            &index_rtxn,
            index_read_database,
            &mut index_wtxn,
            index_write_database,
        )?;

        index_wtxn.commit()?;
    }

    Ok(())
}

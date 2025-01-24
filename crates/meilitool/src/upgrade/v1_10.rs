use std::path::Path;

use anyhow::{bail, Context};
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn, Unspecified};
use meilisearch_types::milli::index::{db_name, main_key};

use super::v1_9;
use crate::uuid_codec::UuidCodec;
use crate::{try_opening_database, try_opening_poly_database};

pub type FieldDistribution = std::collections::BTreeMap<String, u64>;

/// The statistics that can be computed from an `Index` object.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
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
    pub created_at: time::OffsetDateTime,
    /// Date of the last update of the index.
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: time::OffsetDateTime,
}

impl From<v1_9::IndexStats> for IndexStats {
    fn from(
        v1_9::IndexStats {
            number_of_documents,
            database_size,
            used_database_size,
            field_distribution,
            created_at,
            updated_at,
        }: v1_9::IndexStats,
    ) -> Self {
        IndexStats {
            number_of_documents,
            database_size,
            used_database_size,
            field_distribution,
            created_at: created_at.0,
            updated_at: updated_at.0,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct OffsetDateTime(#[serde(with = "time::serde::rfc3339")] pub time::OffsetDateTime);

fn update_index_stats(
    index_stats: Database<UuidCodec, Unspecified>,
    index_uid: &str,
    index_uuid: uuid::Uuid,
    sched_wtxn: &mut RwTxn,
) -> anyhow::Result<()> {
    let ctx = || format!("while updating index stats for index `{index_uid}`");

    let stats: Option<v1_9::IndexStats> = index_stats
        .remap_data_type::<SerdeJson<v1_9::IndexStats>>()
        .get(sched_wtxn, &index_uuid)
        .with_context(ctx)
        .with_context(|| "While reading value")?;

    if let Some(stats) = stats {
        let stats: self::IndexStats = stats.into();

        index_stats
            .remap_data_type::<SerdeJson<self::IndexStats>>()
            .put(sched_wtxn, &index_uuid, &stats)
            .with_context(ctx)
            .with_context(|| "While writing value")?;
    }

    Ok(())
}

fn update_date_format(
    index_uid: &str,
    index_env: &Env,
    index_wtxn: &mut RwTxn,
) -> anyhow::Result<()> {
    let main = try_opening_poly_database(index_env, index_wtxn, db_name::MAIN)
        .with_context(|| format!("while updating date format for index `{index_uid}`"))?;

    date_round_trip(index_wtxn, index_uid, main, main_key::CREATED_AT_KEY)?;
    date_round_trip(index_wtxn, index_uid, main, main_key::UPDATED_AT_KEY)?;

    Ok(())
}

fn find_rest_embedders(
    index_uid: &str,
    index_env: &Env,
    index_txn: &RoTxn,
) -> anyhow::Result<Vec<String>> {
    let main = try_opening_poly_database(index_env, index_txn, db_name::MAIN)
        .with_context(|| format!("while checking REST embedders for index `{index_uid}`"))?;

    let mut rest_embedders = vec![];

    for config in main
        .remap_types::<Str, SerdeJson<Vec<v1_9::IndexEmbeddingConfig>>>()
        .get(index_txn, main_key::EMBEDDING_CONFIGS)?
        .unwrap_or_default()
    {
        if let v1_9::EmbedderOptions::Rest(_) = config.config.embedder_options {
            rest_embedders.push(config.name);
        }
    }

    Ok(rest_embedders)
}

fn date_round_trip(
    wtxn: &mut RwTxn,
    index_uid: &str,
    db: Database<Unspecified, Unspecified>,
    key: &str,
) -> anyhow::Result<()> {
    let datetime =
        db.remap_types::<Str, SerdeJson<v1_9::LegacyDateTime>>().get(wtxn, key).with_context(
            || format!("could not read `{key}` while updating date format for index `{index_uid}`"),
        )?;

    if let Some(datetime) = datetime {
        db.remap_types::<Str, SerdeJson<self::OffsetDateTime>>()
            .put(wtxn, key, &self::OffsetDateTime(datetime.0))
            .with_context(|| {
                format!(
                    "could not write `{key}` while updating date format for index `{index_uid}`"
                )
            })?;
    }

    Ok(())
}

pub fn v1_9_to_v1_10(
    db_path: &Path,
    _origin_major: u32,
    _origin_minor: u32,
    _origin_patch: u32,
) -> anyhow::Result<()> {
    println!("Upgrading from v1.9.0 to v1.10.0");
    // 2 changes here

    // 1. date format. needs to be done before opening the Index
    // 2. REST embedders. We don't support this case right now, so bail

    let index_scheduler_path = db_path.join("tasks");
    let env = unsafe { EnvOpenOptions::new().max_dbs(100).open(&index_scheduler_path) }
        .with_context(|| format!("While trying to open {:?}", index_scheduler_path.display()))?;

    let mut sched_wtxn = env.write_txn()?;

    let index_mapping: Database<Str, UuidCodec> =
        try_opening_database(&env, &sched_wtxn, "index-mapping")?;

    let index_stats: Database<UuidCodec, Unspecified> =
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

    let mut rest_embedders = Vec::new();

    let mut unwrapped_indexes = Vec::new();

    // check that update can take place
    for (index_index, result) in indexes.into_iter().enumerate() {
        let (uid, uuid) = result?;
        let index_path = db_path.join("indexes").join(uuid.to_string());

        println!(
            "[{}/{index_count}]Checking that update can take place for  `{uid}` at `{}`",
            index_index + 1,
            index_path.display()
        );

        let index_env = unsafe {
            // FIXME: fetch the 25 magic number from the index file
            EnvOpenOptions::new().max_dbs(25).open(&index_path).with_context(|| {
                format!("while opening index {uid} at '{}'", index_path.display())
            })?
        };

        let index_txn = index_env.read_txn().with_context(|| {
            format!(
                "while obtaining a write transaction for index {uid} at {}",
                index_path.display()
            )
        })?;

        println!("\t- Checking for incompatible embedders (REST embedders)");
        let rest_embedders_for_index = find_rest_embedders(&uid, &index_env, &index_txn)?;

        if rest_embedders_for_index.is_empty() {
            unwrapped_indexes.push((uid, uuid));
        } else {
            // no need to add to unwrapped indexes because we'll exit early
            rest_embedders.push((uid, rest_embedders_for_index));
        }
    }

    if !rest_embedders.is_empty() {
        let rest_embedders = rest_embedders
            .into_iter()
            .flat_map(|(index, embedders)| std::iter::repeat(index.clone()).zip(embedders))
            .map(|(index, embedder)| format!("\t- embedder `{embedder}` in index `{index}`"))
            .collect::<Vec<_>>()
            .join("\n");
        bail!("The update cannot take place because there are REST embedder(s). Remove them before proceeding with the update:\n{rest_embedders}\n\n\
            The database has not been modified and is still a valid v1.9 database.");
    }

    println!("Update can take place, updating");

    for (index_index, (uid, uuid)) in unwrapped_indexes.into_iter().enumerate() {
        let index_path = db_path.join("indexes").join(uuid.to_string());

        println!(
            "[{}/{index_count}]Updating index `{uid}` at `{}`",
            index_index + 1,
            index_path.display()
        );

        let index_env = unsafe {
            // FIXME: fetch the 25 magic number from the index file
            EnvOpenOptions::new().max_dbs(25).open(&index_path).with_context(|| {
                format!("while opening index {uid} at '{}'", index_path.display())
            })?
        };

        let mut index_wtxn = index_env.write_txn().with_context(|| {
            format!(
                "while obtaining a write transaction for index `{uid}` at `{}`",
                index_path.display()
            )
        })?;

        println!("\t- Updating index stats");
        update_index_stats(index_stats, &uid, uuid, &mut sched_wtxn)?;
        println!("\t- Updating date format");
        update_date_format(&uid, &index_env, &mut index_wtxn)?;

        index_wtxn.commit().with_context(|| {
            format!("while committing the write txn for index `{uid}` at {}", index_path.display())
        })?;
    }

    sched_wtxn.commit().context("while committing the write txn for the index-scheduler")?;

    println!("Upgrading database succeeded");

    Ok(())
}

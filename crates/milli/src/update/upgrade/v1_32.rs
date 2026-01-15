use std::collections::{BTreeSet, LinkedList};
use std::iter;
use std::ops::Bound;

use fst::Streamer as _;
use heed::types::{Bytes, DecodeIgnore, Unit};
use heed::{BytesDecode, Database, RwTxn};
use rand::SeedableRng as _;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use super::UpgradeIndex;
use crate::heed_codec::StrBEU16Codec;
use crate::progress::Progress;
use crate::update::new::steps::SettingsIndexerStep;
use crate::vector::VectorStore;
use crate::{make_enum_progress, Error, Index, InternalError, MustStopProcessing, Result};

pub(super) struct CleanupFidBasedDatabases();

impl UpgradeIndex for CleanupFidBasedDatabases {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        must_stop_processing: &MustStopProcessing,
        progress: Progress,
    ) -> Result<bool> {
        make_enum_progress! {
            enum CleanupFidBasedDatabases {
                RetrievingFidsToDelete,
                DeletingFidBasedDatabases,
            }
        };

        if matches!(std::env::var_os("MEILI_EXPERIMENTAL_DISABLE_FID_BASED_DATABASES_CLEANUP"), Some(x) if x != "false" && x != "0")
        {
            return Ok(false);
        }

        // Force-delete the fid-based databases for the fids that are not searchable.
        // This is a sanity cleanup step to ensure that the database is not corrupted.
        progress.update_progress(CleanupFidBasedDatabases::RetrievingFidsToDelete);
        let fid_map = index.fields_ids_map_with_metadata(wtxn)?;
        let fids_to_delete: BTreeSet<_> = fid_map
            .iter()
            .filter_map(|(id, _, metadata)| if !metadata.is_searchable() { Some(id) } else { None })
            .collect();

        if !fids_to_delete.is_empty() {
            progress.update_progress(CleanupFidBasedDatabases::DeletingFidBasedDatabases);
            delete_old_fid_based_databases_from_fids(
                wtxn,
                index,
                must_stop_processing,
                &fids_to_delete,
                &progress,
            )?;
        }

        Ok(false)
    }
    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 32, 0)
    }

    fn description(&self) -> &'static str {
        "Cleaning up the fid-based databases"
    }
}

/// Deletes entries related to field IDs that must no longer exist in the database.
pub fn delete_old_fid_based_databases_from_fids(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
    must_stop_processing: &MustStopProcessing,
    fids_to_delete: &BTreeSet<u16>,
    progress: &Progress,
) -> Result<()> {
    progress.update_progress(SettingsIndexerStep::DeletingOldWordFidDocids);
    let deleted = delete_old_word_fid_docids(
        wtxn,
        index,
        index.word_fid_docids.remap_data_type(),
        must_stop_processing,
        fids_to_delete,
    )?;
    tracing::debug!("We just deleted {deleted} old word-fid-docids");

    progress.update_progress(SettingsIndexerStep::DeletingOldFidWordCountDocids);
    delete_old_fid_word_count_docids(wtxn, index, must_stop_processing, fids_to_delete)?;

    progress.update_progress(SettingsIndexerStep::DeletingOldWordPrefixFidDocids);
    let deleted = delete_old_word_fid_docids(
        wtxn,
        index,
        index.word_prefix_fid_docids.remap_data_type(),
        must_stop_processing,
        fids_to_delete,
    )?;
    tracing::debug!("We just deleted {deleted} old word-prefix-fid-docids");

    Ok(())
}

fn delete_old_word_fid_docids<'txn>(
    wtxn: &mut RwTxn<'txn>,
    index: &Index,
    database: Database<StrBEU16Codec, Unit>,
    must_stop_processing: &MustStopProcessing,
    fids_to_delete: &BTreeSet<u16>,
) -> crate::Result<usize> {
    let results = fetch_keys_to_delete_in_parallel(wtxn, index, database, fids_to_delete)?;

    let database = database.remap_key_type::<Bytes>();
    let mut count = 0;
    for result in results.into_iter().flatten() {
        let keys = result?;
        if must_stop_processing.get() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }
        keys.into_iter().try_for_each(|key| {
            database.delete(wtxn, &key)?;
            count += 1;
            Ok(()) as Result<()>
        })?;
    }

    Ok(count)
}

fn delete_old_fid_word_count_docids(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
    must_stop_processing: &MustStopProcessing,
    fids_to_delete: &BTreeSet<u16>,
) -> Result<(), Error> {
    let db = index.field_id_word_count_docids.remap_data_type::<DecodeIgnore>();
    for &fid_to_delete in fids_to_delete {
        if must_stop_processing.get() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        let mut iter = db.prefix_iter_mut(wtxn, &(fid_to_delete, 0))?;
        while let Some(((fid, _word_count), ())) = iter.next().transpose()? {
            debug_assert_eq!(fid, fid_to_delete);
            // safety: We don't keep any references to the data.
            unsafe { iter.del_current()? };
        }
    }

    Ok(())
}

/// Fetches keys to delete in parallel by using the FST
/// to balance the work between threads.
fn fetch_keys_to_delete_in_parallel<'txn>(
    wtxn: &mut RwTxn<'txn>,
    index: &Index,
    database: Database<StrBEU16Codec, Unit>,
    fids_to_delete: &BTreeSet<u16>,
) -> Result<LinkedList<Vec<Result<Vec<Box<[u8]>>>>>> {
    let fst = index.words_fst(wtxn)?;

    let threads_count = rayon::current_num_threads() * 4;
    let keys_by_thread = fst.len().div_ceil(threads_count);

    // We iterate over the FST keys that represents the word dictionary and
    // roughly represents what can be found in the database we are cleaning.
    //
    // The database we are cleaning contains different words from the word
    // dictionary as it contains words from fields that are not indexed too
    // but it is mixed with indexed ones.
    //
    // We then divide equally the entries of the database to clean by
    // selecting ranges of keys that will be processed by each thread. We
    // also make sure not to specify the first and last keys to make sure
    // that if the fields to clean have keys that are higher or lower than
    // the first or last keys in the word dictionary we still find them.

    // Here We make sure to start with an unbounded
    // left bound for the first range
    let mut bounds = vec![None];
    let mut stream = fst.stream();
    let mut count = 0;
    while let Some(key) = stream.next() {
        let is_first = count == 0;
        let is_last = count == fst.len() - 1;

        // In this loop we make sure to account for every bounds
        // to divide the work between threads but not send the bounds
        // for the beginning or the end of the word dictionary
        if count % keys_by_thread == 0 && !(is_first || is_last) {
            bounds.push(Some(key.to_vec()));
        }

        count += 1;
    }

    // We now push the last bound that
    // defines the end of the last range
    bounds.push(None);

    // We create a thread pool and generate enough read transactions for each one of them.
    let pool = rayon::ThreadPoolBuilder::new().num_threads(threads_count).build()?;
    let rtxns = iter::repeat_with(|| index.env.nested_read_txn(wtxn))
        .take(bounds.len().saturating_sub(1))
        .collect::<heed::Result<Vec<_>>>()?;

    let results = pool.install(|| {
        rtxns
            .into_par_iter()
            .zip_eq(bounds.windows(2).collect::<Vec<_>>())
            .map(|(rtxn, win)| {
                let bound = match [win[0].as_deref(), win[1].as_deref()] {
                    [None, None] => (Bound::Unbounded, Bound::Unbounded),
                    [None, Some(end)] => (Bound::Unbounded, Bound::Excluded(end)),
                    [Some(start), None] => (Bound::Included(start), Bound::Unbounded),
                    [Some(start), Some(end)] => (Bound::Included(start), Bound::Excluded(end)),
                };

                let mut keys_to_delete = Vec::new();
                let iter = database.remap_types::<Bytes, DecodeIgnore>().range(&rtxn, &bound);
                for result in iter? {
                    let (key_bytes, ()) = result?;
                    let (_word, fid) =
                        StrBEU16Codec::bytes_decode(key_bytes).map_err(heed::Error::Decoding)?;

                    // TODO optimize allocations
                    if fids_to_delete.contains(&fid) {
                        keys_to_delete.push(key_bytes.to_vec().into_boxed_slice());
                    }
                }

                Ok(keys_to_delete) as crate::Result<_>
            })
            .collect_vec_list()
    });

    Ok(results)
}

/// Rebuilds the hannoy graph and do not touch to the embeddings.
///
/// This follows a bug in hannoy v0.0.9 and v0.1.0 where the graph
/// was not built correctly.
pub(super) struct RebuildHannoyGraph();

impl UpgradeIndex for RebuildHannoyGraph {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        must_stop_processing: &MustStopProcessing,
        progress: Progress,
    ) -> Result<bool> {
        let embedders = index.embedding_configs();
        let backend = index.get_vector_store(wtxn)?.unwrap_or_default();

        for config in embedders.embedding_configs(wtxn)? {
            let embedder_info = embedders.embedder_info(wtxn, &config.name)?.unwrap();
            let mut vector_store = VectorStore::new(
                backend,
                index.vector_store,
                embedder_info.embedder_id,
                config.config.quantized(),
            );

            let seed = rand::random();
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            vector_store.rebuild_graph(
                wtxn,
                progress.clone(),
                &mut rng,
                vector_store.dimensions(wtxn)?.unwrap(),
                &|| must_stop_processing.get(),
            )?;
        }

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 32, 0)
    }

    fn description(&self) -> &'static str {
        "Rebuilding graph links"
    }
}

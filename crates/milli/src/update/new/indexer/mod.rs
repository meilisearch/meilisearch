use std::collections::{BTreeMap, BTreeSet, LinkedList};
use std::iter;
use std::ops::Bound;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Once, RwLock};
use std::thread::{self, Builder};

use big_s::S;
use document_changes::{DocumentChanges, IndexingContext};
pub use document_deletion::DocumentDeletion;
pub use document_operation::{IndexOperations, PayloadStats};
use fst::Streamer as _;
use hashbrown::HashMap;
use heed::types::{Bytes, DecodeIgnore, Unit};
use heed::{BytesDecode, Database, RoTxn, RwTxn};
pub use partial_dump::PartialDump;
pub use post_processing::recompute_word_fst_from_word_docids_database;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
pub use settings_changes::settings_change_extract;
pub use update_by_function::UpdateByFunction;
pub use write::ChannelCongestion;
use write::{build_vectors, update_index, write_to_db};

use super::channel::*;
use super::steps::IndexingStep;
use super::thread_local::ThreadLocal;
use crate::documents::PrimaryKey;
use crate::fields_ids_map::metadata::{FieldIdMapWithMetadata, MetadataBuilder};
use crate::heed_codec::StrBEU16Codec;
use crate::progress::{EmbedderStats, Progress};
use crate::proximity::ProximityPrecision;
use crate::update::new::steps::SettingsIndexerStep;
use crate::update::new::FacetFieldIdsDelta;
use crate::update::settings::SettingsDelta;
use crate::update::GrenadParameters;
use crate::vector::settings::{EmbedderAction, RemoveFragments, WriteBackToDocuments};
use crate::vector::{Embedder, RuntimeEmbedders, VectorStore};
use crate::{
    Error, FieldsIdsMap, GlobalFieldsIdsMap, Index, InternalError, Result, ThreadPoolNoAbort,
};

pub(crate) mod de;
pub mod document_changes;
mod document_deletion;
mod document_operation;
mod extract;
mod guess_primary_key;
mod partial_dump;
mod post_processing;
pub mod settings_changes;
mod update_by_function;
mod write;

static LOG_MEMORY_METRICS_ONCE: Once = Once::new();

/// This is the main function of this crate.
///
/// Give it the output of the [`Indexer::document_changes`] method and it will execute it in the [`rayon::ThreadPool`].
#[allow(clippy::too_many_arguments)] // clippy: 😝
pub fn index<'pl, 'indexer, 'index, DC, MSP>(
    wtxn: &mut RwTxn,
    index: &'index Index,
    pool: &ThreadPoolNoAbort,
    grenad_parameters: GrenadParameters,
    db_fields_ids_map: &'indexer FieldsIdsMap,
    new_fields_ids_map: FieldsIdsMap,
    new_primary_key: Option<PrimaryKey<'pl>>,
    document_changes: &DC,
    embedders: RuntimeEmbedders,
    must_stop_processing: &'indexer MSP,
    progress: &'indexer Progress,
    embedder_ip_policy: &'indexer http_client::policy::IpPolicy,
    embedder_stats: &'indexer EmbedderStats,
) -> Result<ChannelCongestion>
where
    DC: DocumentChanges<'pl>,
    MSP: Fn() -> bool + Sync,
{
    let mut bbbuffers = Vec::new();
    let finished_extraction = AtomicBool::new(false);

    let vector_memory = grenad_parameters.max_memory;

    let (grenad_parameters, total_bbbuffer_capacity) =
        indexer_memory_settings(pool.current_num_threads(), grenad_parameters);

    let (extractor_sender, writer_receiver) = pool
        .install(|| extractor_writer_bbqueue(&mut bbbuffers, total_bbbuffer_capacity, 1000))
        .unwrap();

    let metadata_builder = MetadataBuilder::from_index(index, wtxn)?;
    let new_fields_ids_map = FieldIdMapWithMetadata::new(new_fields_ids_map, metadata_builder);
    let new_fields_ids_map = RwLock::new(new_fields_ids_map);
    let fields_ids_map_store = ThreadLocal::with_capacity(rayon::current_num_threads());
    let mut extractor_allocs = ThreadLocal::with_capacity(rayon::current_num_threads());
    let doc_allocs = ThreadLocal::with_capacity(rayon::current_num_threads());

    let indexing_context = IndexingContext {
        index,
        db_fields_ids_map,
        new_fields_ids_map: &new_fields_ids_map,
        doc_allocs: &doc_allocs,
        fields_ids_map_store: &fields_ids_map_store,
        must_stop_processing,
        progress,
        grenad_parameters: &grenad_parameters,
    };

    let index_embeddings = index.embedding_configs().embedding_configs(wtxn)?;
    let mut field_distribution = index.field_distribution(wtxn)?;
    let mut document_ids = index.documents_ids(wtxn)?;
    let mut modified_docids = roaring::RoaringBitmap::new();

    let congestion = thread::scope(|s| -> Result<ChannelCongestion> {
        let indexer_span = tracing::Span::current();
        let embedders = &embedders;
        let finished_extraction = &finished_extraction;
        // prevent moving the field_distribution and document_ids in the inner closure...
        let field_distribution = &mut field_distribution;
        let document_ids = &mut document_ids;
        let modified_docids = &mut modified_docids;
        let extractor_handle =
            Builder::new().name(S("indexer-extractors")).spawn_scoped(s, move || {
                pool.install(move || {
                    extract::extract_all(
                        document_changes,
                        indexing_context,
                        indexer_span,
                        extractor_sender,
                        embedders,
                        &mut extractor_allocs,
                        finished_extraction,
                        field_distribution,
                        index_embeddings,
                        document_ids,
                        modified_docids,
                        embedder_stats,
                    )
                })
                .unwrap()
            })?;

        let global_fields_ids_map = GlobalFieldsIdsMap::new(&new_fields_ids_map);

        let vector_arroy = index.vector_store;
        let backend = index.get_vector_store(wtxn)?.unwrap_or_default();
        let vector_stores: Result<HashMap<_, _>> = embedders
            .inner_as_ref()
            .iter()
            .map(|(embedder_name, runtime)| {
                let embedder_index = index
                    .embedding_configs()
                    .embedder_id(wtxn, embedder_name)?
                    .ok_or(InternalError::DatabaseMissingEntry {
                        db_name: "embedder_category_id",
                        key: None,
                    })?;

                let dimensions = runtime.embedder.dimensions();
                let writer =
                    VectorStore::new(backend, vector_arroy, embedder_index, runtime.is_quantized);

                Ok((
                    embedder_index,
                    (embedder_name.as_str(), &*runtime.embedder, writer, dimensions),
                ))
            })
            .collect();

        let mut vector_stores = vector_stores?;

        let congestion =
            write_to_db(writer_receiver, finished_extraction, index, wtxn, &vector_stores)?;

        indexing_context.progress.update_progress(IndexingStep::WaitingForExtractors);

        let (facet_field_ids_delta, index_embeddings) = extractor_handle.join().unwrap()?;

        indexing_context.progress.update_progress(IndexingStep::WritingEmbeddingsToDatabase);

        pool.install(|| {
            build_vectors(
                index,
                wtxn,
                indexing_context.progress,
                index_embeddings,
                vector_memory,
                &mut vector_stores,
                None,
                &indexing_context.must_stop_processing,
            )
        })
        .unwrap()?;

        pool.install(|| {
            post_processing::post_process(
                indexing_context,
                wtxn,
                global_fields_ids_map,
                facet_field_ids_delta,
            )
        })
        .unwrap()?;

        indexing_context.progress.update_progress(IndexingStep::BuildingGeoJson);
        index.cellulite.build(
            wtxn,
            &indexing_context.must_stop_processing,
            indexing_context.progress,
        )?;

        indexing_context.progress.update_progress(IndexingStep::Finalizing);

        Ok(congestion) as Result<_>
    })?;

    // required to into_inner the new_fields_ids_map
    drop(fields_ids_map_store);

    let shard_docids = index.shard_docids();
    shard_docids
        .update_shards(wtxn, |shard, docids| document_changes.shard_docids(shard, docids))?;

    let new_fields_ids_map = new_fields_ids_map.into_inner().unwrap();
    update_index(
        index,
        wtxn,
        new_fields_ids_map,
        new_primary_key,
        embedders,
        embedder_ip_policy,
        field_distribution,
        document_ids,
    )?;

    Ok(congestion)
}

#[allow(clippy::too_many_arguments)]
pub fn reindex<'indexer, 'index, MSP, SD>(
    wtxn: &mut RwTxn<'index>,
    index: &'index Index,
    pool: &ThreadPoolNoAbort,
    grenad_parameters: GrenadParameters,
    settings_delta: &'indexer SD,
    must_stop_processing: &'indexer MSP,
    progress: &'indexer Progress,
    embedder_ip_policy: &'indexer http_client::policy::IpPolicy,
    embedder_stats: Arc<EmbedderStats>,
) -> Result<ChannelCongestion>
where
    MSP: Fn() -> bool + Sync,
    SD: SettingsDelta + Sync,
{
    delete_old_embedders_and_fragments(wtxn, index, settings_delta)?;
    delete_old_fid_based_databases(wtxn, index, settings_delta, must_stop_processing, progress)?;

    // Clear word_pair_proximity if byWord to byAttribute
    let old_proximity_precision = settings_delta.old_proximity_precision();
    let new_proximity_precision = settings_delta.new_proximity_precision();
    if *old_proximity_precision == ProximityPrecision::ByWord
        && *new_proximity_precision == ProximityPrecision::ByAttribute
    {
        index.word_pair_proximity_docids.clear(wtxn)?;
    }

    // TODO delete useless searchable databases
    //      - Clear fid_prefix_* in the post processing
    //      - clear the prefix + fid_prefix if setting `PrefixSearch` is enabled

    let mut bbbuffers = Vec::new();
    let finished_extraction = AtomicBool::new(false);

    let vector_memory = grenad_parameters.max_memory;

    let (grenad_parameters, total_bbbuffer_capacity) =
        indexer_memory_settings(pool.current_num_threads(), grenad_parameters);

    let (extractor_sender, writer_receiver) = pool
        .install(|| extractor_writer_bbqueue(&mut bbbuffers, total_bbbuffer_capacity, 1000))
        .unwrap();

    let mut extractor_allocs = ThreadLocal::with_capacity(rayon::current_num_threads());

    let db_fields_ids_map = index.fields_ids_map(wtxn)?;
    let new_fields_ids_map = settings_delta.new_fields_ids_map().clone();
    let new_fields_ids_map = RwLock::new(new_fields_ids_map);
    let fields_ids_map_store = ThreadLocal::with_capacity(rayon::current_num_threads());
    let doc_allocs = ThreadLocal::with_capacity(rayon::current_num_threads());

    let indexing_context = IndexingContext {
        index,
        db_fields_ids_map: &db_fields_ids_map,
        new_fields_ids_map: &new_fields_ids_map,
        doc_allocs: &doc_allocs,
        fields_ids_map_store: &fields_ids_map_store,
        must_stop_processing,
        progress,
        grenad_parameters: &grenad_parameters,
    };

    let index_embeddings = index.embedding_configs().embedding_configs(wtxn)?;
    let mut field_distribution = index.field_distribution(wtxn)?;

    let congestion = thread::scope(|s| -> Result<ChannelCongestion> {
        let indexer_span = tracing::Span::current();
        let finished_extraction = &finished_extraction;
        // prevent moving the field_distribution and document_ids in the inner closure...
        let field_distribution = &mut field_distribution;
        let extractor_handle =
            Builder::new().name(S("indexer-extractors")).spawn_scoped(s, move || {
                pool.install(move || {
                    extract::extract_all_settings_changes(
                        indexing_context,
                        indexer_span,
                        extractor_sender,
                        settings_delta,
                        &mut extractor_allocs,
                        finished_extraction,
                        field_distribution,
                        index_embeddings,
                        &embedder_stats,
                    )
                })
                .unwrap()
            })?;

        let global_fields_ids_map = GlobalFieldsIdsMap::new(&new_fields_ids_map);

        let new_embedders = settings_delta.new_embedders();
        let embedder_actions = settings_delta.embedder_actions();
        let index_embedder_category_ids = settings_delta.new_embedder_category_id();
        let mut vector_stores = vector_stores_from_embedder_actions(
            index,
            wtxn,
            embedder_actions,
            new_embedders,
            index_embedder_category_ids,
        )?;

        let congestion =
            write_to_db(writer_receiver, finished_extraction, index, wtxn, &vector_stores)?;

        indexing_context.progress.update_progress(IndexingStep::WaitingForExtractors);

        let index_embeddings = extractor_handle.join().unwrap()?;

        indexing_context.progress.update_progress(IndexingStep::WritingEmbeddingsToDatabase);

        pool.install(|| {
            build_vectors(
                index,
                wtxn,
                indexing_context.progress,
                index_embeddings,
                vector_memory,
                &mut vector_stores,
                Some(embedder_actions),
                &indexing_context.must_stop_processing,
            )
        })
        .unwrap()?;

        pool.install(|| {
            // WARN When implementing the facets don't forget this
            let facet_field_ids_delta = FacetFieldIdsDelta::new(0, 0);
            post_processing::post_process(
                indexing_context,
                wtxn,
                global_fields_ids_map,
                facet_field_ids_delta,
            )
        })
        .unwrap()?;

        indexing_context.progress.update_progress(IndexingStep::BuildingGeoJson);
        index.cellulite.build(
            wtxn,
            &indexing_context.must_stop_processing,
            indexing_context.progress,
        )?;

        indexing_context.progress.update_progress(IndexingStep::Finalizing);

        Ok(congestion) as Result<_>
    })?;

    // required to into_inner the new_fields_ids_map
    drop(fields_ids_map_store);

    let new_fields_ids_map = new_fields_ids_map.into_inner().unwrap();
    let document_ids = index.documents_ids(wtxn)?;
    update_index(
        index,
        wtxn,
        new_fields_ids_map,
        None,
        settings_delta.new_embedders().clone(),
        embedder_ip_policy,
        field_distribution,
        document_ids,
    )?;

    Ok(congestion)
}

fn vector_stores_from_embedder_actions<'indexer>(
    index: &Index,
    rtxn: &RoTxn,
    embedder_actions: &'indexer BTreeMap<String, EmbedderAction>,
    embedders: &'indexer RuntimeEmbedders,
    index_embedder_category_ids: &'indexer std::collections::HashMap<String, u8>,
) -> Result<HashMap<u8, (&'indexer str, &'indexer Embedder, VectorStore, usize)>> {
    let vector_arroy = index.vector_store;
    let backend = index.get_vector_store(rtxn)?.unwrap_or_default();

    embedders
        .inner_as_ref()
        .iter()
        .filter_map(|(embedder_name, runtime)| match embedder_actions.get(embedder_name) {
            None => None,
            Some(action) if action.write_back().is_some() => None,
            Some(action) => {
                let Some(&embedder_category_id) = index_embedder_category_ids.get(embedder_name)
                else {
                    return Some(Err(crate::error::Error::InternalError(
                        crate::InternalError::DatabaseMissingEntry {
                            db_name: crate::index::db_name::VECTOR_EMBEDDER_CATEGORY_ID,
                            key: None,
                        },
                    )));
                };
                let writer = VectorStore::new(
                    backend,
                    vector_arroy,
                    embedder_category_id,
                    action.was_quantized,
                );
                let dimensions = runtime.embedder.dimensions();
                Some(Ok((
                    embedder_category_id,
                    (embedder_name.as_str(), runtime.embedder.as_ref(), writer, dimensions),
                )))
            }
        })
        .collect()
}

fn delete_old_embedders_and_fragments<SD>(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
    settings_delta: &SD,
) -> Result<()>
where
    SD: SettingsDelta,
{
    let backend = index.get_vector_store(wtxn)?.unwrap_or_default();
    for action in settings_delta.embedder_actions().values() {
        let Some(WriteBackToDocuments { embedder_id, .. }) = action.write_back() else {
            continue;
        };
        let reader =
            VectorStore::new(backend, index.vector_store, *embedder_id, action.was_quantized);
        let Some(dimensions) = reader.dimensions(wtxn)? else {
            continue;
        };
        reader.clear(wtxn, dimensions)?;
    }

    // remove all vectors for the specified fragments
    for (embedder_name, RemoveFragments { fragment_ids }, was_quantized) in
        settings_delta.embedder_actions().iter().filter_map(|(name, action)| {
            action.remove_fragments().map(|fragments| (name, fragments, action.was_quantized))
        })
    {
        let Some(infos) = index.embedding_configs().embedder_info(wtxn, embedder_name)? else {
            continue;
        };
        let arroy = VectorStore::new(backend, index.vector_store, infos.embedder_id, was_quantized);
        let Some(dimensions) = arroy.dimensions(wtxn)? else {
            continue;
        };
        for fragment_id in fragment_ids {
            // we must keep the user provided embeddings that ended up in this store

            if infos.embedding_status.user_provided_docids().is_empty() {
                // no user provided: clear store
                arroy.clear_store(wtxn, *fragment_id, dimensions)?;
                continue;
            }

            // some user provided, remove only the ids that are not user provided
            let to_delete = arroy.items_in_store(wtxn, *fragment_id, |items| {
                items - infos.embedding_status.user_provided_docids()
            })?;

            for to_delete in to_delete {
                arroy.del_item_in_store(wtxn, to_delete, *fragment_id, dimensions)?;
            }
        }
    }

    Ok(())
}

/// Deletes entries refering the provided
/// fids from the fid-based databases.
pub fn delete_old_fid_based_databases<SD, MSP>(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
    settings_delta: &SD,
    must_stop_processing: &MSP,
    progress: &Progress,
) -> Result<()>
where
    SD: SettingsDelta + Sync,
    MSP: Fn() -> bool + Sync,
{
    // Get the fids to delete from the settings delta.
    // Compare the old and new fields ids map to find the fids that are no longer searchable.
    let fids_to_delete: BTreeSet<_> = {
        let old_fields_ids_map = settings_delta.old_fields_ids_map();
        let new_fields_ids_map = settings_delta.new_fields_ids_map();
        old_fields_ids_map
            .iter_id_metadata()
            .filter_map(|(id, metadata)| {
                if metadata.is_searchable()
                    && new_fields_ids_map
                        .metadata(id)
                        .is_none_or(|metadata| !metadata.is_searchable())
                {
                    Some(id)
                } else {
                    None
                }
            })
            .collect()
    };

    if fids_to_delete.is_empty() {
        return Ok(());
    };

    delete_old_fid_based_databases_from_fids(
        wtxn,
        index,
        must_stop_processing,
        &fids_to_delete,
        progress,
    )
}

/// Deletes entries related to field IDs that must no longer exist in the database.
/// Uses parallel fetching to speed up the deletion process.
pub fn delete_old_fid_based_databases_from_fids<MSP>(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
    must_stop_processing: &MSP,
    fids_to_delete: &BTreeSet<u16>,
    progress: &Progress,
) -> Result<()>
where
    MSP: Fn() -> bool + Sync,
{
    let bounds = compute_fst_bounds(wtxn, index)?;

    progress.update_progress(SettingsIndexerStep::DeletingOldWordFidDocids);
    delete_old_word_fid_docids_parallel(
        wtxn,
        index,
        index.word_fid_docids.remap_data_type(),
        must_stop_processing,
        fids_to_delete,
        &bounds,
    )?;

    progress.update_progress(SettingsIndexerStep::DeletingOldFidWordCountDocids);
    delete_old_fid_word_count_docids(wtxn, index, must_stop_processing, fids_to_delete)?;

    progress.update_progress(SettingsIndexerStep::DeletingOldWordPrefixFidDocids);
    delete_old_word_fid_docids_parallel(
        wtxn,
        index,
        index.word_prefix_fid_docids.remap_data_type(),
        must_stop_processing,
        fids_to_delete,
        &bounds,
    )?;

    Ok(())
}

/// Use the FST to balance the work between threads
/// by generating appropriate word bounds.
fn compute_fst_bounds(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
) -> crate::Result<Vec<Option<Box<[u8]>>>> {
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

    // Here we make sure to start with an unbounded
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
            bounds.push(Some(key.to_vec().into_boxed_slice()));
        }

        count += 1;
    }

    // We now push the last bound that
    // defines the end of the last range
    bounds.push(None);

    Ok(bounds)
}

/// Fetches keys to delete in parallel by using the FST bounds
/// to balance the work between threads.
fn fetch_keys_to_delete_in_parallel(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
    database: Database<StrBEU16Codec, Unit>,
    fids_to_delete: &BTreeSet<u16>,
    bounds: &[Option<Box<[u8]>>],
) -> Result<LinkedList<Vec<Result<Vec<Box<[u8]>>>>>> {
    // We generate enough read transactions for each thread.
    let rtxns = iter::repeat_with(|| index.env.nested_read_txn(wtxn))
        .take(bounds.len().saturating_sub(1))
        .collect::<heed::Result<Vec<_>>>()?;

    // Run parallel fetching directly in the current rayon threadpool
    let results = rtxns
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

                if fids_to_delete.contains(&fid) {
                    keys_to_delete.push(key_bytes.to_vec().into_boxed_slice());
                }
            }

            Ok(keys_to_delete) as crate::Result<_>
        })
        .collect_vec_list();

    Ok(results)
}

/// Parallel version of delete_old_word_fid_docids that fetches keys in parallel
/// and then deletes them sequentially.
fn delete_old_word_fid_docids_parallel<MSP>(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
    database: Database<StrBEU16Codec, Unit>,
    must_stop_processing: &MSP,
    fids_to_delete: &BTreeSet<u16>,
    bounds: &[Option<Box<[u8]>>],
) -> crate::Result<usize>
where
    MSP: Fn() -> bool + Sync,
{
    let results = fetch_keys_to_delete_in_parallel(wtxn, index, database, fids_to_delete, bounds)?;

    let database = database.remap_key_type::<Bytes>();
    let mut count = 0;
    for result in results.into_iter().flatten() {
        let keys = result?;
        if must_stop_processing() {
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

fn delete_old_fid_word_count_docids<MSP>(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
    must_stop_processing: &MSP,
    fids_to_delete: &BTreeSet<u16>,
) -> Result<(), Error>
where
    MSP: Fn() -> bool + Sync,
{
    let db = index.field_id_word_count_docids.remap_data_type::<DecodeIgnore>();
    for &fid_to_delete in fids_to_delete {
        if must_stop_processing() {
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

fn indexer_memory_settings(
    current_num_threads: usize,
    grenad_parameters: GrenadParameters,
) -> (GrenadParameters, usize) {
    // We reduce the actual memory used to 5%. The reason we do this here and not in Meilisearch
    // is because we still use the old indexer for the settings and it is highly impacted by the
    // max memory. So we keep the changes here and will remove these changes once we use the new
    // indexer to also index settings. Related to #5125 and #5141.
    let grenad_parameters = GrenadParameters {
        max_memory: grenad_parameters.max_memory.map(|mm| mm * 5 / 100),
        ..grenad_parameters
    };

    // 5% percent of the allocated memory for the extractors, or min 100MiB
    // 5% percent of the allocated memory for the bbqueues, or min 50MiB
    //
    // Minimum capacity for bbqueues
    let minimum_total_bbbuffer_capacity = 50 * 1024 * 1024 * current_num_threads;
    // 50 MiB
    let minimum_total_extractors_capacity = minimum_total_bbbuffer_capacity * 2;

    let (grenad_parameters, total_bbbuffer_capacity) = grenad_parameters.max_memory.map_or(
        (
            GrenadParameters {
                max_memory: Some(minimum_total_extractors_capacity),
                ..grenad_parameters
            },
            minimum_total_bbbuffer_capacity,
        ), // 100 MiB by thread by default
        |max_memory| {
            let total_bbbuffer_capacity = max_memory.max(minimum_total_bbbuffer_capacity);
            let new_grenad_parameters = GrenadParameters {
                max_memory: Some(max_memory.max(minimum_total_extractors_capacity)),
                ..grenad_parameters
            };
            (new_grenad_parameters, total_bbbuffer_capacity)
        },
    );

    LOG_MEMORY_METRICS_ONCE.call_once(|| {
        tracing::debug!(
            "Indexation allocated memory metrics - \
            Total BBQueue size: {total_bbbuffer_capacity}, \
            Total extractor memory: {:?}",
            grenad_parameters.max_memory,
        );
    });

    (grenad_parameters, total_bbbuffer_capacity)
}

/// Rebuild the geo RTree from scratch by reading all documents.
/// Used during settings-triggered reindex because `extract_all_settings_changes`
/// does not run the GeoExtractor.
pub fn rebuild_geo_rtree(index: &Index, wtxn: &mut RwTxn<'_>) -> Result<()> {
    use roaring::RoaringBitmap;
    use rstar::RTree;
    use serde_json::value::RawValue;

    use crate::update::new::extract::{extract_geo_coordinates, extract_geo_list_coordinates};
    use crate::{lat_lng_to_xyz, GeoPoint};

    let fields_ids_map = index.fields_ids_map(wtxn)?;
    let geo_fid = fields_ids_map.id("_geo");
    let geo_list_fid = fields_ids_map.id("_geo_list");

    // If neither _geo nor _geo_list fields exist in the field map, nothing to do
    if geo_fid.is_none() && geo_list_fid.is_none() {
        return Ok(());
    }

    let mut rtree = RTree::new();
    let mut faceted = RoaringBitmap::new();

    let all_docids = index.documents_ids(wtxn)?;
    for docid in all_docids {
        let doc = index.document(wtxn, docid)?;
        let mut has_geo = false;

        if let Some(fid) = geo_fid {
            if let Some(value) = doc.get(fid) {
                let raw_value: &RawValue =
                    serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;
                if let Some(point) = extract_geo_coordinates("", raw_value)? {
                    let xyz = lat_lng_to_xyz(&point);
                    rtree.insert(GeoPoint::new(xyz, (docid, point)));
                    has_geo = true;
                }
            }
        }

        if let Some(fid) = geo_list_fid {
            if let Some(value) = doc.get(fid) {
                let raw_value: &RawValue =
                    serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;
                if let Some(points) = extract_geo_list_coordinates("", raw_value)? {
                    for point in points {
                        let xyz = lat_lng_to_xyz(&point);
                        rtree.insert(GeoPoint::new(xyz, (docid, point)));
                    }
                    has_geo = true;
                }
            }
        }

        if has_geo {
            faceted.insert(docid);
        }
    }

    index.put_geo_rtree(wtxn, &rtree)?;
    index.put_geo_faceted_documents_ids(wtxn, &faceted)?;

    Ok(())
}

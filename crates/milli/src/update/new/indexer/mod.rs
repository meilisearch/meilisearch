use std::cmp::Ordering;
use std::sync::RwLock;
use std::thread::{self, Builder};

use big_s::S;
use document_changes::{
    for_each_document_change, DocumentChanges, FullySend, IndexingContext, ThreadLocal,
};
pub use document_deletion::DocumentDeletion;
pub use document_operation::DocumentOperation;
use heed::types::{Bytes, DecodeIgnore, Str};
use heed::{RoTxn, RwTxn};
use itertools::{merge_join_by, EitherOrBoth};
pub use partial_dump::PartialDump;
use rayon::ThreadPool;
use time::OffsetDateTime;
pub use update_by_function::UpdateByFunction;

use super::channel::*;
use super::extract::*;
use super::facet_search_builder::FacetSearchBuilder;
use super::merger::{FacetDatabases, FacetFieldIdsDelta};
use super::word_fst_builder::PrefixDelta;
use super::words_prefix_docids::{
    compute_word_prefix_docids, compute_word_prefix_fid_docids, compute_word_prefix_position_docids,
};
use super::{StdResult, TopLevelMap};
use crate::documents::{PrimaryKey, DEFAULT_PRIMARY_KEY};
use crate::facet::FacetType;
use crate::index::main_key::{WORDS_FST_KEY, WORDS_PREFIXES_FST_KEY};
use crate::proximity::ProximityPrecision;
use crate::update::del_add::DelAdd;
use crate::update::new::word_fst_builder::{PrefixData, WordFstBuilder};
use crate::update::new::words_prefix_docids::compute_exact_word_prefix_docids;
use crate::update::new::{merge_and_send_docids, merge_and_send_facet_docids};
use crate::update::settings::InnerIndexSettings;
use crate::update::{FacetsUpdateBulk, GrenadParameters};
use crate::{FieldsIdsMap, GlobalFieldsIdsMap, Index, Result, UserError};

pub mod de;
pub mod document_changes;
mod document_deletion;
mod document_operation;
mod partial_dump;
mod update_by_function;

/// This is the main function of this crate.
///
/// Give it the output of the [`Indexer::document_changes`] method and it will execute it in the [`rayon::ThreadPool`].
///
/// TODO return stats
pub fn index<'pl, 'indexer, 'index, DC>(
    wtxn: &mut RwTxn,
    index: &'index Index,
    db_fields_ids_map: &'indexer FieldsIdsMap,
    new_fields_ids_map: FieldsIdsMap,
    new_primary_key: Option<PrimaryKey<'pl>>,
    pool: &ThreadPool,
    document_changes: &DC,
) -> Result<()>
where
    DC: DocumentChanges<'pl>,
{
    // TODO find a better channel limit
    let (extractor_sender, writer_receiver) = extractor_writer_channel(10_000);
    let new_fields_ids_map = RwLock::new(new_fields_ids_map);

    let global_fields_ids_map = GlobalFieldsIdsMap::new(&new_fields_ids_map);
    let fields_ids_map_store = ThreadLocal::with_capacity(pool.current_num_threads());
    let mut extractor_allocs = ThreadLocal::with_capacity(pool.current_num_threads());
    let doc_allocs = ThreadLocal::with_capacity(pool.current_num_threads());

    let indexing_context = IndexingContext {
        index,
        db_fields_ids_map,
        new_fields_ids_map: &new_fields_ids_map,
        doc_allocs: &doc_allocs,
        fields_ids_map_store: &fields_ids_map_store,
    };

    thread::scope(|s| -> crate::Result<_> {
        let indexer_span = tracing::Span::current();
        // TODO manage the errors correctly
        let extractor_handle = Builder::new().name(S("indexer-extractors")).spawn_scoped(s, move || {
            pool.in_place_scope(|_s| {
                let span = tracing::trace_span!(target: "indexing::documents", parent: &indexer_span, "extract");
                let _entered = span.enter();

                // document but we need to create a function that collects and compresses documents.
                let rtxn = index.read_txn().unwrap();
                let document_sender = extractor_sender.documents();
                let document_extractor = DocumentsExtractor::new(&document_sender);
                let datastore = ThreadLocal::with_capacity(pool.current_num_threads());
                for_each_document_change(document_changes, &document_extractor, indexing_context, &mut extractor_allocs, &datastore)?;

                let mut documents_ids = index.documents_ids(&rtxn)?;
                let delta_documents_ids = datastore.into_iter().map(|FullySend(d)| d.into_inner()).reduce(DelAddRoaringBitmap::merge).unwrap_or_default();
                delta_documents_ids.apply_to(&mut documents_ids);
                extractor_sender.send_documents_ids(documents_ids).unwrap();

                // document_sender.finish().unwrap();

                const TEN_GIB: usize = 10 * 1024 * 1024 * 1024;
                let current_num_threads = rayon::current_num_threads();
                let max_memory = TEN_GIB / current_num_threads;
                eprintln!("A maximum of {max_memory} bytes will be used for each of the {current_num_threads} threads");
                let grenad_parameters = GrenadParameters {
                    max_memory: Some(max_memory),
                    ..GrenadParameters::default()
                };

                let facet_field_ids_delta;

                {
                    let span = tracing::trace_span!(target: "indexing::documents::extract", "faceted");
                    let _entered = span.enter();
                    facet_field_ids_delta = merge_and_send_facet_docids(
                        FacetedDocidsExtractor::run_extraction(grenad_parameters, document_changes, indexing_context, &mut extractor_allocs)?,
                        FacetDatabases::new(index),
                        index,
                        extractor_sender.facet_docids(),
                    )?;
                }

                {
                    let span = tracing::trace_span!(target: "indexing::documents::extract", "word_docids");
                    let _entered = span.enter();

                    let WordDocidsCaches {
                        word_docids,
                        word_fid_docids,
                        exact_word_docids,
                        word_position_docids,
                        fid_word_count_docids,
                    } = WordDocidsExtractors::run_extraction(grenad_parameters, document_changes, indexing_context, &mut extractor_allocs)?;

                    // TODO Word Docids Merger
                    // extractor_sender.send_searchable::<WordDocids>(word_docids).unwrap();
                    {
                        let span = tracing::trace_span!(target: "indexing::documents::merge", "word_docids");
                        let _entered = span.enter();
                        merge_and_send_docids(
                            word_docids,
                            index.word_docids.remap_types(),
                            index,
                            extractor_sender.docids::<WordDocids>(),
                        )?;
                    }

                    // Word Fid Docids Merging
                    // extractor_sender.send_searchable::<WordFidDocids>(word_fid_docids).unwrap();
                    {
                        let span = tracing::trace_span!(target: "indexing::documents::merge", "word_fid_docids");
                        let _entered = span.enter();
                        merge_and_send_docids(
                            word_fid_docids,
                            index.word_fid_docids.remap_types(),
                            index,
                            extractor_sender.docids::<WordFidDocids>()
                        )?;
                    }

                    // Exact Word Docids Merging
                    // extractor_sender.send_searchable::<ExactWordDocids>(exact_word_docids).unwrap();
                    {
                        let span = tracing::trace_span!(target: "indexing::documents::merge", "exact_word_docids");
                        let _entered = span.enter();
                        merge_and_send_docids(
                            exact_word_docids,
                            index.exact_word_docids.remap_types(),
                            index,
                            extractor_sender.docids::<ExactWordDocids>(),
                        )?;
                    }

                    // Word Position Docids Merging
                    // extractor_sender.send_searchable::<WordPositionDocids>(word_position_docids).unwrap();
                    {
                        let span = tracing::trace_span!(target: "indexing::documents::merge", "word_position_docids");
                        let _entered = span.enter();
                        merge_and_send_docids(
                            word_position_docids,
                            index.word_position_docids.remap_types(),
                            index,
                            extractor_sender.docids::<WordPositionDocids>(),
                        )?;
                    }

                    // Fid Word Count Docids Merging
                    // extractor_sender.send_searchable::<FidWordCountDocids>(fid_word_count_docids).unwrap();
                    {
                        let span = tracing::trace_span!(target: "indexing::documents::merge", "fid_word_count_docids");
                        let _entered = span.enter();
                        merge_and_send_docids(
                            fid_word_count_docids,
                            index.field_id_word_count_docids.remap_types(),
                            index,
                            extractor_sender.docids::<FidWordCountDocids>(),
                        )?;
                    }
                }

                // run the proximity extraction only if the precision is by word
                // this works only if the settings didn't change during this transaction.
                let proximity_precision = index.proximity_precision(&rtxn)?.unwrap_or_default();
                if proximity_precision == ProximityPrecision::ByWord {
                    let span = tracing::trace_span!(target: "indexing::documents::extract", "word_pair_proximity_docids");
                    let _entered = span.enter();
                    let caches = <WordPairProximityDocidsExtractor as DocidsExtractor>::run_extraction(grenad_parameters, document_changes, indexing_context, &mut extractor_allocs)?;
                    merge_and_send_docids(
                        caches,
                        index.word_pair_proximity_docids.remap_types(),
                        index,
                        extractor_sender.docids::<WordPairProximityDocids>(),
                    )?;
                }

                {
                    let span = tracing::trace_span!(target: "indexing::documents::extract", "FINISH");
                    let _entered = span.enter();
                }

                // TODO THIS IS TOO MUCH
                // - [ ] Extract fieldid docid facet number
                // - [ ] Extract fieldid docid facet string
                // - [ ] Extract facetid string fst
                // - [ ] Extract facetid normalized string strings

                // TODO Inverted Indexes again
                // - [x] Extract fieldid facet isempty docids
                // - [x] Extract fieldid facet isnull docids
                // - [x] Extract fieldid facet exists docids

                // TODO This is the normal system
                // - [x] Extract fieldid facet number docids
                // - [x] Extract fieldid facet string docids

                // TODO use None when needed
                Result::Ok(facet_field_ids_delta)
            })
        })?;

        for operation in writer_receiver {
            let database = operation.database(index);
            match operation.entry() {
                EntryOperation::Delete(e) => {
                    if !database.delete(wtxn, e.entry())? {
                        unreachable!("We tried to delete an unknown key")
                    }
                }
                EntryOperation::Write(e) => database.put(wtxn, e.key(), e.value())?,
            }
        }

        /// TODO handle the panicking threads
        let facet_field_ids_delta = extractor_handle.join().unwrap()?;

        if let Some(prefix_delta) = compute_word_fst(index, wtxn)? {
            compute_prefix_database(index, wtxn, prefix_delta)?;
        }

        compute_facet_search_database(index, wtxn, global_fields_ids_map)?;

        compute_facet_level_database(index, wtxn, facet_field_ids_delta)?;

        Result::Ok(())
    })?;

    // required to into_inner the new_fields_ids_map
    drop(fields_ids_map_store);

    let fields_ids_map = new_fields_ids_map.into_inner().unwrap();
    index.put_fields_ids_map(wtxn, &fields_ids_map)?;

    if let Some(new_primary_key) = new_primary_key {
        index.put_primary_key(wtxn, new_primary_key.name())?;
    }

    // used to update the localized and weighted maps while sharing the update code with the settings pipeline.
    let mut inner_index_settings = InnerIndexSettings::from_index(index, wtxn)?;
    inner_index_settings.recompute_facets(wtxn, index)?;
    inner_index_settings.recompute_searchables(wtxn, index)?;

    index.set_updated_at(wtxn, &OffsetDateTime::now_utc())?;

    Ok(())
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
fn compute_prefix_database(
    index: &Index,
    wtxn: &mut RwTxn,
    prefix_delta: PrefixDelta,
) -> Result<()> {
    eprintln!("prefix_delta: {:?}", &prefix_delta);
    let PrefixDelta { modified, deleted } = prefix_delta;
    // Compute word prefix docids
    compute_word_prefix_docids(wtxn, index, &modified, &deleted)?;
    // Compute exact word prefix docids
    compute_exact_word_prefix_docids(wtxn, index, &modified, &deleted)?;
    // Compute word prefix fid docids
    compute_word_prefix_fid_docids(wtxn, index, &modified, &deleted)?;
    // Compute word prefix position docids
    compute_word_prefix_position_docids(wtxn, index, &modified, &deleted)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing")]
fn compute_word_fst(index: &Index, wtxn: &mut RwTxn) -> Result<Option<PrefixDelta>> {
    let rtxn = index.read_txn()?;
    let words_fst = index.words_fst(&rtxn)?;
    let mut word_fst_builder = WordFstBuilder::new(&words_fst)?;
    let prefix_settings = index.prefix_settings(&rtxn)?;
    word_fst_builder.with_prefix_settings(prefix_settings);

    let previous_words = index.word_docids.iter(&rtxn)?.remap_data_type::<Bytes>();
    let current_words = index.word_docids.iter(wtxn)?.remap_data_type::<Bytes>();
    for eob in merge_join_by(previous_words, current_words, |lhs, rhs| match (lhs, rhs) {
        (Ok((l, _)), Ok((r, _))) => l.cmp(r),
        (Err(_), _) | (_, Err(_)) => Ordering::Equal,
    }) {
        match eob {
            EitherOrBoth::Both(lhs, rhs) => {
                let (word, lhs_bytes) = lhs?;
                let (_, rhs_bytes) = rhs?;
                if lhs_bytes != rhs_bytes {
                    word_fst_builder.register_word(DelAdd::Addition, word.as_ref())?;
                }
            }
            EitherOrBoth::Left(result) => {
                let (word, _) = result?;
                word_fst_builder.register_word(DelAdd::Deletion, word.as_ref())?;
            }
            EitherOrBoth::Right(result) => {
                let (word, _) = result?;
                word_fst_builder.register_word(DelAdd::Addition, word.as_ref())?;
            }
        }
    }

    let span = tracing::trace_span!(target: "indexing::documents::merge", "words_fst");
    let _entered = span.enter();

    let (word_fst_mmap, prefix_data) = word_fst_builder.build(index, &rtxn)?;
    // extractor_sender.main().write_words_fst(word_fst_mmap).unwrap();
    index.main.remap_types::<Str, Bytes>().put(wtxn, WORDS_FST_KEY, &word_fst_mmap)?;
    if let Some(PrefixData { prefixes_fst_mmap, prefix_delta }) = prefix_data {
        // extractor_sender.main().write_words_prefixes_fst(prefixes_fst_mmap).unwrap();
        index.main.remap_types::<Str, Bytes>().put(
            wtxn,
            WORDS_PREFIXES_FST_KEY,
            &prefixes_fst_mmap,
        )?;
        Ok(Some(prefix_delta))
    } else {
        Ok(None)
    }
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::facet_search")]
fn compute_facet_search_database(
    index: &Index,
    wtxn: &mut RwTxn,
    global_fields_ids_map: GlobalFieldsIdsMap,
) -> Result<()> {
    let rtxn = index.read_txn()?;
    let localized_attributes_rules = index.localized_attributes_rules(&rtxn)?;
    let mut facet_search_builder = FacetSearchBuilder::new(
        global_fields_ids_map,
        localized_attributes_rules.unwrap_or_default(),
    );

    let previous_facet_id_string_docids = index
        .facet_id_string_docids
        .iter(&rtxn)?
        .remap_data_type::<DecodeIgnore>()
        .filter(|r| r.as_ref().map_or(true, |(k, _)| k.level == 0));
    let current_facet_id_string_docids = index
        .facet_id_string_docids
        .iter(wtxn)?
        .remap_data_type::<DecodeIgnore>()
        .filter(|r| r.as_ref().map_or(true, |(k, _)| k.level == 0));
    for eob in merge_join_by(
        previous_facet_id_string_docids,
        current_facet_id_string_docids,
        |lhs, rhs| match (lhs, rhs) {
            (Ok((l, _)), Ok((r, _))) => l.cmp(r),
            (Err(_), _) | (_, Err(_)) => Ordering::Equal,
        },
    ) {
        match eob {
            EitherOrBoth::Both(lhs, rhs) => {
                let (_, _) = lhs?;
                let (_, _) = rhs?;
            }
            EitherOrBoth::Left(result) => {
                let (key, _) = result?;
                facet_search_builder
                    .register_from_key(DelAdd::Deletion, key.left_bound.as_ref())?;
            }
            EitherOrBoth::Right(result) => {
                let (key, _) = result?;
                facet_search_builder
                    .register_from_key(DelAdd::Addition, key.left_bound.as_ref())?;
            }
        }
    }

    facet_search_builder.merge_and_write(index, wtxn, &rtxn)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::facet_field_ids")]
fn compute_facet_level_database(
    index: &Index,
    wtxn: &mut RwTxn,
    facet_field_ids_delta: FacetFieldIdsDelta,
) -> Result<()> {
    eprintln!("facet_field_ids_delta: {:?}", &facet_field_ids_delta);
    if let Some(modified_facet_string_ids) = facet_field_ids_delta.modified_facet_string_ids() {
        let span = tracing::trace_span!(target: "indexing::facet_field_ids", "string");
        let _entered = span.enter();
        FacetsUpdateBulk::new_not_updating_level_0(
            index,
            modified_facet_string_ids,
            FacetType::String,
        )
        .execute(wtxn)?;
    }
    if let Some(modified_facet_number_ids) = facet_field_ids_delta.modified_facet_number_ids() {
        let span = tracing::trace_span!(target: "indexing::facet_field_ids", "number");
        let _entered = span.enter();
        FacetsUpdateBulk::new_not_updating_level_0(
            index,
            modified_facet_number_ids,
            FacetType::Number,
        )
        .execute(wtxn)?;
    }

    Ok(())
}

/// Returns the primary key that has already been set for this index or the
/// one we will guess by searching for the first key that contains "id" as a substring,
/// and whether the primary key changed
/// TODO move this elsewhere
pub fn retrieve_or_guess_primary_key<'a>(
    rtxn: &'a RoTxn<'a>,
    index: &Index,
    new_fields_ids_map: &mut FieldsIdsMap,
    primary_key_from_op: Option<&'a str>,
    first_document: Option<&'a TopLevelMap<'a>>,
) -> Result<StdResult<(PrimaryKey<'a>, bool), UserError>> {
    // make sure that we have a declared primary key, either fetching it from the index or attempting to guess it.

    // do we have an existing declared primary key?
    let (primary_key, has_changed) = if let Some(primary_key_from_db) = index.primary_key(rtxn)? {
        // did we request a primary key in the operation?
        match primary_key_from_op {
            // we did, and it is different from the DB one
            Some(primary_key_from_op) if primary_key_from_op != primary_key_from_db => {
                // is the index empty?
                if index.number_of_documents(rtxn)? == 0 {
                    // change primary key
                    (primary_key_from_op, true)
                } else {
                    return Ok(Err(UserError::PrimaryKeyCannotBeChanged(
                        primary_key_from_db.to_string(),
                    )));
                }
            }
            _ => (primary_key_from_db, false),
        }
    } else {
        // no primary key in the DB => let's set one
        // did we request a primary key in the operation?
        let primary_key = if let Some(primary_key_from_op) = primary_key_from_op {
            // set primary key from operation
            primary_key_from_op
        } else {
            // guess primary key
            let first_document = match first_document {
                Some(document) => document,
                // previous indexer when no pk is set + we send an empty payload => index_primary_key_no_candidate_found
                None => return Ok(Err(UserError::NoPrimaryKeyCandidateFound)),
            };

            let mut guesses: Vec<&str> = first_document
                .keys()
                .map(AsRef::as_ref)
                .filter(|name| name.to_lowercase().ends_with(DEFAULT_PRIMARY_KEY))
                .collect();

            // sort the keys in lexicographical order, so that fields are always in the same order.
            guesses.sort_unstable();

            match guesses.as_slice() {
                [] => return Ok(Err(UserError::NoPrimaryKeyCandidateFound)),
                [name] => {
                    tracing::info!("Primary key was not specified in index. Inferred to '{name}'");
                    *name
                }
                multiple => {
                    return Ok(Err(UserError::MultiplePrimaryKeyCandidatesFound {
                        candidates: multiple
                            .iter()
                            .map(|candidate| candidate.to_string())
                            .collect(),
                    }))
                }
            }
        };
        (primary_key, true)
    };

    match PrimaryKey::new_or_insert(primary_key, new_fields_ids_map) {
        Ok(primary_key) => Ok(Ok((primary_key, has_changed))),
        Err(err) => Ok(Err(err)),
    }
}

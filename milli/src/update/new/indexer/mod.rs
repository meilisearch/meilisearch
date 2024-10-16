use std::cell::RefCell;
use std::sync::RwLock;
use std::thread::{self, Builder};

use big_s::S;
use bumpalo::Bump;
use document_changes::{
    for_each_document_change, DocumentChanges, Extractor, FullySend, IndexingContext, ThreadLocal,
};
pub use document_deletion::DocumentDeletion;
pub use document_operation::DocumentOperation;
use heed::{RoTxn, RwTxn};
pub use partial_dump::PartialDump;
use rayon::ThreadPool;
use time::OffsetDateTime;
pub use update_by_function::UpdateByFunction;

use super::channel::*;
use super::document::write_to_obkv;
use super::document_change::DocumentChange;
use super::extract::*;
use super::merger::{merge_grenad_entries, FacetFieldIdsDelta};
use super::word_fst_builder::PrefixDelta;
use super::words_prefix_docids::{
    compute_word_prefix_docids, compute_word_prefix_fid_docids, compute_word_prefix_position_docids,
};
use super::{StdResult, TopLevelMap};
use crate::documents::{PrimaryKey, DEFAULT_PRIMARY_KEY};
use crate::facet::FacetType;
use crate::proximity::ProximityPrecision;
use crate::update::new::channel::ExtractorSender;
use crate::update::new::words_prefix_docids::compute_exact_word_prefix_docids;
use crate::update::settings::InnerIndexSettings;
use crate::update::{FacetsUpdateBulk, GrenadParameters};
use crate::{Error, FieldsIdsMap, GlobalFieldsIdsMap, Index, Result, UserError};

pub(crate) mod de;
pub mod document_changes;
mod document_deletion;
mod document_operation;
mod partial_dump;
mod update_by_function;

struct DocumentExtractor<'a> {
    document_sender: &'a DocumentSender<'a>,
}

impl<'a, 'extractor> Extractor<'extractor> for DocumentExtractor<'a> {
    type Data = FullySend<()>;

    fn init_data(
        &self,
        _extractor_alloc: raw_collections::alloc::RefBump<'extractor>,
    ) -> Result<Self::Data> {
        Ok(FullySend(()))
    }

    fn process(
        &self,
        change: DocumentChange,
        context: &document_changes::DocumentChangeContext<Self::Data>,
    ) -> Result<()> {
        let mut document_buffer = Vec::new();

        let new_fields_ids_map = context.new_fields_ids_map.borrow();
        let new_fields_ids_map = &*new_fields_ids_map;
        let new_fields_ids_map = new_fields_ids_map.local_map();

        let external_docid = change.external_docid().to_owned();

        // document but we need to create a function that collects and compresses documents.
        match change {
            DocumentChange::Deletion(deletion) => {
                let docid = deletion.docid();
                self.document_sender.delete(docid, external_docid).unwrap();
            }
            /// TODO: change NONE by SOME(vector) when implemented
            DocumentChange::Update(update) => {
                let docid = update.docid();
                let content =
                    update.new(&context.txn, context.index, &context.db_fields_ids_map)?;
                let content =
                    write_to_obkv(&content, None, new_fields_ids_map, &mut document_buffer)?;
                self.document_sender.insert(docid, external_docid, content.boxed()).unwrap();
            }
            DocumentChange::Insertion(insertion) => {
                let docid = insertion.docid();
                let content = insertion.new();
                let content =
                    write_to_obkv(&content, None, new_fields_ids_map, &mut document_buffer)?;
                self.document_sender.insert(docid, external_docid, content.boxed()).unwrap();
                // extracted_dictionary_sender.send(self, dictionary: &[u8]);
            }
        }
        Ok(())
    }
}

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
    let (merger_sender, writer_receiver) = merger_writer_channel(10_000);
    // This channel acts as a rendezvous point to ensure that we are one task ahead
    let (extractor_sender, merger_receiver) = extractors_merger_channels(4);

    let new_fields_ids_map = RwLock::new(new_fields_ids_map);

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

    thread::scope(|s| {
        let indexer_span = tracing::Span::current();
        // TODO manage the errors correctly
        let handle = Builder::new().name(S("indexer-extractors")).spawn_scoped(s, move || {
            pool.in_place_scope(|_s| {
                    let span = tracing::trace_span!(target: "indexing::documents", parent: &indexer_span, "extract");
                    let _entered = span.enter();

                    // document but we need to create a function that collects and compresses documents.
                    let document_sender = extractor_sender.document_sender();
                    let document_extractor = DocumentExtractor { document_sender: &document_sender};
                    let datastore = ThreadLocal::with_capacity(pool.current_num_threads());
                    for_each_document_change(document_changes, &document_extractor, indexing_context, &mut extractor_allocs, &datastore)?;

                    document_sender.finish().unwrap();

                    const TEN_GIB: usize = 10 * 1024 * 1024 * 1024;
                    let max_memory = TEN_GIB / dbg!(rayon::current_num_threads());
                    let grenad_parameters = GrenadParameters {
                        max_memory: Some(max_memory),
                        ..GrenadParameters::default()
                    };

                    {
                        let span = tracing::trace_span!(target: "indexing::documents::extract", "faceted");
                        let _entered = span.enter();
                        extract_and_send_docids::<
                            _,
                            FacetedDocidsExtractor,
                            FacetDocids,
                        >(
                            grenad_parameters,
                            document_changes,
                            indexing_context,
                            &mut extractor_allocs,
                            &extractor_sender,
                        )?;
                    }

                    {
                        let span = tracing::trace_span!(target: "indexing::documents::extract", "word_docids");
                        let _entered = span.enter();

                        let WordDocidsMergers {
                            word_fid_docids,
                            word_docids,
                            exact_word_docids,
                            word_position_docids,
                            fid_word_count_docids,
                        } = WordDocidsExtractors::run_extraction(grenad_parameters, document_changes, indexing_context, &mut extractor_allocs)?;
                        extractor_sender.send_searchable::<WordDocids>(word_docids).unwrap();
                        extractor_sender.send_searchable::<WordFidDocids>(word_fid_docids).unwrap();
                        extractor_sender.send_searchable::<ExactWordDocids>(exact_word_docids).unwrap();
                        extractor_sender.send_searchable::<WordPositionDocids>(word_position_docids).unwrap();
                        extractor_sender.send_searchable::<FidWordCountDocids>(fid_word_count_docids).unwrap();
                    }

                    // run the proximity extraction only if the precision is by word
                    // this works only if the settings didn't change during this transaction.
                    let rtxn = index.read_txn().unwrap();
                    let proximity_precision = index.proximity_precision(&rtxn)?.unwrap_or_default();
                    if proximity_precision == ProximityPrecision::ByWord {
                        let span = tracing::trace_span!(target: "indexing::documents::extract", "word_pair_proximity_docids");
                        let _entered = span.enter();
                        extract_and_send_docids::<
                            _,
                            WordPairProximityDocidsExtractor,
                            WordPairProximityDocids,
                        >(
                            grenad_parameters,
                            document_changes,
                            indexing_context,
                      &mut extractor_allocs,
                            &extractor_sender,
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

                    Ok(()) as Result<_>
                })
        })?;

        let global_fields_ids_map = GlobalFieldsIdsMap::new(&new_fields_ids_map);

        let indexer_span = tracing::Span::current();
        // TODO manage the errors correctly
        let merger_thread = Builder::new().name(S("indexer-merger")).spawn_scoped(s, move || {
            let span =
                tracing::trace_span!(target: "indexing::documents", parent: &indexer_span, "merge");
            let _entered = span.enter();
            let rtxn = index.read_txn().unwrap();
            merge_grenad_entries(
                merger_receiver,
                merger_sender,
                &rtxn,
                index,
                global_fields_ids_map,
            )
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
        handle.join().unwrap()?;
        let merger_result = merger_thread.join().unwrap()?;

        if let Some(facet_field_ids_delta) = merger_result.facet_field_ids_delta {
            compute_facet_level_database(index, wtxn, facet_field_ids_delta)?;
        }

        if let Some(prefix_delta) = merger_result.prefix_delta {
            compute_prefix_database(index, wtxn, prefix_delta)?;
        }

        Ok(()) as Result<_>
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

/// TODO: GrenadParameters::default() should be removed in favor a passed parameter
/// TODO: manage the errors correctly
/// TODO: we must have a single trait that also gives the extractor type
fn extract_and_send_docids<
    'pl,
    'fid,
    'indexer,
    'index,
    DC: DocumentChanges<'pl>,
    E: DocidsExtractor,
    D: MergerOperationType,
>(
    grenad_parameters: GrenadParameters,
    document_changes: &DC,
    indexing_context: IndexingContext<'fid, 'indexer, 'index>,
    extractor_allocs: &mut ThreadLocal<FullySend<RefCell<Bump>>>,
    sender: &ExtractorSender,
) -> Result<()> {
    let merger =
        E::run_extraction(grenad_parameters, document_changes, indexing_context, extractor_allocs)?;
    sender.send_searchable::<D>(merger).unwrap();
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

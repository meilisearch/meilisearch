use std::fs::File;
use std::sync::RwLock;
use std::thread::{self, Builder};

use big_s::S;
pub use document_deletion::DocumentDeletion;
pub use document_operation::DocumentOperation;
use heed::{RoTxn, RwTxn};
pub use partial_dump::PartialDump;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::ThreadPool;
pub use update_by_function::UpdateByFunction;

use super::channel::{
    extractors_merger_channels, merger_writer_channel, EntryOperation, ExactWordDocids, WordDocids,
    WordFidDocids, WordPositionDocids,
};
use super::document_change::DocumentChange;
use super::extract::{
    ExactWordDocidsExtractor, SearchableExtractor, WordDocidsExtractor, WordFidDocidsExtractor,
    WordPositionDocidsExtractor,
};
use super::merger::merge_grenad_entries;
use super::StdResult;
use crate::documents::{
    obkv_to_object, DocumentsBatchCursor, DocumentsBatchIndex, PrimaryKey, DEFAULT_PRIMARY_KEY,
};
use crate::update::new::channel::{DatabaseType, ExtractorSender};
use crate::update::GrenadParameters;
use crate::{FieldsIdsMap, GlobalFieldsIdsMap, Index, Result, UserError};

mod document_deletion;
mod document_operation;
mod partial_dump;
mod update_by_function;

pub trait DocumentChanges<'p> {
    type Parameter: 'p;

    fn document_changes(
        self,
        fields_ids_map: &mut FieldsIdsMap,
        param: Self::Parameter,
    ) -> Result<impl ParallelIterator<Item = Result<DocumentChange>> + Clone + 'p>;
}

/// This is the main function of this crate.
///
/// Give it the output of the [`Indexer::document_changes`] method and it will execute it in the [`rayon::ThreadPool`].
///
/// TODO return stats
pub fn index<PI>(
    wtxn: &mut RwTxn,
    index: &Index,
    fields_ids_map: FieldsIdsMap,
    pool: &ThreadPool,
    document_changes: PI,
) -> Result<()>
where
    PI: IntoParallelIterator<Item = Result<DocumentChange>> + Send,
    PI::Iter: Clone,
{
    let (merger_sender, writer_receiver) = merger_writer_channel(100);
    // This channel acts as a rendezvous point to ensure that we are one task ahead
    let (extractor_sender, merger_receiver) = extractors_merger_channels(0);

    let fields_ids_map_lock = RwLock::new(fields_ids_map);
    let global_fields_ids_map = GlobalFieldsIdsMap::new(&fields_ids_map_lock);

    thread::scope(|s| {
        // TODO manage the errors correctly
        let handle = Builder::new().name(S("indexer-extractors")).spawn_scoped(s, move || {
            pool.in_place_scope(|_s| {
                let document_changes = document_changes.into_par_iter();

                // document but we need to create a function that collects and compresses documents.
                document_changes.clone().into_par_iter().try_for_each(|result| {
                    match result? {
                        DocumentChange::Deletion(deletion) => {
                            let docid = deletion.docid();
                            extractor_sender.document_delete(docid).unwrap();
                        }
                        DocumentChange::Update(update) => {
                            let docid = update.docid();
                            let content = update.new();
                            extractor_sender.document_insert(docid, content.boxed()).unwrap();
                        }
                        DocumentChange::Insertion(insertion) => {
                            let docid = insertion.docid();
                            let content = insertion.new();
                            extractor_sender.document_insert(docid, content.boxed()).unwrap();
                            // extracted_dictionary_sender.send(self, dictionary: &[u8]);
                        }
                    }
                    Ok(()) as Result<_>
                })?;

                extract_and_send_docids::<WordDocidsExtractor, WordDocids>(
                    index,
                    &global_fields_ids_map,
                    GrenadParameters::default(),
                    document_changes.clone(),
                    &extractor_sender,
                )?;

                extract_and_send_docids::<WordFidDocidsExtractor, WordFidDocids>(
                    index,
                    &global_fields_ids_map,
                    GrenadParameters::default(),
                    document_changes.clone(),
                    &extractor_sender,
                )?;

                extract_and_send_docids::<ExactWordDocidsExtractor, ExactWordDocids>(
                    index,
                    &global_fields_ids_map,
                    GrenadParameters::default(),
                    document_changes.clone(),
                    &extractor_sender,
                )?;

                extract_and_send_docids::<WordPositionDocidsExtractor, WordPositionDocids>(
                    index,
                    &global_fields_ids_map,
                    GrenadParameters::default(),
                    document_changes.clone(),
                    &extractor_sender,
                )?;

                // TODO THIS IS TOO MUCH
                // Extract fieldid docid facet number
                // Extract fieldid docid facet string
                // Extract facetid string fst
                // Extract facetid normalized string strings

                // TODO Inverted Indexes again
                // Extract fieldid facet isempty docids
                // Extract fieldid facet isnull docids
                // Extract fieldid facet exists docids

                // TODO This is the normal system
                // Extract fieldid facet number docids
                // Extract fieldid facet string docids

                Ok(()) as Result<_>
            })
        })?;

        // TODO manage the errors correctly
        let handle2 = Builder::new().name(S("indexer-merger")).spawn_scoped(s, move || {
            let rtxn = index.read_txn().unwrap();
            merge_grenad_entries(merger_receiver, merger_sender, &rtxn, index)
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
        handle2.join().unwrap()?;

        Ok(()) as Result<_>
    })?;

    let fields_ids_map = fields_ids_map_lock.into_inner().unwrap();
    index.put_fields_ids_map(wtxn, &fields_ids_map)?;

    Ok(())
}

/// TODO: GrenadParameters::default() should be removed in favor a passed parameter
/// TODO: manage the errors correctly
/// TODO: we must have a single trait that also gives the extractor type
fn extract_and_send_docids<E: SearchableExtractor, D: DatabaseType>(
    index: &Index,
    fields_ids_map: &GlobalFieldsIdsMap,
    indexer: GrenadParameters,
    document_changes: impl IntoParallelIterator<Item = Result<DocumentChange>>,
    sender: &ExtractorSender,
) -> Result<()> {
    let merger = E::run_extraction(index, fields_ids_map, indexer, document_changes)?;
    Ok(sender.send_searchable::<D>(merger).unwrap())
}

/// TODO move this elsewhere
pub fn guess_primary_key<'a>(
    rtxn: &'a RoTxn<'a>,
    index: &Index,
    mut cursor: DocumentsBatchCursor<File>,
    documents_batch_index: &'a DocumentsBatchIndex,
) -> Result<StdResult<PrimaryKey<'a>, UserError>> {
    // The primary key *field id* that has already been set for this index or the one
    // we will guess by searching for the first key that contains "id" as a substring.
    match index.primary_key(rtxn)? {
        Some(primary_key) => match PrimaryKey::new(primary_key, documents_batch_index) {
            Some(primary_key) => Ok(Ok(primary_key)),
            None => match cursor.next_document()? {
                Some(first_document) => Ok(Err(UserError::MissingDocumentId {
                    primary_key: primary_key.to_string(),
                    document: obkv_to_object(first_document, documents_batch_index)?,
                })),
                None => unreachable!("Called with reader.is_empty()"),
            },
        },
        None => {
            let mut guesses: Vec<(u16, &str)> = documents_batch_index
                .iter()
                .filter(|(_, name)| name.to_lowercase().ends_with(DEFAULT_PRIMARY_KEY))
                .map(|(field_id, name)| (*field_id, name.as_str()))
                .collect();

            // sort the keys in a deterministic, obvious way, so that fields are always in the same order.
            guesses.sort_by(|(_, left_name), (_, right_name)| {
                // shortest name first
                left_name.len().cmp(&right_name.len()).then_with(
                    // then alphabetical order
                    || left_name.cmp(right_name),
                )
            });

            match guesses.as_slice() {
                [] => Ok(Err(UserError::NoPrimaryKeyCandidateFound)),
                [(field_id, name)] => {
                    tracing::info!("Primary key was not specified in index. Inferred to '{name}'");
                    Ok(Ok(PrimaryKey::Flat { name, field_id: *field_id }))
                }
                multiple => Ok(Err(UserError::MultiplePrimaryKeyCandidatesFound {
                    candidates: multiple
                        .iter()
                        .map(|(_, candidate)| candidate.to_string())
                        .collect(),
                })),
            }
        }
    }
}

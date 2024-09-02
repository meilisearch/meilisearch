use std::fs::File;
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
    extractors_merger_channels, merger_writer_channel, EntryOperation, ExtractorsMergerChannels,
    WriterOperation,
};
use super::document_change::DocumentChange;
use super::merger::merge_grenad_entries;
use super::StdResult;
use crate::documents::{
    obkv_to_object, DocumentsBatchCursor, DocumentsBatchIndex, PrimaryKey, DEFAULT_PRIMARY_KEY,
};
use crate::{Index, Result, UserError};

mod document_deletion;
mod document_operation;
mod partial_dump;
mod update_by_function;

pub trait DocumentChanges<'p> {
    type Parameter: 'p;

    fn document_changes(
        self,
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
    pool: &ThreadPool,
    _document_changes: PI,
) -> Result<()>
where
    PI: IntoParallelIterator<Item = Result<DocumentChange>> + Send,
    PI::Iter: Clone,
{
    let (merger_sender, writer_receiver) = merger_writer_channel(100);
    let ExtractorsMergerChannels { merger_receiver, deladd_cbo_roaring_bitmap_sender } =
        extractors_merger_channels(100);

    thread::scope(|s| {
        // TODO manage the errors correctly
        let handle = Builder::new().name(S("indexer-extractors")).spawn_scoped(s, || {
            pool.in_place_scope(|_s| {
                // word docids
                // document_changes.into_par_iter().try_for_each(|_dc| Ok(()) as Result<_>)
                // let grenads = extractor_function(document_changes)?;
                // deladd_cbo_roaring_bitmap_sender.word_docids(grenads)?;

                Ok(()) as Result<_>
            })
        })?;

        // TODO manage the errors correctly
        let handle2 = Builder::new().name(S("indexer-merger")).spawn_scoped(s, || {
            let rtxn = index.read_txn().unwrap();
            merge_grenad_entries(merger_receiver, merger_sender, &rtxn, index)
        })?;

        // TODO Split this code into another function
        for operation in writer_receiver {
            let database = operation.database(index);
            match operation {
                WriterOperation::WordDocids(operation) => match operation {
                    EntryOperation::Delete(e) => database.delete(wtxn, e.entry()).map(drop)?,
                    EntryOperation::Write(e) => database.put(wtxn, e.key(), e.value())?,
                },
                WriterOperation::Document(e) => database.put(wtxn, &e.key(), e.content())?,
            }
        }

        /// TODO handle the panicking threads
        handle.join().unwrap()?;
        handle2.join().unwrap()?;

        Ok(())
    })
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

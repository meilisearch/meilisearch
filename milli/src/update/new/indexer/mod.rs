use std::thread;

use big_s::S;
pub use document_deletion::DocumentDeletionIndexer;
pub use document_operation::DocumentOperationIndexer;
use heed::RwTxn;
pub use partial_dump::PartialDumpIndexer;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::ThreadPool;
pub use update_by_function::UpdateByFunctionIndexer;

use super::channel::{
    extractors_merger_channels, merger_writer_channels, EntryOperation, ExtractorsMergerChannels,
    WriterOperation,
};
use super::document_change::DocumentChange;
use super::merger::merge_grenad_entries;
use crate::{Index, Result};

mod document_deletion;
mod document_operation;
mod partial_dump;
mod update_by_function;

pub trait Indexer<'p> {
    type Parameter: 'p;

    fn document_changes(
        self,
        param: Self::Parameter,
    ) -> Result<impl ParallelIterator<Item = Result<Option<DocumentChange>>> + 'p>;
}

/// This is the main function of this crate.
///
/// Give it the output of the [`Indexer::document_changes`] method and it will execute it in the [`rayon::ThreadPool`].
///
/// TODO return stats
/// TODO take the rayon ThreadPool
pub fn index<PI>(
    wtxn: &mut RwTxn,
    index: &Index,
    pool: &ThreadPool,
    document_changes: PI,
) -> Result<()>
where
    PI: IntoParallelIterator<Item = Result<DocumentChange>> + Send,
    PI::Iter: Clone,
{
    let (merger_sender, writer_receiver) = merger_writer_channels(100);
    let ExtractorsMergerChannels { merger_receiver, deladd_cbo_roaring_bitmap_sender } =
        extractors_merger_channels(100);

    thread::scope(|s| {
        // TODO manage the errors correctly
        thread::Builder::new().name(S("indexer-extractors")).spawn_scoped(s, || {
            pool.in_place_scope(|_s| {
                document_changes.into_par_iter().for_each(|_dc| ());
            })
        })?;

        // TODO manage the errors correctly
        thread::Builder::new().name(S("indexer-merger")).spawn_scoped(s, || {
            let rtxn = index.read_txn().unwrap();
            merge_grenad_entries(merger_receiver, merger_sender, &rtxn, index).unwrap()
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

        Ok(())
    })
}

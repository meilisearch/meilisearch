use std::thread;

use big_s::S;
pub use document_deletion::DocumentDeletion;
pub use document_operation::DocumentOperation;
use heed::RwTxn;
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
use crate::{Index, Result};

mod document_deletion;
mod document_operation;
mod partial_dump;
mod update_by_function;

pub trait DocumentChanges<'p> {
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
pub fn index<PI>(
    wtxn: &mut RwTxn,
    index: &Index,
    pool: &ThreadPool,
    document_changes: PI,
) -> Result<()>
where
    PI: IntoParallelIterator<Item = Result<Option<DocumentChange>>> + Send,
    PI::Iter: Clone,
{
    let (merger_sender, writer_receiver) = merger_writer_channel(100);
    let ExtractorsMergerChannels { merger_receiver, deladd_cbo_roaring_bitmap_sender } =
        extractors_merger_channels(100);

    thread::scope(|s| {
        // TODO manage the errors correctly
        let handle =
            thread::Builder::new().name(S("indexer-extractors")).spawn_scoped(s, || {
                pool.in_place_scope(|_s| {
                    // word docids
                    // document_changes.into_par_iter().try_for_each(|_dc| Ok(()) as Result<_>)
                    // let grenads = extractor_function(document_changes)?;
                    // deladd_cbo_roaring_bitmap_sender.word_docids(grenads)?;

                    Ok(()) as Result<_>
                })
            })?;

        // TODO manage the errors correctly
        let handle2 = thread::Builder::new().name(S("indexer-merger")).spawn_scoped(s, || {
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

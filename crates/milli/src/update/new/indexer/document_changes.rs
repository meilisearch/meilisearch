use std::cell::{Cell, RefCell};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use bumpalo::Bump;
use rayon::iter::IndexedParallelIterator;

use super::super::document_change::DocumentChange;
use crate::fields_ids_map::metadata::FieldIdMapWithMetadata;
use crate::progress::{AtomicDocumentStep, Progress};
use crate::update::new::document::DocumentContext;
use crate::update::new::parallel_iterator_ext::ParallelIteratorExt as _;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::{FullySend, MostlySend, ThreadLocal};
use crate::update::GrenadParameters;
use crate::{FieldsIdsMap, GlobalFieldsIdsMap, Index, InternalError, Result};

/// An internal iterator (i.e. using `foreach`) of `DocumentChange`s
pub trait Extractor<'extractor>: Sync {
    type Data: MostlySend;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> Result<Self::Data>;

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &'doc DocumentContext<'doc, 'extractor, '_, '_, Self::Data>,
    ) -> Result<()>;
}

pub trait DocumentChanges<'pl // lifetime of the underlying payload
>: Sync {
    type Item: Send;

    fn iter(&self, chunk_size: usize) -> impl IndexedParallelIterator<Item = impl AsRef<[Self::Item]>>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn item_to_document_change<'doc, // lifetime of a single `process` call
     T: MostlySend>(
        &'doc self,
        context: &'doc DocumentContext<T>,
        item: &'doc Self::Item,
    ) -> Result<Option<DocumentChange<'doc>>> where 'pl: 'doc // the payload must survive the process calls
    ;
}

pub struct IndexingContext<
    'fid,     // invariant lifetime of fields ids map
    'indexer, // covariant lifetime of objects that are borrowed  during the entire indexing operation
    'index,   // covariant lifetime of the index
    MSP,
> where
    MSP: Fn() -> bool + Sync,
{
    pub index: &'index Index,
    pub db_fields_ids_map: &'indexer FieldsIdsMap,
    pub new_fields_ids_map: &'fid RwLock<FieldIdMapWithMetadata>,
    pub doc_allocs: &'indexer ThreadLocal<FullySend<Cell<Bump>>>,
    pub fields_ids_map_store: &'indexer ThreadLocal<FullySend<RefCell<GlobalFieldsIdsMap<'fid>>>>,
    pub must_stop_processing: &'indexer MSP,
    pub progress: &'indexer Progress,
    pub grenad_parameters: &'indexer GrenadParameters,
}

impl<MSP> Copy
    for IndexingContext<
        '_, // invariant lifetime of fields ids map
        '_, // covariant lifetime of objects that are borrowed  during the entire indexing operation
        '_, // covariant lifetime of the index
        MSP,
    >
where
    MSP: Fn() -> bool + Sync,
{
}

impl<MSP> Clone
    for IndexingContext<
        '_, // invariant lifetime of fields ids map
        '_, // covariant lifetime of objects that are borrowed  during the entire indexing operation
        '_, // covariant lifetime of the index
        MSP,
    >
where
    MSP: Fn() -> bool + Sync,
{
    fn clone(&self) -> Self {
        *self
    }
}

const CHUNK_SIZE: usize = 100;

pub fn extract<
    'pl,        // covariant lifetime of the underlying payload
    'extractor, // invariant lifetime of extractor_alloc
    'fid,       // invariant lifetime of fields ids map
    'indexer,   // covariant lifetime of objects that are borrowed during the entire indexing
    'data,      // invariant on EX::Data lifetime of datastore
    'index,     // covariant lifetime of the index
    EX,
    DC: DocumentChanges<'pl>,
    MSP,
>(
    document_changes: &DC,
    extractor: &EX,
    IndexingContext {
        index,
        db_fields_ids_map,
        new_fields_ids_map,
        doc_allocs,
        fields_ids_map_store,
        must_stop_processing,
        progress,
        grenad_parameters: _,
    }: IndexingContext<'fid, 'indexer, 'index, MSP>,
    extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
    datastore: &'data ThreadLocal<EX::Data>,
    step: IndexingStep,
) -> Result<()>
where
    EX: Extractor<'extractor>,
    MSP: Fn() -> bool + Sync,
{
    tracing::trace!("We are resetting the extractor allocators");
    progress.update_progress(step);
    // Clean up and reuse the extractor allocs
    for extractor_alloc in extractor_allocs.iter_mut() {
        tracing::trace!("\tWith {} bytes reset", extractor_alloc.0.allocated_bytes());
        extractor_alloc.0.reset();
    }

    let total_documents = document_changes.len() as u32;
    let (step, progress_step) = AtomicDocumentStep::new(total_documents);
    progress.update_progress(progress_step);

    let pi = document_changes.iter(CHUNK_SIZE);
    pi.try_arc_for_each_try_init(
        || {
            DocumentContext::new(
                index,
                db_fields_ids_map,
                new_fields_ids_map,
                extractor_allocs,
                doc_allocs,
                datastore,
                fields_ids_map_store,
                move |index_alloc| extractor.init_data(index_alloc),
            )
        },
        |context, items| {
            if (must_stop_processing)() {
                return Err(Arc::new(InternalError::AbortedIndexation.into()));
            }

            // Clean up and reuse the document-specific allocator
            context.doc_alloc.reset();

            let items = items.as_ref();
            let changes = items.iter().filter_map(|item| {
                document_changes.item_to_document_change(context, item).transpose()
            });

            let res = extractor.process(changes, context).map_err(Arc::new);
            step.fetch_add(items.as_ref().len() as u32, Ordering::Relaxed);

            // send back the doc_alloc in the pool
            context.doc_allocs.get_or_default().0.set(std::mem::take(&mut context.doc_alloc));

            res
        },
    )?;
    step.store(total_documents, Ordering::Relaxed);

    Ok(())
}

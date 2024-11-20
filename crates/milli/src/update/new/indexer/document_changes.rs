use std::cell::{Cell, RefCell};
use std::sync::{Arc, RwLock};

use bumpalo::Bump;
use heed::RoTxn;
use rayon::iter::IndexedParallelIterator;

use super::super::document_change::DocumentChange;
use crate::fields_ids_map::metadata::FieldIdMapWithMetadata;
use crate::update::new::parallel_iterator_ext::ParallelIteratorExt as _;
use crate::update::new::steps::Step;
use crate::update::new::thread_local::{FullySend, MostlySend, ThreadLocal};
use crate::{FieldsIdsMap, GlobalFieldsIdsMap, Index, InternalError, Result};

pub struct DocumentChangeContext<
    'doc,             // covariant lifetime of a single `process` call
    'extractor: 'doc, // invariant lifetime of the extractor_allocs
    'fid: 'doc,       // invariant lifetime of the new_fields_ids_map
    'indexer: 'doc,   // covariant lifetime of objects that outlive a single `process` call
    T: MostlySend,
> {
    /// The index we're indexing in
    pub index: &'indexer Index,
    /// The fields ids map as it was at the start of this indexing process. Contains at least all top-level fields from documents
    /// inside of the DB.
    pub db_fields_ids_map: &'indexer FieldsIdsMap,
    /// A transaction providing data from the DB before all indexing operations
    pub rtxn: RoTxn<'indexer>,

    /// Global field id map that is up to date with the current state of the indexing process.
    ///
    /// - Inserting a field will take a lock
    /// - Retrieving a field may take a lock as well
    pub new_fields_ids_map: &'doc std::cell::RefCell<GlobalFieldsIdsMap<'fid>>,

    /// Data allocated in this allocator is cleared between each call to `process`.
    pub doc_alloc: Bump,

    /// Data allocated in this allocator is not cleared between each call to `process`, unless the data spills.
    pub extractor_alloc: &'extractor Bump,

    /// Pool of doc allocators, used to retrieve the doc allocator we provided for the documents
    doc_allocs: &'doc ThreadLocal<FullySend<Cell<Bump>>>,

    /// Extractor-specific data
    pub data: &'doc T,
}

impl<
        'doc,             // covariant lifetime of a single `process` call
        'data: 'doc,      // invariant on T lifetime of the datastore
        'extractor: 'doc, // invariant lifetime of extractor_allocs
        'fid: 'doc,       // invariant lifetime of fields ids map
        'indexer: 'doc,   // covariant lifetime of objects that survive a `process` call
        T: MostlySend,
    > DocumentChangeContext<'doc, 'extractor, 'fid, 'indexer, T>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new<F>(
        index: &'indexer Index,
        db_fields_ids_map: &'indexer FieldsIdsMap,
        new_fields_ids_map: &'fid RwLock<FieldIdMapWithMetadata>,
        extractor_allocs: &'extractor ThreadLocal<FullySend<Bump>>,
        doc_allocs: &'doc ThreadLocal<FullySend<Cell<Bump>>>,
        datastore: &'data ThreadLocal<T>,
        fields_ids_map_store: &'doc ThreadLocal<FullySend<RefCell<GlobalFieldsIdsMap<'fid>>>>,
        init_data: F,
    ) -> Result<Self>
    where
        F: FnOnce(&'extractor Bump) -> Result<T>,
    {
        let doc_alloc =
            doc_allocs.get_or(|| FullySend(Cell::new(Bump::with_capacity(1024 * 1024 * 1024))));
        let doc_alloc = doc_alloc.0.take();
        let fields_ids_map = fields_ids_map_store
            .get_or(|| RefCell::new(GlobalFieldsIdsMap::new(new_fields_ids_map)).into());

        let fields_ids_map = &fields_ids_map.0;
        let extractor_alloc = extractor_allocs.get_or_default();

        let data = datastore.get_or_try(move || init_data(&extractor_alloc.0))?;

        let txn = index.read_txn()?;
        Ok(DocumentChangeContext {
            index,
            rtxn: txn,
            db_fields_ids_map,
            new_fields_ids_map: fields_ids_map,
            doc_alloc,
            extractor_alloc: &extractor_alloc.0,
            data,
            doc_allocs,
        })
    }
}

/// An internal iterator (i.e. using `foreach`) of `DocumentChange`s
pub trait Extractor<'extractor>: Sync {
    type Data: MostlySend;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> Result<Self::Data>;

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &'doc DocumentChangeContext<Self::Data>,
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
        context: &'doc DocumentChangeContext<T>,
        item: &'doc Self::Item,
    ) -> Result<Option<DocumentChange<'doc>>> where 'pl: 'doc // the payload must survive the process calls
    ;
}

pub struct IndexingContext<
    'fid,     // invariant lifetime of fields ids map
    'indexer, // covariant lifetime of objects that are borrowed  during the entire indexing operation
    'index,   // covariant lifetime of the index
    MSP,
    SP,
> where
    MSP: Fn() -> bool + Sync,
    SP: Fn(Progress) + Sync,
{
    pub index: &'index Index,
    pub db_fields_ids_map: &'indexer FieldsIdsMap,
    pub new_fields_ids_map: &'fid RwLock<FieldIdMapWithMetadata>,
    pub doc_allocs: &'indexer ThreadLocal<FullySend<Cell<Bump>>>,
    pub fields_ids_map_store: &'indexer ThreadLocal<FullySend<RefCell<GlobalFieldsIdsMap<'fid>>>>,
    pub must_stop_processing: &'indexer MSP,
    pub send_progress: &'indexer SP,
}

impl<
        'fid,     // invariant lifetime of fields ids map
        'indexer, // covariant lifetime of objects that are borrowed  during the entire indexing operation
        'index,   // covariant lifetime of the index
        MSP,
        SP,
    > Copy
    for IndexingContext<
        'fid,     // invariant lifetime of fields ids map
        'indexer, // covariant lifetime of objects that are borrowed  during the entire indexing operation
        'index,   // covariant lifetime of the index
        MSP,
        SP,
    >
where
    MSP: Fn() -> bool + Sync,
    SP: Fn(Progress) + Sync,
{
}

impl<
        'fid,     // invariant lifetime of fields ids map
        'indexer, // covariant lifetime of objects that are borrowed  during the entire indexing operation
        'index,   // covariant lifetime of the index
        MSP,
        SP,
    > Clone
    for IndexingContext<
        'fid,     // invariant lifetime of fields ids map
        'indexer, // covariant lifetime of objects that are borrowed  during the entire indexing operation
        'index,   // covariant lifetime of the index
        MSP,
        SP,
    >
where
    MSP: Fn() -> bool + Sync,
    SP: Fn(Progress) + Sync,
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
    SP,
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
        send_progress,
    }: IndexingContext<'fid, 'indexer, 'index, MSP, SP>,
    extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
    datastore: &'data ThreadLocal<EX::Data>,
    step: Step,
) -> Result<()>
where
    EX: Extractor<'extractor>,
    MSP: Fn() -> bool + Sync,
    SP: Fn(Progress) + Sync,
{
    eprintln!("We are resetting the extractor allocators");
    // Clean up and reuse the extractor allocs
    for extractor_alloc in extractor_allocs.iter_mut() {
        eprintln!("\tWith {} bytes resetted", extractor_alloc.0.allocated_bytes());
        extractor_alloc.0.reset();
    }

    let total_documents = document_changes.len() as u32;

    let pi = document_changes.iter(CHUNK_SIZE);
    pi.enumerate().try_arc_for_each_try_init(
        || {
            DocumentChangeContext::new(
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
        |context, (finished_documents, items)| {
            if (must_stop_processing)() {
                return Err(Arc::new(InternalError::AbortedIndexation.into()));
            }
            let finished_documents = (finished_documents * CHUNK_SIZE) as u32;

            (send_progress)(Progress::from_step_substep(step, finished_documents, total_documents));

            // Clean up and reuse the document-specific allocator
            context.doc_alloc.reset();

            let items = items.as_ref();
            let changes = items.iter().filter_map(|item| {
                document_changes.item_to_document_change(context, item).transpose()
            });

            let res = extractor.process(changes, context).map_err(Arc::new);

            // send back the doc_alloc in the pool
            context.doc_allocs.get_or_default().0.set(std::mem::take(&mut context.doc_alloc));

            res
        },
    )?;

    (send_progress)(Progress::from_step_substep(step, total_documents, total_documents));

    Ok(())
}

pub struct Progress {
    pub finished_steps: u16,
    pub total_steps: u16,
    pub step_name: &'static str,
    pub finished_total_substep: Option<(u32, u32)>,
}

impl Progress {
    pub fn from_step(step: Step) -> Self {
        Self {
            finished_steps: step.finished_steps(),
            total_steps: Step::total_steps(),
            step_name: step.name(),
            finished_total_substep: None,
        }
    }
    pub fn from_step_substep(step: Step, finished_substep: u32, total_substep: u32) -> Self {
        Self {
            finished_total_substep: Some((finished_substep, total_substep)),
            ..Progress::from_step(step)
        }
    }
}

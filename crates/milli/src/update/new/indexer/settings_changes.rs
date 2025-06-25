use std::sync::atomic::Ordering;
use std::sync::Arc;

use bumpalo::Bump;
use rayon::iter::IndexedParallelIterator;
use rayon::slice::ParallelSlice;

use super::document_changes::IndexingContext;
use crate::documents::PrimaryKey;
use crate::progress::AtomicDocumentStep;
use crate::update::new::document_change::DatabaseDocument;
use crate::update::new::indexer::document_changes::DocumentChangeContext;
use crate::update::new::parallel_iterator_ext::ParallelIteratorExt as _;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::{FullySend, MostlySend, ThreadLocal};
use crate::{DocumentId, InternalError, Result};

/// An internal iterator (i.e. using `foreach`) of `DocumentChange`s
pub trait SettingsChangeExtractor<'extractor>: Sync {
    type Data: MostlySend;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> Result<Self::Data>;

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = Result<DatabaseDocument<'doc>>>,
        context: &'doc DocumentChangeContext<Self::Data>,
    ) -> Result<()>;
}
pub struct DatabaseDocuments<'indexer> {
    documents: &'indexer [DocumentId],
    primary_key: PrimaryKey<'indexer>,
}

impl<'indexer> DatabaseDocuments<'indexer> {
    pub fn new(documents: &'indexer [DocumentId], primary_key: PrimaryKey<'indexer>) -> Self {
        Self { documents, primary_key }
    }

    fn iter(&self, chunk_size: usize) -> impl IndexedParallelIterator<Item = &[DocumentId]> {
        self.documents.par_chunks(chunk_size)
    }

    fn item_to_database_document<
        'doc, // lifetime of a single `process` call
        T: MostlySend,
    >(
        &'doc self,
        context: &'doc DocumentChangeContext<T>,
        docid: &'doc DocumentId,
    ) -> Result<Option<DatabaseDocument<'doc>>> {
        let current = context.index.document(&context.rtxn, *docid)?;

        let external_document_id = self.primary_key.extract_docid_from_db(
            current,
            &context.db_fields_ids_map,
            &context.doc_alloc,
        )?;

        let external_document_id = external_document_id.to_bump(&context.doc_alloc);

        Ok(Some(DatabaseDocument::create(*docid, external_document_id)))
    }

    fn len(&self) -> usize {
        self.documents.len()
    }
}

const CHUNK_SIZE: usize = 100;

pub fn settings_change_extract<
    'extractor, // invariant lifetime of extractor_alloc
    'fid,       // invariant lifetime of fields ids map
    'indexer,   // covariant lifetime of objects that are borrowed during the entire indexing
    'data,      // invariant on EX::Data lifetime of datastore
    'index,     // covariant lifetime of the index
    EX: SettingsChangeExtractor<'extractor>,
    MSP: Fn() -> bool + Sync,
>(
    documents: &'indexer DatabaseDocuments<'indexer>,
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
) -> Result<()> {
    tracing::trace!("We are resetting the extractor allocators");
    progress.update_progress(step);
    // Clean up and reuse the extractor allocs
    for extractor_alloc in extractor_allocs.iter_mut() {
        tracing::trace!("\tWith {} bytes reset", extractor_alloc.0.allocated_bytes());
        extractor_alloc.0.reset();
    }

    let total_documents = documents.len() as u32;
    let (step, progress_step) = AtomicDocumentStep::new(total_documents);
    progress.update_progress(progress_step);

    let pi = documents.iter(CHUNK_SIZE);
    pi.try_arc_for_each_try_init(
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
        |context, items| {
            if (must_stop_processing)() {
                return Err(Arc::new(InternalError::AbortedIndexation.into()));
            }

            // Clean up and reuse the document-specific allocator
            context.doc_alloc.reset();

            let items = items.as_ref();
            let changes = items
                .iter()
                .filter_map(|item| documents.item_to_database_document(context, item).transpose());

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

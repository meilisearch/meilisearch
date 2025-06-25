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

use rayon::iter::IndexedParallelIterator;
use rayon::slice::ParallelSlice as _;
use roaring::RoaringBitmap;

use super::DocumentChanges;
use crate::update::new::document::{Document, DocumentContext, DocumentFromDb};
use crate::update::new::thread_local::MostlySend;
use crate::update::new::DocumentChange;
use crate::{DocumentId, Result, UserError};

pub trait DocumentUpdater {
    fn update<'doc, T, D>(
        &self,
        context: &'doc DocumentContext<T>,
        docid: DocumentId,
        current: D,
    ) -> Result<Option<DocumentChange<'doc>>>
    where
        T: MostlySend + 'doc,
        D: Document<'doc>;

    /// Creates a new updater that applies `self`, and if `self` does not yield an update, applies `other`
    fn or_else<Other: DocumentUpdater>(self, other: Other) -> DocumentUpdaterOrElse<Self, Other>
    where
        Self: Sized,
    {
        DocumentUpdaterOrElse { u1: self, u2: other }
    }
}

pub struct DocumentUpdaterOrElse<U1, U2> {
    u1: U1,
    u2: U2,
}

impl<U1, U2> DocumentUpdater for DocumentUpdaterOrElse<U1, U2>
where
    U1: DocumentUpdater,
    U2: DocumentUpdater,
{
    fn update<'doc, T, D>(
        &self,
        context: &'doc DocumentContext<T>,
        docid: DocumentId,
        current: D,
    ) -> Result<Option<DocumentChange<'doc>>>
    where
        T: MostlySend + 'doc,
        D: Document<'doc>,
    {
        let u1 = self.u1.update(context, docid, &current)?;
        match u1 {
            Some(u1) => Ok(Some(u1)),
            None => self.u2.update(context, docid, current),
        }
    }
}

pub struct DocumentUpdateChanges<U: DocumentUpdater> {
    documents: Vec<DocumentId>,
    updater: U,
}

impl<U: DocumentUpdater> DocumentUpdateChanges<U> {
    pub fn new(documents: impl IntoIterator<Item = DocumentId>, updater: U) -> Self {
        Self { updater, documents: documents.into_iter().collect() }
    }
}

impl<'index, U: DocumentUpdater + Sync> DocumentChanges<'index> for DocumentUpdateChanges<U> {
    type Item = DocumentId;

    fn iter(
        &self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = impl AsRef<[Self::Item]>> {
        self.documents.as_slice().par_chunks(chunk_size)
    }

    fn len(&self) -> usize {
        self.documents.len()
    }

    fn item_to_document_change<
        'doc, // lifetime of a single `process` call
        T: MostlySend,
    >(
        &'doc self,
        context: &'doc DocumentContext<T>,
        item: &'doc Self::Item,
    ) -> Result<Option<DocumentChange<'doc>>>
    where
        'index: 'doc,
    {
        let docid = *item;
        let Some(current) =
            DocumentFromDb::new(docid, &context.rtxn, context.index, context.db_fields_ids_map)?
        else {
            return Err(UserError::UnknownInternalDocumentId { document_id: docid }.into());
        };

        self.updater.update(context, docid, current)
    }

    fn shard_docids(&self, _shard: &str, _docids: &mut RoaringBitmap) -> bool {
        false
    }
}

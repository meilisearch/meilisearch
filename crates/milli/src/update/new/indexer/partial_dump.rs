use std::ops::DerefMut;

use bumparaw_collections::RawMap;
use rayon::iter::IndexedParallelIterator;
use rustc_hash::FxBuildHasher;
use serde_json::value::RawValue;

use super::document_changes::{DocumentChangeContext, DocumentChanges};
use crate::documents::PrimaryKey;
use crate::update::concurrent_available_ids::ConcurrentAvailableIds;
use crate::update::new::document::Versions;
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::{DocumentChange, Insertion};
use crate::{Error, InternalError, Result, UserError};

pub struct PartialDump<I> {
    iter: I,
}

impl<I> PartialDump<I> {
    pub fn new_from_jsonlines(iter: I) -> Self {
        PartialDump { iter }
    }

    pub fn into_changes<'index>(
        self,
        concurrent_available_ids: &'index ConcurrentAvailableIds,
        primary_key: &'index PrimaryKey,
    ) -> PartialDumpChanges<'index, I> {
        // Note for future self:
        //   - We recommend sending chunks of documents in this `PartialDumpIndexer` we therefore need to create a custom take_while_size method (that doesn't drop items).
        PartialDumpChanges { iter: self.iter, concurrent_available_ids, primary_key }
    }
}

pub struct PartialDumpChanges<'doc, I> {
    iter: I,
    concurrent_available_ids: &'doc ConcurrentAvailableIds,
    primary_key: &'doc PrimaryKey<'doc>,
}

impl<'index, Iter> DocumentChanges<'index> for PartialDumpChanges<'index, Iter>
where
    Iter: IndexedParallelIterator<Item = Box<RawValue>> + Clone + Sync + 'index,
{
    type Item = Box<RawValue>;

    fn iter(
        &self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = impl AsRef<[Self::Item]>> {
        self.iter.clone().chunks(chunk_size)
    }

    fn item_to_document_change<'doc, T: MostlySend + 'doc>(
        &'doc self,
        context: &'doc DocumentChangeContext<T>,
        document: &'doc Self::Item,
    ) -> Result<Option<DocumentChange<'doc>>>
    where
        'index: 'doc,
    {
        let doc_alloc = &context.doc_alloc;
        let docid = match self.concurrent_available_ids.next() {
            Some(id) => id,
            None => return Err(Error::UserError(UserError::DocumentLimitReached)),
        };

        let mut fields_ids_map = context.new_fields_ids_map.borrow_mut_or_yield();
        let fields_ids_map = fields_ids_map.deref_mut();

        let document = doc_alloc.alloc_str(document.get());
        let document: &RawValue = unsafe { std::mem::transmute(document) };

        let external_document_id =
            self.primary_key.extract_fields_and_docid(document, fields_ids_map, doc_alloc)?;
        let external_document_id = external_document_id.to_de();

        let document = RawMap::from_raw_value_and_hasher(document, FxBuildHasher, doc_alloc)
            .map_err(InternalError::SerdeJson)?;

        let insertion = Insertion::create(docid, external_document_id, Versions::single(document));
        Ok(Some(DocumentChange::Insertion(insertion)))
    }

    fn len(&self) -> usize {
        self.iter.len()
    }
}

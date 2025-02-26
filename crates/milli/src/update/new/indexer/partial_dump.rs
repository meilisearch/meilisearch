use std::ops::DerefMut;

use bumparaw_collections::RawMap;
use rayon::iter::IndexedParallelIterator;
use rustc_hash::FxBuildHasher;
use scoped_thread_pool::ThreadPool;
use serde_json::value::RawValue;

use super::document_changes::{DocumentChangeContext, DocumentChanges};
use crate::documents::PrimaryKey;
use crate::update::concurrent_available_ids::ConcurrentAvailableIds;
use crate::update::new::document::Versions;
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::{DocumentChange, Insertion};
use crate::{Error, InternalError, Result, UserError};

pub struct PartialDump;

impl PartialDump {
    pub fn new_from_jsonlines() -> Self {
        PartialDump
    }

    pub fn into_changes<'index>(
        self,
        concurrent_available_ids: &'index ConcurrentAvailableIds,
        primary_key: &'index PrimaryKey,
        thread_pool: &ThreadPool<crate::Error>,
        chunk_size: usize,
    ) -> PartialDumpChanges<'index> {
        // Note for future self:
        //   - We recommend sending chunks of documents in this `PartialDumpIndexer` we therefore need to create a custom take_while_size method (that doesn't drop items).
        PartialDumpChanges { concurrent_available_ids, primary_key }
    }
}

pub struct PartialDumpChanges<'doc> {
    concurrent_available_ids: &'doc ConcurrentAvailableIds,
    primary_key: &'doc PrimaryKey<'doc>,
}

impl<'index> DocumentChanges<'index> for PartialDumpChanges<'index> {
    type Item = Box<RawValue>;

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
        unimplemented!()
    }

    fn items(&self, thread_index: usize, task_index: usize) -> Option<&[Self::Item]> {
        unimplemented!()
    }
}

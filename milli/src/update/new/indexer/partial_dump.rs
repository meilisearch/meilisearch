use std::ops::DerefMut;

use rayon::iter::IndexedParallelIterator;
use serde::Deserializer;
use serde_json::value::RawValue;

use super::de::DocumentVisitor;
use super::document_changes::{DocumentChangeContext, DocumentChanges, MostlySend};
use crate::documents::{DocumentIdExtractionError, PrimaryKey};
use crate::update::concurrent_available_ids::ConcurrentAvailableIds;
use crate::update::new::document::DocumentFromVersions;
use crate::update::new::document_change::Versions;
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
        /// Note for future self:
        ///   - We recommend sending chunks of documents in this `PartialDumpIndexer` we therefore need to create a custom take_while_size method (that doesn't drop items).
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

    fn iter(&self) -> impl IndexedParallelIterator<Item = Self::Item> {
        self.iter.clone()
    }

    fn item_to_document_change<'doc, T: MostlySend + 'doc>(
        &'doc self,
        context: &'doc DocumentChangeContext<T>,
        document: Self::Item,
    ) -> Result<DocumentChange<'doc>>
    where
        'index: 'doc,
    {
        let doc_alloc = &context.doc_alloc;
        let docid = match self.concurrent_available_ids.next() {
            Some(id) => id,
            None => return Err(Error::UserError(UserError::DocumentLimitReached)),
        };

        let mut fields_ids_map = context.new_fields_ids_map.borrow_mut();
        let fields_ids_map = fields_ids_map.deref_mut();

        let res = document
            .deserialize_map(DocumentVisitor::new(fields_ids_map, self.primary_key, doc_alloc))
            .map_err(UserError::SerdeJson)?;

        let external_document_id = match res {
            Ok(document_id) => Ok(document_id),
            Err(DocumentIdExtractionError::InvalidDocumentId(e)) => Err(e),
            Err(DocumentIdExtractionError::MissingDocumentId) => {
                Err(UserError::MissingDocumentId {
                    primary_key: self.primary_key.name().to_string(),
                    document: serde_json::from_str(document.get()).unwrap(),
                })
            }
            Err(DocumentIdExtractionError::TooManyDocumentIds(_)) => {
                Err(UserError::TooManyDocumentIds {
                    primary_key: self.primary_key.name().to_string(),
                    document: serde_json::from_str(document.get()).unwrap(),
                })
            }
        }?;
        let document = doc_alloc.alloc_str(document.get());
        let document: &RawValue = unsafe { std::mem::transmute(document) };

        let document = raw_collections::RawMap::from_raw_value(document, doc_alloc)
            .map_err(InternalError::SerdeJson)?;

        let document = document.into_bump_slice();
        let document = DocumentFromVersions::new(Versions::Single(document));

        let insertion = Insertion::create(docid, external_document_id.to_owned(), document);
        Ok(DocumentChange::Insertion(insertion))
    }
}

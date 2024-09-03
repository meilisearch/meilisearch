use std::sync::Arc;

use rayon::iter::{ParallelBridge, ParallelIterator};
use roaring::RoaringBitmap;

use super::DocumentChanges;
use crate::documents::PrimaryKey;
use crate::update::new::{Deletion, DocumentChange, ItemsPool};
use crate::{FieldsIdsMap, Index, InternalError, Result};

pub struct DocumentDeletion {
    pub to_delete: RoaringBitmap,
}

impl DocumentDeletion {
    pub fn new() -> Self {
        Self { to_delete: Default::default() }
    }

    pub fn delete_documents_by_docids(&mut self, docids: RoaringBitmap) {
        self.to_delete |= docids;
    }
}

impl<'p> DocumentChanges<'p> for DocumentDeletion {
    type Parameter = (&'p Index, &'p FieldsIdsMap, &'p PrimaryKey<'p>);

    fn document_changes(
        self,
        _fields_ids_map: &mut FieldsIdsMap,
        param: Self::Parameter,
    ) -> Result<impl ParallelIterator<Item = Result<DocumentChange>> + Clone + 'p> {
        let (index, fields, primary_key) = param;
        let items = Arc::new(ItemsPool::new(|| index.read_txn().map_err(crate::Error::from)));
        Ok(self.to_delete.into_iter().par_bridge().map_with(items, |items, docid| {
            items.with(|rtxn| {
                let current = index.document(rtxn, docid)?;
                let external_docid = match primary_key.document_id(current, fields)? {
                    Ok(document_id) => Ok(document_id) as Result<_>,
                    Err(_) => Err(InternalError::DocumentsError(
                        crate::documents::Error::InvalidDocumentFormat,
                    )
                    .into()),
                }?;

                Ok(DocumentChange::Deletion(Deletion::create(
                    docid,
                    external_docid,
                    current.boxed(),
                )))
            })
        }))
    }
}

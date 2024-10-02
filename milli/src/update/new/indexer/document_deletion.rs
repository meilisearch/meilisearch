use std::sync::Arc;

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator};
use roaring::RoaringBitmap;

use super::DocumentChanges;
use crate::documents::PrimaryKey;
use crate::index::db_name::EXTERNAL_DOCUMENTS_IDS;
use crate::update::new::parallel_iterator_ext::ParallelIteratorExt as _;
use crate::update::new::{Deletion, DocumentChange};
use crate::{Error, FieldsIdsMap, Index, InternalError, Result};

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
    ) -> Result<
        impl IndexedParallelIterator<Item = std::result::Result<DocumentChange, Arc<Error>>>
            + Clone
            + 'p,
    > {
        let (index, fields_ids_map, primary_key) = param;
        let to_delete: Vec<_> = self.to_delete.into_iter().collect();
        Ok(to_delete.into_par_iter().try_map_try_init(
            || index.read_txn().map_err(crate::Error::from),
            |rtxn, docid| {
                let current = index.document(rtxn, docid)?;
                let external_document_id = primary_key
                    .document_id(&current, fields_ids_map)?
                    .map_err(|_| InternalError::DatabaseMissingEntry {
                        db_name: EXTERNAL_DOCUMENTS_IDS,
                        key: None,
                    })?;
                Ok(DocumentChange::Deletion(Deletion::create(
                    docid,
                    external_document_id,
                    current.boxed(),
                )))
            },
        ))
    }
}

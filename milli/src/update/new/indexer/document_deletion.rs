use std::sync::Arc;

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator};
use roaring::RoaringBitmap;

use super::DocumentChanges;
use crate::update::new::items_pool::ParallelIteratorExt as _;
use crate::update::new::{Deletion, DocumentChange};
use crate::{Error, FieldsIdsMap, Index, Result};

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
    type Parameter = &'p Index;

    fn document_changes(
        self,
        _fields_ids_map: &mut FieldsIdsMap,
        param: Self::Parameter,
    ) -> Result<
        impl IndexedParallelIterator<Item = std::result::Result<DocumentChange, Arc<Error>>>
            + Clone
            + 'p,
    > {
        let index = param;
        let to_delete: Vec<_> = self.to_delete.into_iter().collect();
        Ok(to_delete.into_par_iter().try_map_try_init(
            || index.read_txn().map_err(crate::Error::from),
            |rtxn, docid| {
                let current = index.document(rtxn, docid)?;
                Ok(DocumentChange::Deletion(Deletion::create(docid, current.boxed())))
            },
        ))
    }
}

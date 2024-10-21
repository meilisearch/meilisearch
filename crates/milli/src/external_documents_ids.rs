use std::collections::HashMap;

use heed::types::Str;
use heed::{Database, RoIter, RoTxn, RwTxn};

use crate::{DocumentId, BEU32};

pub enum DocumentOperationKind {
    Create,
    Delete,
}

pub struct DocumentOperation {
    pub external_id: String,
    pub internal_id: DocumentId,
    pub kind: DocumentOperationKind,
}

pub struct ExternalDocumentsIds(Database<Str, BEU32>);

impl ExternalDocumentsIds {
    pub fn new(db: Database<Str, BEU32>) -> ExternalDocumentsIds {
        ExternalDocumentsIds(db)
    }

    /// Returns `true` if hard and soft external documents lists are empty.
    pub fn is_empty(&self, rtxn: &RoTxn<'_>) -> heed::Result<bool> {
        self.0.is_empty(rtxn).map_err(Into::into)
    }

    pub fn get<A: AsRef<str>>(
        &self,
        rtxn: &RoTxn<'_>,
        external_id: A,
    ) -> heed::Result<Option<u32>> {
        self.0.get(rtxn, external_id.as_ref())
    }

    /// An helper function to debug this type, returns an `HashMap` of both,
    /// soft and hard fst maps, combined.
    pub fn to_hash_map(&self, rtxn: &RoTxn<'_>) -> heed::Result<HashMap<String, u32>> {
        let mut map = HashMap::default();
        for result in self.0.iter(rtxn)? {
            let (external, internal) = result?;
            map.insert(external.to_owned(), internal);
        }
        Ok(map)
    }

    /// Applies the list of operations passed as argument, modifying the current external to internal id mapping.
    ///
    /// If the list contains multiple operations on the same external id, then the result is unspecified.
    ///
    /// # Panics
    ///
    /// - If attempting to delete a document that doesn't exist
    /// - If attempting to create a document that already exists
    pub fn apply(
        &self,
        wtxn: &mut RwTxn<'_>,
        operations: Vec<DocumentOperation>,
    ) -> heed::Result<()> {
        for DocumentOperation { external_id, internal_id, kind } in operations {
            match kind {
                DocumentOperationKind::Create => {
                    self.0.put(wtxn, &external_id, &internal_id)?;
                }
                DocumentOperationKind::Delete => {
                    if !self.0.delete(wtxn, &external_id)? {
                        panic!("Attempting to delete a non-existing document")
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns an iterator over all the external ids.
    pub fn iter<'t>(&self, rtxn: &'t RoTxn<'_>) -> heed::Result<RoIter<'t, Str, BEU32>> {
        self.0.iter(rtxn)
    }
}

use std::collections::HashMap;
use std::convert::TryInto;

use heed::types::{OwnedType, Str};
use heed::{Database, RoIter, RoTxn, RwTxn};
use roaring::RoaringBitmap;

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

pub struct ExternalDocumentsIds(pub Database<Str, OwnedType<BEU32>>);

impl ExternalDocumentsIds {
    pub fn new(db: Database<Str, OwnedType<BEU32>>) -> ExternalDocumentsIds {
        ExternalDocumentsIds(db)
    }

    /// Returns `true` if hard and soft external documents lists are empty.
    pub fn is_empty(&self, rtxn: &RoTxn) -> heed::Result<bool> {
        self.0.is_empty(rtxn).map_err(Into::into)
    }

    pub fn get<A: AsRef<str>>(&self, rtxn: &RoTxn, external_id: A) -> heed::Result<Option<u32>> {
        Ok(self.0.get(rtxn, external_id.as_ref())?.map(|x| x.get().try_into().unwrap()))
    }

    /// An helper function to debug this type, returns an `HashMap` of both,
    /// soft and hard fst maps, combined.
    pub fn to_hash_map(&self, rtxn: &RoTxn) -> heed::Result<HashMap<String, u32>> {
        let mut map = HashMap::default();
        for result in self.0.iter(rtxn)? {
            let (external, internal) = result?;
            map.insert(external.to_owned(), internal.get().try_into().unwrap());
        }
        Ok(map)
    }

    /// Looks for the internal ids in the passed bitmap, and returns an iterator over the mapping between
    /// these internal ids and their external id.
    ///
    /// The returned iterator has `Result<(String, DocumentId), RoaringBitmap>` as `Item`,
    /// where the returned values can be:
    /// - `Ok((external_id, internal_id))`: if a mapping was found
    /// - `Err(remaining_ids)`: if the external ids for some of the requested internal ids weren't found.
    ///   In that case the returned bitmap contains the internal ids whose external ids were not found after traversing
    ///   the entire fst.
    pub fn find_external_id_of<'t>(
        &self,
        rtxn: &'t RoTxn,
        internal_ids: RoaringBitmap,
    ) -> heed::Result<ExternalToInternalOwnedIterator<'t>> {
        self.0.iter(rtxn).map(|iter| ExternalToInternalOwnedIterator { iter, internal_ids })
    }

    /// Applies the list of operations passed as argument, modifying the current external to internal id mapping.
    ///
    /// If the list contains multiple operations on the same external id, then the result is unspecified.
    ///
    /// # Panics
    ///
    /// - If attempting to delete a document that doesn't exist
    /// - If attempting to create a document that already exists
    pub fn apply(&self, wtxn: &mut RwTxn, operations: Vec<DocumentOperation>) -> heed::Result<()> {
        for DocumentOperation { external_id, internal_id, kind } in operations {
            match kind {
                DocumentOperationKind::Create => {
                    self.0.put(wtxn, &external_id, &BEU32::new(internal_id))?;
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
}

/// An iterator over mappings between requested internal ids and external ids.
///
/// See [`ExternalDocumentsIds::find_external_id_of`] for details.
pub struct ExternalToInternalOwnedIterator<'t> {
    iter: RoIter<'t, Str, OwnedType<BEU32>>,
    internal_ids: RoaringBitmap,
}

impl<'t> Iterator for ExternalToInternalOwnedIterator<'t> {
    /// A result indicating if a mapping was found, or if the stream was exhausted without finding all internal ids.
    type Item = Result<(&'t str, DocumentId), RoaringBitmap>;

    fn next(&mut self) -> Option<Self::Item> {
        // if all requested ids were found, we won't find any other, so short-circuit
        if self.internal_ids.is_empty() {
            return None;
        }
        loop {
            let (external, internal) = match self.iter.next() {
                Some(Ok((external, internal))) => (external, internal),
                // TODO manage this better, remove panic
                Some(Err(e)) => panic!("{}", e),
                _ => {
                    // we exhausted the stream but we still have some internal ids to find
                    let remaining_ids = std::mem::take(&mut self.internal_ids);
                    return Some(Err(remaining_ids));
                    // note: next calls to `next` will return `None` since we replaced the internal_ids
                    // with the default empty bitmap
                }
            };
            let internal = internal.get();
            let was_contained = self.internal_ids.remove(internal);
            if was_contained {
                return Some(Ok((external, internal)));
            }
        }
    }
}

impl<'t> ExternalToInternalOwnedIterator<'t> {
    /// Returns the bitmap of internal ids whose external id are yet to be found
    pub fn remaining_internal_ids(&self) -> &RoaringBitmap {
        &self.internal_ids
    }

    /// Consumes this iterator and returns an iterator over only the external ids, ignoring the internal ids.
    ///
    /// Use this when you don't need the mapping between the external and the internal ids.
    pub fn only_external_ids(self) -> impl Iterator<Item = Result<String, RoaringBitmap>> + 't {
        self.map(|res| res.map(|(external, _internal)| external.to_owned()))
    }
}

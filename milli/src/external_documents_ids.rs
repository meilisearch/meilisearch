use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;

use fst::Streamer;
use roaring::RoaringBitmap;

use crate::DocumentId;

pub enum DocumentOperationKind {
    Create,
    Delete,
}

pub struct DocumentOperation {
    pub external_id: String,
    pub internal_id: DocumentId,
    pub kind: DocumentOperationKind,
}

pub struct ExternalDocumentsIds<'a>(fst::Map<Cow<'a, [u8]>>);

impl<'a> ExternalDocumentsIds<'a> {
    pub fn new(fst: fst::Map<Cow<'a, [u8]>>) -> ExternalDocumentsIds<'a> {
        ExternalDocumentsIds(fst)
    }

    pub fn into_static(self) -> ExternalDocumentsIds<'static> {
        ExternalDocumentsIds(self.0.map_data(|c| Cow::Owned(c.into_owned())).unwrap())
    }

    /// Returns `true` if hard and soft external documents lists are empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get<A: AsRef<[u8]>>(&self, external_id: A) -> Option<u32> {
        let external_id = external_id.as_ref();
        self.0.get(external_id).map(|x| x.try_into().unwrap())
    }

    /// An helper function to debug this type, returns an `HashMap` of both,
    /// soft and hard fst maps, combined.
    pub fn to_hash_map(&self) -> HashMap<String, u32> {
        let mut map = HashMap::default();
        let mut stream = self.0.stream();
        while let Some((k, v)) = stream.next() {
            let k = String::from_utf8(k.to_vec()).unwrap();
            map.insert(k, v.try_into().unwrap());
        }
        map
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_fst().as_bytes()
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
    pub fn find_external_id_of(
        &self,
        internal_ids: RoaringBitmap,
    ) -> ExternalToInternalOwnedIterator<'_> {
        let it = ExternalToInternalOwnedIterator { stream: self.0.stream(), internal_ids };
        it
    }

    /// Applies the list of operations passed as argument, modifying the current external to internal id mapping.
    ///
    /// If the list contains multiple operations on the same external id, then the result is unspecified.
    ///
    /// # Panics
    ///
    /// - If attempting to delete a document that doesn't exist
    /// - If attempting to create a document that already exists
    pub fn apply(&mut self, mut operations: Vec<DocumentOperation>) {
        operations.sort_unstable_by(|left, right| left.external_id.cmp(&right.external_id));
        operations.dedup_by(|left, right| left.external_id == right.external_id);

        let mut builder = fst::MapBuilder::memory();

        let mut stream = self.0.stream();
        let mut next_stream = stream.next();
        let mut operations = operations.iter();
        let mut next_operation = operations.next();

        loop {
            (next_stream, next_operation) = match (next_stream.take(), next_operation.take()) {
                (None, None) => break,
                (None, Some(DocumentOperation { external_id, internal_id, kind })) => {
                    if matches!(kind, DocumentOperationKind::Delete) {
                        panic!("Attempting to delete a non-existing document")
                    }
                    builder.insert(external_id, (*internal_id).into()).unwrap();
                    (None, operations.next())
                }
                (Some((k, v)), None) => {
                    builder.insert(k, v).unwrap();
                    (stream.next(), None)
                }
                (
                    current_stream @ Some((left_external_id, left_internal_id)),
                    current_operation @ Some(DocumentOperation {
                        external_id: right_external_id,
                        internal_id: right_internal_id,
                        kind,
                    }),
                ) => match left_external_id.cmp(right_external_id.as_bytes()) {
                    std::cmp::Ordering::Less => {
                        builder.insert(left_external_id, left_internal_id).unwrap();
                        (stream.next(), current_operation)
                    }
                    std::cmp::Ordering::Greater => {
                        builder.insert(right_external_id, (*right_internal_id).into()).unwrap();
                        (current_stream, operations.next())
                    }
                    std::cmp::Ordering::Equal => {
                        if matches!(kind, DocumentOperationKind::Create) {
                            panic!("Attempting to create an already-existing document");
                        }
                        // we delete the document, so we just advance both iterators to skip in stream
                        (stream.next(), operations.next())
                    }
                },
            }
        }
        self.0 = builder.into_map().map_data(Cow::Owned).unwrap();
    }
}

impl fmt::Debug for ExternalDocumentsIds<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("ExternalDocumentsIds").field(&self.to_hash_map()).finish()
    }
}

impl Default for ExternalDocumentsIds<'static> {
    fn default() -> Self {
        ExternalDocumentsIds(fst::Map::default().map_data(Cow::Owned).unwrap())
    }
}

/// An iterator over mappings between requested internal ids and external ids.
///
/// See [`ExternalDocumentsIds::find_external_id_of`] for details.
pub struct ExternalToInternalOwnedIterator<'it> {
    stream: fst::map::Stream<'it>,
    internal_ids: RoaringBitmap,
}

impl<'it> Iterator for ExternalToInternalOwnedIterator<'it> {
    /// A result indicating if a mapping was found, or if the stream was exhausted without finding all internal ids.
    type Item = Result<(String, DocumentId), RoaringBitmap>;

    fn next(&mut self) -> Option<Self::Item> {
        // if all requested ids were found, we won't find any other, so short-circuit
        if self.internal_ids.is_empty() {
            return None;
        }
        loop {
            let Some((external, internal)) = self.stream.next() else {
                // we exhausted the stream but we still have some internal ids to find
                let remaining_ids = std::mem::take(&mut self.internal_ids);
                return Some(Err(remaining_ids));
                // note: next calls to `next` will return `None` since we replaced the internal_ids
                // with the default empty bitmap
            };
            let internal = internal.try_into().unwrap();
            let was_contained = self.internal_ids.remove(internal);
            if was_contained {
                return Some(Ok((std::str::from_utf8(external).unwrap().to_owned(), internal)));
            }
        }
    }
}

impl<'it> ExternalToInternalOwnedIterator<'it> {
    /// Returns the bitmap of internal ids whose external id are yet to be found
    pub fn remaining_internal_ids(&self) -> &RoaringBitmap {
        &self.internal_ids
    }

    /// Consumes this iterator and returns an iterator over only the external ids, ignoring the internal ids.
    ///
    /// Use this when you don't need the mapping between the external and the internal ids.
    pub fn only_external_ids(self) -> impl Iterator<Item = Result<String, RoaringBitmap>> + 'it {
        self.map(|res| res.map(|(external, _internal)| external))
    }
}

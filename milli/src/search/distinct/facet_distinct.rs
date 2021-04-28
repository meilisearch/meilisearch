use std::mem::size_of;

use heed::types::ByteSlice;
use roaring::RoaringBitmap;

use super::{Distinct, DocIter};
use crate::heed_codec::facet::*;
use crate::{DocumentId, FieldId, Index};

const FID_SIZE: usize = size_of::<FieldId>();
const DOCID_SIZE: usize = size_of::<DocumentId>();

/// A distinct implementer that is backed by facets.
///
/// On each iteration, the facet values for the
/// distinct attribute of the first document are retrieved. The document ids for these facet values
/// are then retrieved and taken out of the the candidate and added to the excluded set. We take
/// care to keep the document we are currently on, and remove it from the excluded list. The next
/// iterations will never contain any occurence of a document with the same distinct value as a
/// document from previous iterations.
pub struct FacetDistinct<'a> {
    distinct: FieldId,
    index: &'a Index,
    txn: &'a heed::RoTxn<'a>,
}

impl<'a> FacetDistinct<'a> {
    pub fn new(
        distinct: FieldId,
        index: &'a Index,
        txn: &'a heed::RoTxn<'a>,
    ) -> Self
    {
        Self { distinct, index, txn }
    }
}

pub struct FacetDistinctIter<'a> {
    candidates: RoaringBitmap,
    distinct: FieldId,
    excluded: RoaringBitmap,
    index: &'a Index,
    iter_offset: usize,
    txn: &'a heed::RoTxn<'a>,
}

impl<'a> FacetDistinctIter<'a> {
    fn facet_string_docids(&self, key: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index
            .facet_id_string_docids
            .get(self.txn, &(self.distinct, key))
    }

    fn facet_number_docids(&self, key: f64) -> heed::Result<Option<RoaringBitmap>> {
        // get facet docids on level 0
        self.index
            .facet_id_f64_docids
            .get(self.txn, &(self.distinct, 0, key, key))
    }

    fn distinct_string(&mut self, id: DocumentId) -> anyhow::Result<()> {
        let iter = facet_string_values(id, self.distinct, self.index, self.txn)?;

        for item in iter {
            let ((_, _, value), _) = item?;
            let facet_docids = self
                .facet_string_docids(value)?
                .expect("Corrupted data: Facet values must exist");
            self.excluded.union_with(&facet_docids);
        }

        self.excluded.remove(id);

        Ok(())
    }

    fn distinct_number(&mut self, id: DocumentId) -> anyhow::Result<()> {
        let iter = facet_number_values(id, self.distinct, self.index, self.txn)?;

        for item in iter {
            let ((_, _, value), _) = item?;
            let facet_docids = self
                .facet_number_docids(value)?
                .expect("Corrupted data: Facet values must exist");
            self.excluded.union_with(&facet_docids);
        }

        self.excluded.remove(id);

        Ok(())
    }

    /// Performs the next iteration of the facet distinct. This is a convenience method that is
    /// called by the Iterator::next implementation that transposes the result. It makes error
    /// handling easier.
    fn next_inner(&mut self) -> anyhow::Result<Option<DocumentId>> {
        // The first step is to remove all the excluded documents from our candidates
        self.candidates.difference_with(&self.excluded);

        let mut candidates_iter = self.candidates.iter().skip(self.iter_offset);
        match candidates_iter.next() {
            Some(id) => {
                match self.facet_type {
                    FacetType::String => self.distinct_string(id)?,
                    FacetType::Number => self.distinct_number(id)?,
                };

                // The first document of each iteration is kept, since the next call to
                // `difference_with` will filter out all the documents for that facet value. By
                // increasing the offset we make sure to get the first valid value for the next
                // distinct document to keep.
                self.iter_offset += 1;

                Ok(Some(id))
            }
            // no more candidate at this offset, return.
            None => Ok(None),
        }
    }
}

fn facet_values_prefix_key(distinct: FieldId, id: DocumentId) -> [u8; FID_SIZE + DOCID_SIZE] {
    let mut key = [0; FID_SIZE + DOCID_SIZE];
    key[0..FID_SIZE].copy_from_slice(&distinct.to_be_bytes());
    key[FID_SIZE..].copy_from_slice(&id.to_be_bytes());
    key
}

fn facet_number_values<'a>(
    id: DocumentId,
    distinct: FieldId,
    index: &Index,
    txn: &'a heed::RoTxn,
) -> anyhow::Result<heed::RoPrefix<'a, FieldDocIdFacetF64Codec, heed::types::Unit>> {
    let key = facet_values_prefix_key(distinct, id);

    let iter = index
        .field_id_docid_facet_f64s
        .remap_key_type::<ByteSlice>()
        .prefix_iter(txn, &key)?
        .remap_key_type::<FieldDocIdFacetF64Codec>();

    Ok(iter)
}

fn facet_string_values<'a>(
    id: DocumentId,
    distinct: FieldId,
    index: &Index,
    txn: &'a heed::RoTxn,
) -> anyhow::Result<heed::RoPrefix<'a, FieldDocIdFacetStringCodec, heed::types::Unit>> {
    let key = facet_values_prefix_key(distinct, id);

    let iter = index
        .field_id_docid_facet_strings
        .remap_key_type::<ByteSlice>()
        .prefix_iter(txn, &key)?
        .remap_key_type::<FieldDocIdFacetStringCodec>();

    Ok(iter)
}

impl Iterator for FacetDistinctIter<'_> {
    type Item = anyhow::Result<DocumentId>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}

impl DocIter for FacetDistinctIter<'_> {
    fn into_excluded(self) -> RoaringBitmap {
        self.excluded
    }
}

impl<'a> Distinct<'_> for FacetDistinct<'a> {
    type Iter = FacetDistinctIter<'a>;

    fn distinct(&mut self, candidates: RoaringBitmap, excluded: RoaringBitmap) -> Self::Iter {
        FacetDistinctIter {
            candidates,
            distinct: self.distinct,
            excluded,
            index: self.index,
            iter_offset: 0,
            txn: self.txn,
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::super::test::{generate_index, validate_distinct_candidates};
    use super::*;
    use crate::facet::FacetType;

    macro_rules! test_facet_distinct {
        ($name:ident, $distinct:literal, $facet_type:expr) => {
            #[test]
            fn $name() {
                use std::iter::FromIterator;

                let facets =
                    HashMap::from_iter(Some(($distinct.to_string(), $facet_type.to_string())));
                let (index, fid, candidates) = generate_index($distinct, facets);
                let txn = index.read_txn().unwrap();
                let mut map_distinct = FacetDistinct::new(fid, &index, &txn, $facet_type);
                let excluded = RoaringBitmap::new();
                let mut iter = map_distinct.distinct(candidates.clone(), excluded);
                let count = validate_distinct_candidates(iter.by_ref(), fid, &index);
                let excluded = iter.into_excluded();
                assert_eq!(count as u64 + excluded.len(), candidates.len());
            }
        };
    }

    test_facet_distinct!(test_string, "txt", FacetType::String);
    test_facet_distinct!(test_strings, "txts", FacetType::String);
    test_facet_distinct!(test_number, "cat-int", FacetType::Number);
}

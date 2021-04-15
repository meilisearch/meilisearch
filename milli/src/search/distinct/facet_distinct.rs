use std::mem::size_of;

use roaring::RoaringBitmap;

use crate::heed_codec::facet::*;
use crate::{facet::FacetType, DocumentId, FieldId, Index};
use super::{Distinct, DocIter};

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
    facet_type: FacetType,
}

impl<'a> FacetDistinct<'a> {
    pub fn new(
        distinct: FieldId,
        index: &'a Index,
        txn: &'a heed::RoTxn<'a>,
        facet_type: FacetType,
    ) -> Self {
        Self {
            distinct,
            index,
            txn,
            facet_type,
        }
    }
}

pub struct FacetDistinctIter<'a> {
    candidates: RoaringBitmap,
    distinct: FieldId,
    excluded: RoaringBitmap,
    facet_type: FacetType,
    index: &'a Index,
    iter_offset: usize,
    txn: &'a heed::RoTxn<'a>,
}

impl<'a> FacetDistinctIter<'a> {
    fn get_facet_docids<'c, KC>(&self, key: &'c KC::EItem) -> anyhow::Result<RoaringBitmap>
    where
        KC: heed::BytesEncode<'c>,
    {
        let facet_docids = self
            .index
            .facet_field_id_value_docids
            .remap_key_type::<KC>()
            .get(self.txn, key)?
            .expect("Corrupted data: Facet values must exist");
        Ok(facet_docids)
    }

    fn distinct_string(&mut self, id: DocumentId) -> anyhow::Result<()> {
        let iter = get_facet_values::<FieldDocIdFacetStringCodec>(
            id,
            self.distinct,
            self.index,
            self.txn,
        )?;

        for item in iter {
            let ((_, _, value), _) = item?;
            let key = (self.distinct, value);
            let facet_docids = self.get_facet_docids::<FacetValueStringCodec>(&key)?;
            self.excluded.union_with(&facet_docids);
        }

        self.excluded.remove(id);

        Ok(())
    }

    fn distinct_integer(&mut self, id: DocumentId) -> anyhow::Result<()> {
        let iter = get_facet_values::<FieldDocIdFacetI64Codec>(
            id,
            self.distinct,
            self.index,
            self.txn,
        )?;

        for item in iter {
            let ((_, _, value), _) = item?;
            // get facet docids on level 0
            let key = (self.distinct, 0, value, value);
            let facet_docids = self.get_facet_docids::<FacetLevelValueI64Codec>(&key)?;
            self.excluded.union_with(&facet_docids);
        }

        self.excluded.remove(id);

        Ok(())
    }

    fn distinct_float(&mut self, id: DocumentId) -> anyhow::Result<()> {
        let iter = get_facet_values::<FieldDocIdFacetF64Codec>(id,
            self.distinct,
            self.index,
            self.txn,
        )?;

        for item in iter {
            let ((_, _, value), _) = item?;
            // get facet docids on level 0
            let key = (self.distinct, 0, value, value);
            let facet_docids = self.get_facet_docids::<FacetLevelValueF64Codec>(&key)?;
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
                    FacetType::Integer => self.distinct_integer(id)?,
                    FacetType::Float => self.distinct_float(id)?,
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

fn get_facet_values<'a, KC>(
    id: DocumentId,
    distinct: FieldId,
    index: &Index,
    txn: &'a heed::RoTxn,
) -> anyhow::Result<heed::RoPrefix<'a, KC, heed::types::Unit>>
where
    KC: heed::BytesDecode<'a>,
{
    const FID_SIZE: usize = size_of::<FieldId>();
    const DOCID_SIZE: usize = size_of::<DocumentId>();

    let mut key = [0; FID_SIZE + DOCID_SIZE];
    key[0..FID_SIZE].copy_from_slice(&distinct.to_be_bytes());
    key[FID_SIZE..].copy_from_slice(&id.to_be_bytes());

    let iter = index
        .field_id_docid_facet_values
        .prefix_iter(txn, &key)?
        .remap_key_type::<KC>();
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
            facet_type: self.facet_type,
            index: self.index,
            iter_offset: 0,
            txn: self.txn,
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use super::super::test::{generate_index, validate_distinct_candidates};
    use crate::facet::FacetType;

    macro_rules! test_facet_distinct {
        ($name:ident, $distinct:literal, $facet_type:expr) => {
            #[test]
            fn $name() {
                use std::iter::FromIterator;

                let facets = HashMap::from_iter(Some(($distinct.to_string(), $facet_type.to_string())));
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
    test_facet_distinct!(test_int, "cat-int", FacetType::Integer);
    test_facet_distinct!(test_ints, "cat-ints", FacetType::Integer);
}

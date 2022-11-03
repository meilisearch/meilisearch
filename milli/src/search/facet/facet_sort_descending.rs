use std::ops::Bound;

use heed::Result;
use roaring::RoaringBitmap;

use super::{get_first_facet_value, get_highest_level, get_last_facet_value};
use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValue, FacetGroupValueCodec,
};
use crate::heed_codec::ByteSliceRefCodec;

/// See documentationg for [`ascending_facet_sort`](super::ascending_facet_sort).
///
/// This function does the same thing, but in the opposite order.
pub fn descending_facet_sort<'t>(
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
    field_id: u16,
    candidates: RoaringBitmap,
) -> Result<Box<dyn Iterator<Item = Result<RoaringBitmap>> + 't>> {
    let highest_level = get_highest_level(rtxn, db, field_id)?;
    if let Some(first_bound) = get_first_facet_value::<ByteSliceRefCodec>(rtxn, db, field_id)? {
        let first_key = FacetGroupKey { field_id, level: highest_level, left_bound: first_bound };
        let last_bound = get_last_facet_value::<ByteSliceRefCodec>(rtxn, db, field_id)?.unwrap();
        let last_key = FacetGroupKey { field_id, level: highest_level, left_bound: last_bound };
        let iter = db.rev_range(rtxn, &(first_key..=last_key))?.take(usize::MAX);
        Ok(Box::new(DescendingFacetSort {
            rtxn,
            db,
            field_id,
            stack: vec![(candidates, iter, Bound::Included(last_bound))],
        }))
    } else {
        Ok(Box::new(std::iter::empty()))
    }
}

struct DescendingFacetSort<'t> {
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
    field_id: u16,
    #[allow(clippy::type_complexity)]
    stack: Vec<(
        RoaringBitmap,
        std::iter::Take<
            heed::RoRevRange<'t, FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
        >,
        Bound<&'t [u8]>,
    )>,
}

impl<'t> Iterator for DescendingFacetSort<'t> {
    type Item = Result<RoaringBitmap>;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            let (documents_ids, deepest_iter, right_bound) = self.stack.last_mut()?;
            for result in deepest_iter.by_ref() {
                let (
                    FacetGroupKey { level, left_bound, field_id },
                    FacetGroupValue { size: group_size, mut bitmap },
                ) = result.unwrap();
                // The range is unbounded on the right and the group size for the highest level is MAX,
                // so we need to check that we are not iterating over the next field id
                if field_id != self.field_id {
                    return None;
                }
                // If the last iterator found an empty set of documents it means
                // that we found all the documents in the sub level iterations already,
                // we can pop this level iterator.
                if documents_ids.is_empty() {
                    break;
                }

                bitmap &= &*documents_ids;
                if !bitmap.is_empty() {
                    *documents_ids -= &bitmap;

                    if level == 0 {
                        return Some(Ok(bitmap));
                    }
                    let starting_key_below =
                        FacetGroupKey { field_id, level: level - 1, left_bound };

                    let end_key_kelow = match *right_bound {
                        Bound::Included(right) => Bound::Included(FacetGroupKey {
                            field_id,
                            level: level - 1,
                            left_bound: right,
                        }),
                        Bound::Excluded(right) => Bound::Excluded(FacetGroupKey {
                            field_id,
                            level: level - 1,
                            left_bound: right,
                        }),
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    let prev_right_bound = *right_bound;
                    *right_bound = Bound::Excluded(left_bound);
                    let iter = match self
                        .db
                        .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>()
                        .rev_range(self.rtxn, &(Bound::Included(starting_key_below), end_key_kelow))
                    {
                        Ok(iter) => iter,
                        Err(e) => return Some(Err(e)),
                    }
                    .take(group_size as usize);

                    self.stack.push((bitmap, iter, prev_right_bound));
                    continue 'outer;
                }
                *right_bound = Bound::Excluded(left_bound);
            }
            self.stack.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use roaring::RoaringBitmap;

    use crate::heed_codec::facet::FacetGroupKeyCodec;
    use crate::heed_codec::ByteSliceRefCodec;
    use crate::milli_snap;
    use crate::search::facet::facet_sort_descending::descending_facet_sort;
    use crate::search::facet::tests::{get_random_looking_index, get_simple_index};
    use crate::snapshot_tests::display_bitmap;

    #[test]
    fn filter_sort_descending() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = (200..=300).into_iter().collect::<RoaringBitmap>();
            let mut results = String::new();
            let db = index.content.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();
            let iter = descending_facet_sort(&txn, db, 0, candidates).unwrap();
            for el in iter {
                let docids = el.unwrap();
                results.push_str(&display_bitmap(&docids));
                results.push('\n');
            }
            milli_snap!(results, i);

            txn.commit().unwrap();
        }
    }
}

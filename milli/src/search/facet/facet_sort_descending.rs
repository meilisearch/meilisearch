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
) -> Result<impl Iterator<Item = Result<(RoaringBitmap, &'t [u8])>> + 't> {
    let highest_level = get_highest_level(rtxn, db, field_id)?;
    if let Some(first_bound) = get_first_facet_value::<ByteSliceRefCodec>(rtxn, db, field_id)? {
        let first_key = FacetGroupKey { field_id, level: highest_level, left_bound: first_bound };
        let last_bound = get_last_facet_value::<ByteSliceRefCodec>(rtxn, db, field_id)?.unwrap();
        let last_key = FacetGroupKey { field_id, level: highest_level, left_bound: last_bound };
        let iter = db.rev_range(rtxn, &(first_key..=last_key))?.take(usize::MAX);
        Ok(itertools::Either::Left(DescendingFacetSort {
            rtxn,
            db,
            field_id,
            stack: vec![(candidates, iter, Bound::Included(last_bound))],
        }))
    } else {
        Ok(itertools::Either::Right(std::iter::empty()))
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
    type Item = Result<(RoaringBitmap, &'t [u8])>;

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
                        // Since we're at the level 0 the left_bound is the exact value.
                        return Some(Ok((bitmap, left_bound)));
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
    use crate::search::facet::tests::{
        get_random_looking_index, get_random_looking_string_index_with_multiple_field_ids,
        get_simple_index, get_simple_index_with_multiple_field_ids,
        get_simple_string_index_with_multiple_field_ids,
    };
    use crate::snapshot_tests::display_bitmap;

    #[test]
    fn filter_sort_descending() {
        let indexes = [
            get_simple_index(),
            get_random_looking_index(),
            get_simple_index_with_multiple_field_ids(),
        ];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = (200..=300).into_iter().collect::<RoaringBitmap>();
            let mut results = String::new();
            let db = index.content.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();
            let iter = descending_facet_sort(&txn, db, 0, candidates).unwrap();
            for el in iter {
                let (docids, _) = el.unwrap();
                results.push_str(&display_bitmap(&docids));
                results.push('\n');
            }
            milli_snap!(results, i);

            txn.commit().unwrap();
        }
    }

    #[test]
    fn filter_sort_descending_multiple_field_ids() {
        let indexes = [
            get_simple_string_index_with_multiple_field_ids(),
            get_random_looking_string_index_with_multiple_field_ids(),
        ];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = (200..=300).into_iter().collect::<RoaringBitmap>();
            let mut results = String::new();
            let db = index.content.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();
            let iter = descending_facet_sort(&txn, db, 0, candidates.clone()).unwrap();
            for el in iter {
                let (docids, _) = el.unwrap();
                results.push_str(&display_bitmap(&docids));
                results.push('\n');
            }
            milli_snap!(results, format!("{i}-0"));

            let mut results = String::new();

            let iter = descending_facet_sort(&txn, db, 1, candidates).unwrap();
            for el in iter {
                let (docids, _) = el.unwrap();
                results.push_str(&display_bitmap(&docids));
                results.push('\n');
            }
            milli_snap!(results, format!("{i}-1"));

            txn.commit().unwrap();
        }
    }
    #[test]
    fn filter_sort_ascending_with_no_candidates() {
        let indexes = [
            get_simple_string_index_with_multiple_field_ids(),
            get_random_looking_string_index_with_multiple_field_ids(),
        ];
        for (_i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = RoaringBitmap::new();
            let mut results = String::new();
            let iter = descending_facet_sort(&txn, index.content, 0, candidates.clone()).unwrap();
            for el in iter {
                let (docids, _) = el.unwrap();
                results.push_str(&display_bitmap(&docids));
                results.push('\n');
            }
            assert!(results.is_empty());

            let mut results = String::new();
            let iter = descending_facet_sort(&txn, index.content, 1, candidates).unwrap();
            for el in iter {
                let (docids, _) = el.unwrap();
                results.push_str(&display_bitmap(&docids));
                results.push('\n');
            }
            assert!(results.is_empty());

            txn.commit().unwrap();
        }
    }

    #[test]
    fn filter_sort_ascending_with_inexisting_field_id() {
        let indexes = [
            get_simple_string_index_with_multiple_field_ids(),
            get_random_looking_string_index_with_multiple_field_ids(),
        ];
        for (_i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = RoaringBitmap::new();
            let mut results = String::new();
            let iter = descending_facet_sort(&txn, index.content, 3, candidates.clone()).unwrap();
            for el in iter {
                let (docids, _) = el.unwrap();
                results.push_str(&display_bitmap(&docids));
                results.push('\n');
            }
            assert!(results.is_empty());

            txn.commit().unwrap();
        }
    }
}

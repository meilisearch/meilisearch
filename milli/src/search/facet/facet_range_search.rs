use std::ops::{Bound, RangeBounds};

use heed::BytesEncode;
use roaring::RoaringBitmap;

use super::{get_first_facet_value, get_highest_level, get_last_facet_value};
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::heed_codec::ByteSliceRefCodec;
use crate::Result;

/// Find all the document ids for which the given field contains a value contained within
/// the two bounds.
pub fn find_docids_of_facet_within_bounds<'t, BoundCodec>(
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<BoundCodec>, FacetGroupValueCodec>,
    field_id: u16,
    left: &'t Bound<<BoundCodec as BytesEncode<'t>>::EItem>,
    right: &'t Bound<<BoundCodec as BytesEncode<'t>>::EItem>,
    docids: &mut RoaringBitmap,
) -> Result<()>
where
    BoundCodec: for<'a> BytesEncode<'a>,
    for<'a> <BoundCodec as BytesEncode<'a>>::EItem: Sized,
{
    let inner;
    let left = match left {
        Bound::Included(left) => {
            inner = BoundCodec::bytes_encode(left).ok_or(heed::Error::Encoding)?;
            Bound::Included(inner.as_ref())
        }
        Bound::Excluded(left) => {
            inner = BoundCodec::bytes_encode(left).ok_or(heed::Error::Encoding)?;
            Bound::Excluded(inner.as_ref())
        }
        Bound::Unbounded => Bound::Unbounded,
    };
    let inner;
    let right = match right {
        Bound::Included(right) => {
            inner = BoundCodec::bytes_encode(right).ok_or(heed::Error::Encoding)?;
            Bound::Included(inner.as_ref())
        }
        Bound::Excluded(right) => {
            inner = BoundCodec::bytes_encode(right).ok_or(heed::Error::Encoding)?;
            Bound::Excluded(inner.as_ref())
        }
        Bound::Unbounded => Bound::Unbounded,
    };
    let db = db.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();
    let mut f = FacetRangeSearch { rtxn, db, field_id, left, right, docids };
    let highest_level = get_highest_level(rtxn, db, field_id)?;

    if let Some(starting_left_bound) =
        get_first_facet_value::<ByteSliceRefCodec>(rtxn, db, field_id)?
    {
        let rightmost_bound = Bound::Included(
            get_last_facet_value::<ByteSliceRefCodec>(rtxn, db, field_id)?.unwrap(),
        ); // will not fail because get_first_facet_value succeeded
        let group_size = usize::MAX;
        f.run(highest_level, starting_left_bound, rightmost_bound, group_size)?;
        Ok(())
    } else {
        Ok(())
    }
}

/// Fetch the document ids that have a facet with a value between the two given bounds
struct FacetRangeSearch<'t, 'b, 'bitmap> {
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
    field_id: u16,
    left: Bound<&'b [u8]>,
    right: Bound<&'b [u8]>,
    docids: &'bitmap mut RoaringBitmap,
}
impl<'t, 'b, 'bitmap> FacetRangeSearch<'t, 'b, 'bitmap> {
    fn run_level_0(&mut self, starting_left_bound: &'t [u8], group_size: usize) -> Result<()> {
        let left_key =
            FacetGroupKey { field_id: self.field_id, level: 0, left_bound: starting_left_bound };
        let iter = self.db.range(self.rtxn, &(left_key..))?.take(group_size);
        for el in iter {
            let (key, value) = el?;
            // the right side of the iter range is unbounded, so we need to make sure that we are not iterating
            // on the next field id
            if key.field_id != self.field_id {
                return Ok(());
            }
            let should_skip = {
                match self.left {
                    Bound::Included(left) => left > key.left_bound,
                    Bound::Excluded(left) => left >= key.left_bound,
                    Bound::Unbounded => false,
                }
            };
            if should_skip {
                continue;
            }
            let should_stop = {
                match self.right {
                    Bound::Included(right) => right < key.left_bound,
                    Bound::Excluded(right) => right <= key.left_bound,
                    Bound::Unbounded => false,
                }
            };
            if should_stop {
                break;
            }

            if RangeBounds::<&[u8]>::contains(&(self.left, self.right), &key.left_bound) {
                *self.docids |= value.bitmap;
            }
        }
        Ok(())
    }

    /// Recursive part of the algorithm for level > 0.
    ///
    /// It works by visiting a slice of a level and checking whether the range asscociated
    /// with each visited element is contained within the bounds.
    ///
    /// 1. So long as the element's range is less than the left bound, we do nothing and keep iterating
    /// 2. If the element's range is fully contained by the bounds, then all of its docids are added to
    /// the roaring bitmap.
    /// 3. If the element's range merely intersects the bounds, then we call the algorithm recursively
    /// on the children of the element from the level below.
    /// 4. If the element's range is greater than the right bound, we do nothing and stop iterating.
    /// Note that the right bound is found through either the `left_bound` of the *next* element,
    /// or from the `rightmost_bound` argument
    ///
    /// ## Arguments
    /// - `level`: the level being visited
    /// - `starting_left_bound`: the left_bound of the first element to visit
    /// - `rightmost_bound`: the right bound of the last element that should be visited
    /// - `group_size`: the number of elements that should be visited
    fn run(
        &mut self,
        level: u8,
        starting_left_bound: &'t [u8],
        rightmost_bound: Bound<&'t [u8]>,
        group_size: usize,
    ) -> Result<()> {
        if level == 0 {
            return self.run_level_0(starting_left_bound, group_size);
        }

        let left_key =
            FacetGroupKey { field_id: self.field_id, level, left_bound: starting_left_bound };
        let mut iter = self.db.range(self.rtxn, &(left_key..))?.take(group_size);

        // We iterate over the range while keeping in memory the previous value
        let (mut previous_key, mut previous_value) = iter.next().unwrap()?;
        for el in iter {
            let (next_key, next_value) = el?;
            // the right of the iter range is potentially unbounded (e.g. if `group_size` is usize::MAX),
            // so we need to make sure that we are not iterating on the next field id
            if next_key.field_id != self.field_id {
                break;
            }
            // now, do we skip, stop, or visit?
            let should_skip = {
                match self.left {
                    Bound::Included(left) => left >= next_key.left_bound,
                    Bound::Excluded(left) => left >= next_key.left_bound,
                    Bound::Unbounded => false,
                }
            };
            if should_skip {
                previous_key = next_key;
                previous_value = next_value;
                continue;
            }

            // should we stop?
            // We should if the the search range doesn't include any
            // element from the previous key or its successors
            let should_stop = {
                match self.right {
                    Bound::Included(right) => right < previous_key.left_bound,
                    Bound::Excluded(right) => right <= previous_key.left_bound,
                    Bound::Unbounded => false,
                }
            };
            if should_stop {
                return Ok(());
            }
            // should we take the whole thing, without recursing down?
            let should_take_whole_group = {
                let left_condition = match self.left {
                    Bound::Included(left) => previous_key.left_bound >= left,
                    Bound::Excluded(left) => previous_key.left_bound > left,
                    Bound::Unbounded => true,
                };
                let right_condition = match self.right {
                    Bound::Included(right) => next_key.left_bound <= right,
                    Bound::Excluded(right) => next_key.left_bound <= right,
                    Bound::Unbounded => true,
                };
                left_condition && right_condition
            };
            if should_take_whole_group {
                *self.docids |= &previous_value.bitmap;
                previous_key = next_key;
                previous_value = next_value;
                continue;
            }
            // from here, we should visit the children of the previous element and
            // call the function recursively

            let level = level - 1;
            let starting_left_bound = previous_key.left_bound;
            let rightmost_bound = Bound::Excluded(next_key.left_bound);
            let group_size = previous_value.size as usize;

            self.run(level, starting_left_bound, rightmost_bound, group_size)?;

            previous_key = next_key;
            previous_value = next_value;
        }
        // previous_key/previous_value are the last element's key/value

        // now, do we skip, stop, or visit?
        let should_skip = {
            match (self.left, rightmost_bound) {
                (Bound::Included(left), Bound::Included(right)) => left > right,
                (Bound::Included(left), Bound::Excluded(right)) => left >= right,
                (Bound::Excluded(left), Bound::Included(right) | Bound::Excluded(right)) => {
                    left >= right
                }
                (Bound::Unbounded, _) => false,
                (_, Bound::Unbounded) => false, // should never run?
            }
        };
        if should_skip {
            return Ok(());
        }

        // should we stop?
        // We should if the the search range doesn't include any
        // element from the previous key or its successors
        let should_stop = {
            match self.right {
                Bound::Included(right) => right < previous_key.left_bound,
                Bound::Excluded(right) => right <= previous_key.left_bound,
                Bound::Unbounded => false,
            }
        };
        if should_stop {
            return Ok(());
        }
        // should we take the whole thing, without recursing down?
        let should_take_whole_group = {
            let left_condition = match self.left {
                Bound::Included(left) => previous_key.left_bound >= left,
                Bound::Excluded(left) => previous_key.left_bound > left,
                Bound::Unbounded => true,
            };
            let right_condition = match (self.right, rightmost_bound) {
                (Bound::Included(right), Bound::Included(rightmost)) => {
                    // we need to stay within the bound ..=right
                    // the element's range goes to ..=righmost
                    // so the element fits entirely within the bound if rightmost <= right
                    rightmost <= right
                }
                (Bound::Included(right), Bound::Excluded(rightmost)) => {
                    // we need to stay within the bound ..=right
                    // the element's range goes to ..righmost
                    // so the element fits entirely within the bound if rightmost <= right
                    rightmost <= right
                }
                (Bound::Excluded(right), Bound::Included(rightmost)) => {
                    // we need to stay within the bound ..right
                    // the element's range goes to ..=righmost
                    // so the element fits entirely within the bound if rightmost < right
                    rightmost < right
                }
                (Bound::Excluded(right), Bound::Excluded(rightmost)) => {
                    // we need to stay within the bound ..right
                    // the element's range goes to ..righmost
                    // so the element fits entirely within the bound if rightmost <= right
                    rightmost <= right
                }
                (Bound::Unbounded, _) => {
                    // we need to stay within the bound ..inf
                    // so the element always fits entirely within the bound
                    true
                }
                (_, Bound::Unbounded) => {
                    // we need to stay within a finite bound
                    // but the element's range goes to ..inf
                    // so the element never fits entirely within the bound
                    false
                }
            };
            left_condition && right_condition
        };
        if should_take_whole_group {
            *self.docids |= &previous_value.bitmap;
        } else {
            let level = level - 1;
            let starting_left_bound = previous_key.left_bound;
            let group_size = previous_value.size as usize;

            self.run(level, starting_left_bound, rightmost_bound, group_size)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use roaring::RoaringBitmap;

    use super::find_docids_of_facet_within_bounds;
    use crate::heed_codec::facet::{FacetGroupKeyCodec, OrderedF64Codec};
    use crate::milli_snap;
    use crate::search::facet::tests::{
        get_random_looking_index, get_random_looking_index_with_multiple_field_ids,
        get_simple_index, get_simple_index_with_multiple_field_ids,
    };
    use crate::snapshot_tests::display_bitmap;

    #[test]
    fn random_looking_index_snap() {
        let index = get_random_looking_index();
        milli_snap!(format!("{index}"), @"3256c76a7c1b768a013e78d5fa6e9ff9");
    }

    #[test]
    fn random_looking_index_with_multiple_field_ids_snap() {
        let index = get_random_looking_index_with_multiple_field_ids();
        milli_snap!(format!("{index}"), @"c3e5fe06a8f1c404ed4935b32c90a89b");
    }

    #[test]
    fn simple_index_snap() {
        let index = get_simple_index();
        milli_snap!(format!("{index}"), @"5dbfa134cc44abeb3ab6242fc182e48e");
    }

    #[test]
    fn simple_index_with_multiple_field_ids_snap() {
        let index = get_simple_index_with_multiple_field_ids();
        milli_snap!(format!("{index}"), @"a4893298218f682bc76357f46777448c");
    }

    #[test]
    fn filter_range_increasing() {
        let indexes = [
            get_simple_index(),
            get_random_looking_index(),
            get_simple_index_with_multiple_field_ids(),
            get_random_looking_index_with_multiple_field_ids(),
        ];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let mut results = String::new();
            for i in 0..=255 {
                let i = i as f64;
                let start = Bound::Included(0.);
                let end = Bound::Included(i);
                let mut docids = RoaringBitmap::new();
                find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                    &mut docids,
                )
                .unwrap();
                #[allow(clippy::format_push_string)]
                results.push_str(&format!("0 <= . <= {i} : {}\n", display_bitmap(&docids)));
            }
            milli_snap!(results, format!("included_{i}"));
            let mut results = String::new();
            for i in 0..=255 {
                let i = i as f64;
                let start = Bound::Excluded(0.);
                let end = Bound::Excluded(i);
                let mut docids = RoaringBitmap::new();
                find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                    &mut docids,
                )
                .unwrap();
                #[allow(clippy::format_push_string)]
                results.push_str(&format!("0 < . < {i} : {}\n", display_bitmap(&docids)));
            }
            milli_snap!(results, format!("excluded_{i}"));
            txn.commit().unwrap();
        }
    }
    #[test]
    fn filter_range_decreasing() {
        let indexes = [
            get_simple_index(),
            get_random_looking_index(),
            get_simple_index_with_multiple_field_ids(),
            get_random_looking_index_with_multiple_field_ids(),
        ];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();

            let mut results = String::new();

            for i in (0..=255).into_iter().rev() {
                let i = i as f64;
                let start = Bound::Included(i);
                let end = Bound::Included(255.);
                let mut docids = RoaringBitmap::new();
                find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                    &mut docids,
                )
                .unwrap();
                results.push_str(&format!("{i} <= . <= 255 : {}\n", display_bitmap(&docids)));
            }

            milli_snap!(results, format!("included_{i}"));

            let mut results = String::new();

            for i in (0..=255).into_iter().rev() {
                let i = i as f64;
                let start = Bound::Excluded(i);
                let end = Bound::Excluded(255.);
                let mut docids = RoaringBitmap::new();
                find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                    &mut docids,
                )
                .unwrap();
                results.push_str(&format!("{i} < . < 255 : {}\n", display_bitmap(&docids)));
            }

            milli_snap!(results, format!("excluded_{i}"));

            txn.commit().unwrap();
        }
    }
    #[test]
    fn filter_range_pinch() {
        let indexes = [
            get_simple_index(),
            get_random_looking_index(),
            get_simple_index_with_multiple_field_ids(),
            get_random_looking_index_with_multiple_field_ids(),
        ];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();

            let mut results = String::new();

            for i in (0..=128).into_iter().rev() {
                let i = i as f64;
                let start = Bound::Included(i);
                let end = Bound::Included(255. - i);
                let mut docids = RoaringBitmap::new();
                find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                    &mut docids,
                )
                .unwrap();
                results.push_str(&format!(
                    "{i} <= . <= {r} : {docids}\n",
                    r = 255. - i,
                    docids = display_bitmap(&docids)
                ));
            }

            milli_snap!(results, format!("included_{i}"));

            let mut results = String::new();

            for i in (0..=128).into_iter().rev() {
                let i = i as f64;
                let start = Bound::Excluded(i);
                let end = Bound::Excluded(255. - i);
                let mut docids = RoaringBitmap::new();
                find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                    &mut docids,
                )
                .unwrap();
                results.push_str(&format!(
                    "{i} <  . < {r} {docids}\n",
                    r = 255. - i,
                    docids = display_bitmap(&docids)
                ));
            }

            milli_snap!(results, format!("excluded_{i}"));

            txn.commit().unwrap();
        }
    }

    #[test]
    fn filter_range_unbounded() {
        let indexes = [
            get_simple_index(),
            get_random_looking_index(),
            get_simple_index_with_multiple_field_ids(),
            get_random_looking_index_with_multiple_field_ids(),
        ];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let mut results = String::new();
            for i in 0..=255 {
                let i = i as f64;
                let start = Bound::Included(i);
                let end = Bound::Unbounded;
                let mut docids = RoaringBitmap::new();
                find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                    &mut docids,
                )
                .unwrap();
                #[allow(clippy::format_push_string)]
                results.push_str(&format!(">= {i}: {}\n", display_bitmap(&docids)));
            }
            milli_snap!(results, format!("start_from_included_{i}"));
            let mut results = String::new();
            for i in 0..=255 {
                let i = i as f64;
                let start = Bound::Unbounded;
                let end = Bound::Included(i);
                let mut docids = RoaringBitmap::new();
                find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                    &mut docids,
                )
                .unwrap();
                #[allow(clippy::format_push_string)]
                results.push_str(&format!("<= {i}: {}\n", display_bitmap(&docids)));
            }
            milli_snap!(results, format!("end_at_included_{i}"));

            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &Bound::Unbounded,
                &Bound::Unbounded,
                &mut docids,
            )
            .unwrap();
            milli_snap!(
                &format!("all field_id 0: {}\n", display_bitmap(&docids)),
                format!("unbounded_field_id_0_{i}")
            );

            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                1,
                &Bound::Unbounded,
                &Bound::Unbounded,
                &mut docids,
            )
            .unwrap();
            milli_snap!(
                &format!("all field_id 1:  {}\n", display_bitmap(&docids)),
                format!("unbounded_field_id_1_{i}")
            );

            drop(txn);
        }
    }

    #[test]
    fn filter_range_exact() {
        let indexes = [
            get_simple_index(),
            get_random_looking_index(),
            get_simple_index_with_multiple_field_ids(),
            get_random_looking_index_with_multiple_field_ids(),
        ];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let mut results = String::new();
            for i in 0..=255 {
                let i = i as f64;
                let start = Bound::Included(i);
                let end = Bound::Included(i);
                let mut docids = RoaringBitmap::new();
                find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                    &mut docids,
                )
                .unwrap();
                #[allow(clippy::format_push_string)]
                results.push_str(&format!("{i}: {}\n", display_bitmap(&docids)));
            }
            milli_snap!(results, format!("exact_{i}"));

            drop(txn);
        }
    }
}

use std::ops::{Bound, RangeBounds};

use heed::BytesEncode;
use roaring::RoaringBitmap;

use super::{get_first_facet_value, get_highest_level, get_last_facet_value};
use crate::heed_codec::facet::new::{FacetGroupValueCodec, FacetKey, FacetKeyCodec, MyByteSlice};
use crate::Result;

pub fn find_docids_of_facet_within_bounds<'t, BoundCodec>(
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetKeyCodec<BoundCodec>, FacetGroupValueCodec>,
    field_id: u16,
    left: &'t Bound<<BoundCodec as BytesEncode<'t>>::EItem>,
    right: &'t Bound<<BoundCodec as BytesEncode<'t>>::EItem>,
) -> Result<RoaringBitmap>
where
    BoundCodec: for<'a> BytesEncode<'a>,
    for<'a> <BoundCodec as BytesEncode<'a>>::EItem: Sized,
{
    let inner;
    let left = match left {
        Bound::Included(left) => {
            inner = BoundCodec::bytes_encode(left).unwrap();
            Bound::Included(inner.as_ref())
        }
        Bound::Excluded(left) => {
            inner = BoundCodec::bytes_encode(left).unwrap();
            Bound::Excluded(inner.as_ref())
        }
        Bound::Unbounded => Bound::Unbounded,
    };
    let inner;
    let right = match right {
        Bound::Included(right) => {
            inner = BoundCodec::bytes_encode(right).unwrap();
            Bound::Included(inner.as_ref())
        }
        Bound::Excluded(right) => {
            inner = BoundCodec::bytes_encode(right).unwrap();
            Bound::Excluded(inner.as_ref())
        }
        Bound::Unbounded => Bound::Unbounded,
    };
    let db = db.remap_key_type::<FacetKeyCodec<MyByteSlice>>();
    let mut docids = RoaringBitmap::new();
    let mut f = FacetRangeSearch { rtxn, db, field_id, left, right, docids: &mut docids };
    let highest_level = get_highest_level(rtxn, db, field_id)?;

    if let Some(first_bound) = get_first_facet_value::<MyByteSlice>(rtxn, db, field_id)? {
        let last_bound = get_last_facet_value::<MyByteSlice>(rtxn, db, field_id)?.unwrap();
        f.run(highest_level, first_bound, Bound::Included(last_bound), usize::MAX)?;
        Ok(docids)
    } else {
        return Ok(RoaringBitmap::new());
    }
}

/// Fetch the document ids that have a facet with a value between the two given bounds
struct FacetRangeSearch<'t, 'b, 'bitmap> {
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
    left: Bound<&'b [u8]>,
    right: Bound<&'b [u8]>,
    docids: &'bitmap mut RoaringBitmap,
}
impl<'t, 'b, 'bitmap> FacetRangeSearch<'t, 'b, 'bitmap> {
    fn run_level_0(&mut self, starting_left_bound: &'t [u8], group_size: usize) -> Result<()> {
        let left_key =
            FacetKey { field_id: self.field_id, level: 0, left_bound: starting_left_bound };
        let iter = self.db.range(&self.rtxn, &(left_key..))?.take(group_size);
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

    /// Recursive part of the algorithm for level > 0
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

        let left_key = FacetKey { field_id: self.field_id, level, left_bound: starting_left_bound };
        let mut iter = self.db.range(&self.rtxn, &(left_key..))?.take(group_size);

        let (mut previous_key, mut previous_value) = iter.next().unwrap()?;
        for el in iter {
            let (next_key, next_value) = el?;
            // the right of the iter range is unbounded, so we need to make sure that we are not iterating
            // on the next field id
            if next_key.field_id != self.field_id {
                return Ok(());
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

            let level = level - 1;
            let starting_left_bound = previous_key.left_bound;
            let rightmost_bound = Bound::Excluded(next_key.left_bound);
            let group_size = previous_value.size as usize;

            self.run(level, starting_left_bound, rightmost_bound, group_size)?;

            previous_key = next_key;
            previous_value = next_value;
        }
        // previous_key/previous_value are the last element

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
        let should_stop = {
            match self.right {
                Bound::Included(right) => right <= previous_key.left_bound,
                Bound::Excluded(right) => right < previous_key.left_bound,
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
                (Bound::Included(right), Bound::Included(rightmost)) => rightmost <= right,
                (Bound::Included(right), Bound::Excluded(rightmost)) => rightmost < right,
                // e.g. x < 8 and rightmost is <= y
                // condition met if rightmost < 8
                (Bound::Excluded(right), Bound::Included(rightmost)) => rightmost < right,
                // e.g. x < 8 and rightmost is < y
                // condition met only if y <= 8?
                (Bound::Excluded(right), Bound::Excluded(rightmost)) => rightmost <= right,
                // e.g. x < inf. , so yes we take the whole thing
                (Bound::Unbounded, _) => true,
                // e.g. x < 7 , righmost is inf
                (_, Bound::Unbounded) => false, // panic?
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

    use rand::{Rng, SeedableRng};
    use roaring::RoaringBitmap;

    use super::find_docids_of_facet_within_bounds;
    use crate::heed_codec::facet::new::ordered_f64_codec::OrderedF64Codec;
    use crate::heed_codec::facet::new::FacetKeyCodec;
    use crate::milli_snap;
    use crate::search::facet::test::FacetIndex;
    use crate::snapshot_tests::display_bitmap;

    fn get_simple_index() -> FacetIndex<OrderedF64Codec> {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
        let mut txn = index.env.write_txn().unwrap();
        for i in 0..256u16 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
        }
        txn.commit().unwrap();
        index
    }
    fn get_random_looking_index() -> FacetIndex<OrderedF64Codec> {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
        let mut txn = index.env.write_txn().unwrap();

        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        let keys =
            std::iter::from_fn(|| Some(rng.gen_range(0..256))).take(128).collect::<Vec<u32>>();

        for (_i, key) in keys.into_iter().enumerate() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(key + 100);
            index.insert(&mut txn, 0, &(key as f64), &bitmap);
        }
        txn.commit().unwrap();
        index
    }

    #[test]
    fn random_looking_index_snap() {
        let index = get_random_looking_index();
        milli_snap!(format!("{index}"));
    }
    #[test]
    fn filter_range_increasing() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let mut results = String::new();
            for i in 0..=255 {
                let i = i as f64;
                let start = Bound::Included(0.);
                let end = Bound::Included(i);
                let docids = find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.db.content.remap_key_type::<FacetKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                )
                .unwrap();
                results.push_str(&format!("{}\n", display_bitmap(&docids)));
            }
            milli_snap!(results, format!("included_{i}"));
            let mut results = String::new();
            for i in 0..=255 {
                let i = i as f64;
                let start = Bound::Excluded(0.);
                let end = Bound::Excluded(i);
                let docids = find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.db.content.remap_key_type::<FacetKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                )
                .unwrap();
                results.push_str(&format!("{}\n", display_bitmap(&docids)));
            }
            milli_snap!(results, format!("excluded_{i}"));
            txn.commit().unwrap();
        }
    }
    #[test]
    fn filter_range_decreasing() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();

            let mut results = String::new();

            for i in (0..=255).into_iter().rev() {
                let i = i as f64;
                let start = Bound::Included(i);
                let end = Bound::Included(255.);
                let docids = find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.db.content.remap_key_type::<FacetKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                )
                .unwrap();
                results.push_str(&format!("{}\n", display_bitmap(&docids)));
            }

            milli_snap!(results, format!("included_{i}"));

            let mut results = String::new();

            for i in (0..=255).into_iter().rev() {
                let i = i as f64;
                let start = Bound::Excluded(i);
                let end = Bound::Excluded(255.);
                let docids = find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.db.content.remap_key_type::<FacetKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                )
                .unwrap();
                results.push_str(&format!("{}\n", display_bitmap(&docids)));
            }

            milli_snap!(results, format!("excluded_{i}"));

            txn.commit().unwrap();
        }
    }
    #[test]
    fn filter_range_pinch() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();

            let mut results = String::new();

            for i in (0..=128).into_iter().rev() {
                let i = i as f64;
                let start = Bound::Included(i);
                let end = Bound::Included(255. - i);
                let docids = find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.db.content.remap_key_type::<FacetKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                )
                .unwrap();
                results.push_str(&format!("{}\n", display_bitmap(&docids)));
            }

            milli_snap!(results, format!("included_{i}"));

            let mut results = String::new();

            for i in (0..=128).into_iter().rev() {
                let i = i as f64;
                let start = Bound::Excluded(i);
                let end = Bound::Excluded(255. - i);
                let docids = find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                    &txn,
                    index.db.content.remap_key_type::<FacetKeyCodec<OrderedF64Codec>>(),
                    0,
                    &start,
                    &end,
                )
                .unwrap();
                results.push_str(&format!("{}\n", display_bitmap(&docids)));
            }

            milli_snap!(results, format!("excluded_{i}"));

            txn.commit().unwrap();
        }
    }
}

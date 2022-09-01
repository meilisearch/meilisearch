use crate::heed_codec::facet::new::{
    FacetGroupValue, FacetGroupValueCodec, FacetKey, FacetKeyCodec, MyByteSlice,
};
use heed::Result;
use roaring::RoaringBitmap;

use super::{get_first_facet_value, get_highest_level};

pub fn ascending_facet_sort<'t>(
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
    candidates: RoaringBitmap,
) -> Result<Box<dyn Iterator<Item = Result<RoaringBitmap>> + 't>> {
    let highest_level = get_highest_level(rtxn, db, field_id)?;
    if let Some(first_bound) = get_first_facet_value::<MyByteSlice>(rtxn, db, field_id)? {
        let first_key = FacetKey { field_id, level: highest_level, left_bound: first_bound };
        let iter = db.range(rtxn, &(first_key..)).unwrap().take(usize::MAX);

        Ok(Box::new(AscendingFacetSort { rtxn, db, field_id, stack: vec![(candidates, iter)] }))
    } else {
        Ok(Box::new(std::iter::empty()))
    }
}

struct AscendingFacetSort<'t, 'e> {
    rtxn: &'t heed::RoTxn<'e>,
    db: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
    stack: Vec<(
        RoaringBitmap,
        std::iter::Take<heed::RoRange<'t, FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>>,
    )>,
}

impl<'t, 'e> Iterator for AscendingFacetSort<'t, 'e> {
    type Item = Result<RoaringBitmap>;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            let (documents_ids, deepest_iter) = self.stack.last_mut()?;
            for result in deepest_iter {
                let (
                    FacetKey { level, left_bound, field_id },
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
                        FacetKey { field_id: self.field_id, level: level - 1, left_bound };
                    let iter = match self.db.range(&self.rtxn, &(starting_key_below..)) {
                        Ok(iter) => iter,
                        Err(e) => return Some(Err(e.into())),
                    }
                    .take(group_size as usize);

                    self.stack.push((bitmap, iter));
                    continue 'outer;
                }
            }
            self.stack.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::milli_snap;
    use crate::{
        heed_codec::facet::new::ordered_f64_codec::OrderedF64Codec,
        search::facet::{facet_sort_ascending::ascending_facet_sort, test::FacetIndex},
        snapshot_tests::display_bitmap,
    };
    use rand::Rng;
    use rand::SeedableRng;
    use roaring::RoaringBitmap;

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
    fn filter_sort() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = (200..=300).into_iter().collect::<RoaringBitmap>();
            let mut results = String::new();
            let iter = ascending_facet_sort(&txn, index.db.content, 0, candidates).unwrap();
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

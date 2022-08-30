use roaring::RoaringBitmap;

use crate::heed_codec::facet::new::{
    FacetGroupValue, FacetGroupValueCodec, FacetKey, FacetKeyCodec, MyByteSlice,
};

use super::{get_first_facet_value, get_highest_level};

pub fn ascending_facet_sort<'t>(
    rtxn: &'t heed::RoTxn<'t>,
    db: &'t heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
    candidates: RoaringBitmap,
) -> Box<dyn Iterator<Item = (&'t [u8], RoaringBitmap)> + 't> {
    let highest_level =
        get_highest_level(rtxn, &db.remap_key_type::<FacetKeyCodec<MyByteSlice>>(), field_id);
    if let Some(first_bound) = get_first_facet_value::<MyByteSlice>(
        rtxn,
        &db.remap_key_type::<FacetKeyCodec<MyByteSlice>>(),
        field_id,
    ) {
        let first_key = FacetKey { field_id, level: highest_level, left_bound: first_bound };
        let iter = db.range(rtxn, &(first_key..)).unwrap().take(usize::MAX);

        Box::new(AscendingFacetSort { rtxn, db, field_id, stack: vec![(candidates, iter)] })
    } else {
        return Box::new(std::iter::empty());
    }
}

struct AscendingFacetSort<'t, 'e> {
    rtxn: &'t heed::RoTxn<'e>,
    db: &'t heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
    stack: Vec<(
        RoaringBitmap,
        std::iter::Take<heed::RoRange<'t, FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>>,
    )>,
}

impl<'t, 'e> Iterator for AscendingFacetSort<'t, 'e> {
    type Item = (&'t [u8], RoaringBitmap);

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
                        return Some((left_bound, bitmap));
                    }
                    let starting_key_below =
                        FacetKey { field_id: self.field_id, level: level - 1, left_bound };
                    let iter = self
                        .db
                        .range(&self.rtxn, &(starting_key_below..))
                        .unwrap()
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
    use crate::{
        ascending_facet_sort::ascending_facet_sort, codec::U16Codec, display_bitmap, Index,
    };
    use heed::BytesDecode;
    use roaring::RoaringBitmap;

    fn get_simple_index() -> Index<U16Codec> {
        let index = Index::<U16Codec>::new(4, 8);
        let mut txn = index.env.write_txn().unwrap();
        for i in 0..256u16 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            index.insert(&mut txn, 0, &i, &bitmap);
        }
        txn.commit().unwrap();
        index
    }
    fn get_random_looking_index() -> Index<U16Codec> {
        let index = Index::<U16Codec>::new(4, 8);
        let mut txn = index.env.write_txn().unwrap();

        let rng = fastrand::Rng::with_seed(0);
        let keys = std::iter::from_fn(|| Some(rng.u32(..256))).take(128).collect::<Vec<u32>>();

        for (_i, key) in keys.into_iter().enumerate() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(key + 100);
            index.insert(&mut txn, 0, &(key as u16), &bitmap);
        }
        txn.commit().unwrap();
        index
    }

    #[test]
    fn random_looking_index_snap() {
        let index = get_random_looking_index();
        insta::assert_display_snapshot!(index)
    }
    #[test]
    fn filter_sort() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.into_iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = (200..=300).into_iter().collect::<RoaringBitmap>();
            let mut results = String::new();
            let iter = ascending_facet_sort(&txn, &index.db.content, 0, candidates);
            for (facet, docids) in iter {
                let facet = U16Codec::bytes_decode(facet).unwrap();
                results.push_str(&format!("{facet}: {}\n", display_bitmap(&docids)));
            }
            insta::assert_snapshot!(format!("filter_sort_{i}_ascending"), results);

            txn.commit().unwrap();
        }
    }
}

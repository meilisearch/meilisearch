use crate::heed_codec::facet::new::{FacetGroupValueCodec, FacetKey, FacetKeyCodec, MyByteSlice};
use heed::Result;
use roaring::RoaringBitmap;
use std::ops::ControlFlow;

use super::{get_first_facet_value, get_highest_level};

pub fn iterate_over_facet_distribution<'t, CB>(
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
    candidates: &RoaringBitmap,
    callback: CB,
) -> Result<()>
where
    CB: FnMut(&'t [u8], u64) -> ControlFlow<()>,
{
    let mut fd = FacetDistribution { rtxn, db, field_id, callback };
    let highest_level =
        get_highest_level(rtxn, db.remap_key_type::<FacetKeyCodec<MyByteSlice>>(), field_id)?;

    if let Some(first_bound) = get_first_facet_value::<MyByteSlice>(rtxn, db, field_id)? {
        fd.iterate(candidates, highest_level, first_bound, usize::MAX)?;
        return Ok(());
    } else {
        return Ok(());
    }
}

struct FacetDistribution<'t, CB>
where
    CB: FnMut(&'t [u8], u64) -> ControlFlow<()>,
{
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
    callback: CB,
}

impl<'t, CB> FacetDistribution<'t, CB>
where
    CB: FnMut(&'t [u8], u64) -> ControlFlow<()>,
{
    fn iterate_level_0(
        &mut self,
        candidates: &RoaringBitmap,
        starting_bound: &'t [u8],
        group_size: usize,
    ) -> Result<ControlFlow<()>> {
        let starting_key =
            FacetKey { field_id: self.field_id, level: 0, left_bound: starting_bound };
        let iter = self.db.range(self.rtxn, &(starting_key..))?.take(group_size);
        for el in iter {
            let (key, value) = el?;
            // The range is unbounded on the right and the group size for the highest level is MAX,
            // so we need to check that we are not iterating over the next field id
            if key.field_id != self.field_id {
                return Ok(ControlFlow::Break(()));
            }
            let docids_in_common = value.bitmap.intersection_len(candidates);
            if docids_in_common > 0 {
                match (self.callback)(key.left_bound, docids_in_common) {
                    ControlFlow::Continue(_) => {}
                    ControlFlow::Break(_) => return Ok(ControlFlow::Break(())),
                }
            }
        }
        return Ok(ControlFlow::Continue(()));
    }
    fn iterate(
        &mut self,
        candidates: &RoaringBitmap,
        level: u8,
        starting_bound: &'t [u8],
        group_size: usize,
    ) -> Result<ControlFlow<()>> {
        if level == 0 {
            return self.iterate_level_0(candidates, starting_bound, group_size);
        }
        let starting_key = FacetKey { field_id: self.field_id, level, left_bound: starting_bound };
        let iter = self.db.range(&self.rtxn, &(&starting_key..)).unwrap().take(group_size);

        for el in iter {
            let (key, value) = el.unwrap();
            // The range is unbounded on the right and the group size for the highest level is MAX,
            // so we need to check that we are not iterating over the next field id
            if key.field_id != self.field_id {
                return Ok(ControlFlow::Break(()));
            }
            let docids_in_common = value.bitmap & candidates;
            if docids_in_common.len() > 0 {
                let cf = self.iterate(
                    &docids_in_common,
                    level - 1,
                    key.left_bound,
                    value.size as usize,
                )?;
                match cf {
                    ControlFlow::Continue(_) => {}
                    ControlFlow::Break(_) => return Ok(ControlFlow::Break(())),
                }
            }
        }

        return Ok(ControlFlow::Continue(()));
    }
}

#[cfg(test)]
mod tests {
    use heed::BytesDecode;
    use rand::{rngs::SmallRng, Rng, SeedableRng};
    use roaring::RoaringBitmap;
    use std::ops::ControlFlow;

    use crate::{
        heed_codec::facet::new::ordered_f64_codec::OrderedF64Codec, search::facet::test::FacetIndex,
    };

    use super::iterate_over_facet_distribution;

    fn get_simple_index() -> FacetIndex<OrderedF64Codec> {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
        let mut txn = index.env.write_txn().unwrap();
        for i in 0..256u16 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            index.insert(&mut txn, 0, &i, &bitmap);
        }
        txn.commit().unwrap();
        index
    }
    fn get_random_looking_index() -> FacetIndex<OrderedF64Codec> {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
        let mut txn = index.env.write_txn().unwrap();

        let rng = rand::rngs::SmallRng::from_seed([0; 32]);
        let keys =
            std::iter::from_fn(|| Some(rng.gen_range(0..256))).take(128).collect::<Vec<u32>>();

        for (_i, key) in keys.into_iter().enumerate() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(key + 100.);
            index.insert(&mut txn, 0, &(key as f64), &bitmap);
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
    fn filter_distribution_all() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.into_iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = (0..=255).into_iter().collect::<RoaringBitmap>();
            let mut results = String::new();
            iterate_over_facet_distribution(
                &txn,
                &index.db.content,
                0,
                &candidates,
                |facet, count| {
                    let facet = OrderedF64Codec::bytes_decode(facet).unwrap();
                    results.push_str(&format!("{facet}: {count}\n"));
                    ControlFlow::Continue(())
                },
            );
            insta::assert_snapshot!(format!("filter_distribution_{i}_all"), results);

            txn.commit().unwrap();
        }
    }
    #[test]
    fn filter_distribution_all_stop_early() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.into_iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = (0..=255).into_iter().collect::<RoaringBitmap>();
            let mut results = String::new();
            let mut nbr_facets = 0;
            iterate_over_facet_distribution(
                &txn,
                &index.db.content,
                0,
                &candidates,
                |facet, count| {
                    let facet = OrderedF64Codec::bytes_decode(facet).unwrap();
                    if nbr_facets == 100 {
                        return ControlFlow::Break(());
                    } else {
                        nbr_facets += 1;
                        results.push_str(&format!("{facet}: {count}\n"));

                        ControlFlow::Continue(())
                    }
                },
            );
            insta::assert_snapshot!(format!("filter_distribution_{i}_all_stop_early"), results);

            txn.commit().unwrap();
        }
    }
}

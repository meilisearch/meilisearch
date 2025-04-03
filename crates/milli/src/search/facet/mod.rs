pub use facet_sort_ascending::ascending_facet_sort;
pub use facet_sort_descending::descending_facet_sort;
use heed::types::{Bytes, DecodeIgnore};
use heed::{BytesDecode, RoTxn};
use roaring::RoaringBitmap;

pub use self::facet_distribution::{FacetDistribution, OrderBy, DEFAULT_VALUES_PER_FACET};
pub use self::filter::{BadGeoError, Filter};
pub use self::search::{FacetValueHit, SearchForFacetValues};
use crate::heed_codec::facet::{FacetGroupKeyCodec, OrderedF64Codec};
use crate::heed_codec::BytesRefCodec;
use crate::{Index, Result};

mod facet_distribution;
mod facet_distribution_iter;
mod facet_range_search;
mod facet_sort_ascending;
mod facet_sort_descending;
mod filter;
mod search;

fn facet_extreme_value<'t>(
    mut extreme_it: impl Iterator<Item = heed::Result<(RoaringBitmap, &'t [u8])>> + 't,
) -> Result<Option<f64>> {
    let extreme_value =
        match extreme_it.next() { Some(extreme_value) => { extreme_value } _ => { return Ok(None) }};
    let (_, extreme_value) = extreme_value?;
    OrderedF64Codec::bytes_decode(extreme_value)
        .map(Some)
        .map_err(heed::Error::Decoding)
        .map_err(Into::into)
}

pub fn facet_min_value<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn<'t>,
    field_id: u16,
    candidates: RoaringBitmap,
) -> Result<Option<f64>> {
    let db = index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
    let it = ascending_facet_sort(rtxn, db, field_id, candidates)?;
    facet_extreme_value(it)
}

pub fn facet_max_value<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn<'t>,
    field_id: u16,
    candidates: RoaringBitmap,
) -> Result<Option<f64>> {
    let db = index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
    let it = descending_facet_sort(rtxn, db, field_id, candidates)?;
    facet_extreme_value(it)
}

/// Get the first facet value in the facet database
pub(crate) fn get_first_facet_value<'t, BoundCodec, DC>(
    txn: &'t RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, DC>,
    field_id: u16,
) -> heed::Result<Option<BoundCodec::DItem>>
where
    BoundCodec: BytesDecode<'t>,
{
    let mut level0prefix = vec![];
    level0prefix.extend_from_slice(&field_id.to_be_bytes());
    level0prefix.push(0);
    let mut level0_iter_forward =
        db.remap_types::<Bytes, DecodeIgnore>().prefix_iter(txn, level0prefix.as_slice())?;
    match level0_iter_forward.next() { Some(first) => {
        let (first_key, _) = first?;
        let first_key = FacetGroupKeyCodec::<BoundCodec>::bytes_decode(first_key)
            .map_err(heed::Error::Decoding)?;
        Ok(Some(first_key.left_bound))
    } _ => {
        Ok(None)
    }}
}

/// Get the last facet value in the facet database
pub(crate) fn get_last_facet_value<'t, BoundCodec, DC>(
    txn: &'t RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, DC>,
    field_id: u16,
) -> heed::Result<Option<BoundCodec::DItem>>
where
    BoundCodec: BytesDecode<'t>,
{
    let mut level0prefix = vec![];
    level0prefix.extend_from_slice(&field_id.to_be_bytes());
    level0prefix.push(0);
    let mut level0_iter_backward =
        db.remap_types::<Bytes, DecodeIgnore>().rev_prefix_iter(txn, level0prefix.as_slice())?;
    match level0_iter_backward.next() { Some(last) => {
        let (last_key, _) = last?;
        let last_key = FacetGroupKeyCodec::<BoundCodec>::bytes_decode(last_key)
            .map_err(heed::Error::Decoding)?;
        Ok(Some(last_key.left_bound))
    } _ => {
        Ok(None)
    }}
}

/// Get the height of the highest level in the facet database
pub(crate) fn get_highest_level<'t, DC>(
    txn: &'t RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, DC>,
    field_id: u16,
) -> heed::Result<u8> {
    let field_id_prefix = &field_id.to_be_bytes();
    Ok(db
        .remap_types::<Bytes, DecodeIgnore>()
        .rev_prefix_iter(txn, field_id_prefix)?
        .next()
        .map(|el| {
            let (key, _) = el.unwrap();
            let key = FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(key).unwrap();
            key.level
        })
        .unwrap_or(0))
}

#[cfg(test)]
pub(crate) mod tests {
    use rand::{Rng, SeedableRng};
    use roaring::RoaringBitmap;

    use crate::heed_codec::facet::OrderedF64Codec;
    use crate::heed_codec::StrRefCodec;
    use crate::update::facet::test_helpers::FacetIndex;

    pub fn get_simple_index() -> FacetIndex<OrderedF64Codec> {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();
        for i in 0..256u16 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
        }
        txn.commit().unwrap();
        index
    }
    pub fn get_random_looking_index() -> FacetIndex<OrderedF64Codec> {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);

        for key in std::iter::from_fn(|| Some(rng.gen_range(0..256))).take(128) {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(key + 100);
            index.insert(&mut txn, 0, &(key as f64), &bitmap);
        }
        txn.commit().unwrap();
        index
    }
    pub fn get_simple_index_with_multiple_field_ids() -> FacetIndex<OrderedF64Codec> {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();
        for fid in 0..2 {
            for i in 0..256u16 {
                let mut bitmap = RoaringBitmap::new();
                bitmap.insert(i as u32);
                index.insert(&mut txn, fid, &(i as f64), &bitmap);
            }
        }
        txn.commit().unwrap();
        index
    }
    pub fn get_random_looking_index_with_multiple_field_ids() -> FacetIndex<OrderedF64Codec> {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();

        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        let keys =
            std::iter::from_fn(|| Some(rng.gen_range(0..256))).take(128).collect::<Vec<u32>>();
        for fid in 0..2 {
            for &key in &keys {
                let mut bitmap = RoaringBitmap::new();
                bitmap.insert(key);
                bitmap.insert(key + 100);
                index.insert(&mut txn, fid, &(key as f64), &bitmap);
            }
        }
        txn.commit().unwrap();
        index
    }
    pub fn get_simple_string_index_with_multiple_field_ids() -> FacetIndex<StrRefCodec> {
        let index = FacetIndex::<StrRefCodec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();
        for fid in 0..2 {
            for i in 0..256u16 {
                let mut bitmap = RoaringBitmap::new();
                bitmap.insert(i as u32);
                if i % 2 == 0 {
                    index.insert(&mut txn, fid, &format!("{i}").as_str(), &bitmap);
                } else {
                    index.insert(&mut txn, fid, &"", &bitmap);
                }
            }
        }
        txn.commit().unwrap();
        index
    }
    pub fn get_random_looking_string_index_with_multiple_field_ids() -> FacetIndex<StrRefCodec> {
        let index = FacetIndex::<StrRefCodec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();

        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        let keys =
            std::iter::from_fn(|| Some(rng.gen_range(0..256))).take(128).collect::<Vec<u32>>();
        for fid in 0..2 {
            for &key in &keys {
                let mut bitmap = RoaringBitmap::new();
                bitmap.insert(key);
                bitmap.insert(key + 100);
                if key % 2 == 0 {
                    index.insert(&mut txn, fid, &format!("{key}").as_str(), &bitmap);
                } else {
                    index.insert(&mut txn, fid, &"", &bitmap);
                }
            }
        }
        txn.commit().unwrap();
        index
    }
}

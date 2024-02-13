pub use facet_sort_ascending::ascending_facet_sort;
pub use facet_sort_descending::descending_facet_sort;
use heed::types::{Bytes, DecodeIgnore};
use heed::{BytesDecode, RoTxn};
use roaring::RoaringBitmap;

pub use self::facet_distribution::{FacetDistribution, OrderBy, DEFAULT_VALUES_PER_FACET};
pub use self::filter::{BadGeoError, Filter};
use crate::heed_codec::facet::{FacetGroupKeyCodec, FacetGroupValueCodec, OrderedF64Codec};
use crate::heed_codec::BytesRefCodec;
use crate::{Index, Result};
mod facet_distribution;
mod facet_distribution_iter;
mod facet_range_search;
mod facet_sort_ascending;
mod facet_sort_descending;
mod filter;

fn facet_extreme_value<'t>(
    mut extreme_it: impl Iterator<Item = heed::Result<(RoaringBitmap, &'t [u8])>> + 't,
) -> Result<Option<f64>> {
    let extreme_value =
        if let Some(extreme_value) = extreme_it.next() { extreme_value } else { return Ok(None) };
    let (_, extreme_value) = extreme_value?;
    OrderedF64Codec::bytes_decode(extreme_value)
        .map(Some)
        .map_err(heed::Error::Decoding)
        .map_err(Into::into)
}

pub fn facet_min_value<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: u16,
    candidates: RoaringBitmap,
) -> Result<Option<f64>> {
    let db = index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
    let it = ascending_facet_sort(rtxn, db, field_id, candidates)?;
    facet_extreme_value(it)
}

pub fn facet_max_value<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: u16,
    candidates: RoaringBitmap,
) -> Result<Option<f64>> {
    let db = index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
    let it = descending_facet_sort(rtxn, db, field_id, candidates)?;
    facet_extreme_value(it)
}

/// Get the first facet value in the facet database
pub(crate) fn get_first_facet_value<'t, BoundCodec>(
    txn: &'t RoTxn,
    db: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
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
    if let Some(first) = level0_iter_forward.next() {
        let (first_key, _) = first?;
        let first_key = FacetGroupKeyCodec::<BoundCodec>::bytes_decode(first_key)
            .map_err(heed::Error::Decoding)?;
        Ok(Some(first_key.left_bound))
    } else {
        Ok(None)
    }
}

/// Get the last facet value in the facet database
pub(crate) fn get_last_facet_value<'t, BoundCodec>(
    txn: &'t RoTxn,
    db: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
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
    if let Some(last) = level0_iter_backward.next() {
        let (last_key, _) = last?;
        let last_key = FacetGroupKeyCodec::<BoundCodec>::bytes_decode(last_key)
            .map_err(heed::Error::Decoding)?;
        Ok(Some(last_key.left_bound))
    } else {
        Ok(None)
    }
}

/// Get the height of the highest level in the facet database
pub(crate) fn get_highest_level<'t>(
    txn: &'t RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
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

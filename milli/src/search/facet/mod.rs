use heed::types::ByteSlice;
use heed::{BytesDecode, RoTxn};

use crate::heed_codec::facet::new::{FacetGroupValueCodec, FacetKeyCodec, MyByteSlice};

pub use self::facet_distribution::{FacetDistribution, DEFAULT_VALUES_PER_FACET};
// pub use self::facet_number::{FacetNumberIter, FacetNumberRange, FacetNumberRevRange};
// pub use self::facet_string::FacetStringIter;
pub use self::filter::Filter;

mod facet_distribution;
mod facet_distribution_iter;
mod facet_sort_ascending;
mod facet_sort_descending;
mod filter;

fn get_first_facet_value<'t, BoundCodec>(
    txn: &'t RoTxn,
    db: &'t heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
) -> Option<BoundCodec::DItem>
where
    BoundCodec: BytesDecode<'t>,
{
    let mut level0prefix = vec![];
    level0prefix.extend_from_slice(&field_id.to_be_bytes());
    level0prefix.push(0);
    let mut level0_iter_forward = db
        .as_polymorph()
        .prefix_iter::<_, ByteSlice, ByteSlice>(txn, level0prefix.as_slice())
        .unwrap();
    if let Some(first) = level0_iter_forward.next() {
        let (first_key, _) = first.unwrap();
        let first_key = FacetKeyCodec::<BoundCodec>::bytes_decode(first_key).unwrap();
        Some(first_key.left_bound)
    } else {
        None
    }
}
fn get_last_facet_value<'t, BoundCodec>(
    txn: &'t RoTxn,
    db: &'t heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
) -> Option<BoundCodec::DItem>
where
    BoundCodec: BytesDecode<'t>,
{
    let mut level0prefix = vec![];
    level0prefix.extend_from_slice(&field_id.to_be_bytes());
    level0prefix.push(0);
    let mut level0_iter_backward = db
        .as_polymorph()
        .rev_prefix_iter::<_, ByteSlice, ByteSlice>(txn, level0prefix.as_slice())
        .unwrap();
    if let Some(last) = level0_iter_backward.next() {
        let (last_key, _) = last.unwrap();
        let last_key = FacetKeyCodec::<BoundCodec>::bytes_decode(last_key).unwrap();
        Some(last_key.left_bound)
    } else {
        None
    }
}
fn get_highest_level<'t>(
    txn: &'t RoTxn<'t>,
    db: &'t heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
) -> u8 {
    let field_id_prefix = &field_id.to_be_bytes();
    db.as_polymorph()
        .rev_prefix_iter::<_, ByteSlice, ByteSlice>(&txn, field_id_prefix)
        .unwrap()
        .next()
        .map(|el| {
            let (key, _) = el.unwrap();
            let key = FacetKeyCodec::<MyByteSlice>::bytes_decode(key).unwrap();
            key.level
        })
        .unwrap_or(0)
}

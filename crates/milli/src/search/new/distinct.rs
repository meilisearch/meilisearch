use heed::types::{Bytes, Str, Unit};
use heed::{Database, RoPrefix, RoTxn};
use roaring::RoaringBitmap;

const FID_SIZE: usize = 2;
const DOCID_SIZE: usize = 4;

use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec, FieldDocIdFacetCodec,
};
use crate::heed_codec::BytesRefCodec;
use crate::{Index, Result, SearchContext};

pub struct DistinctOutput {
    pub remaining: RoaringBitmap,
    pub excluded: RoaringBitmap,
}

/// Return a [`DistinctOutput`] containing:
/// - `remaining`: a set of docids built such that exactly one element from `candidates`
///   is kept for each distinct value inside the given field. If the field does not exist, it
///   is considered unique.
/// - `excluded`: the set of document ids that contain a value for the given field that occurs
///   in the given candidates.
pub fn apply_distinct_rule(
    ctx: &mut SearchContext<'_>,
    field_id: u16,
    candidates: &RoaringBitmap,
) -> Result<DistinctOutput> {
    let mut excluded = RoaringBitmap::new();
    let mut remaining = RoaringBitmap::new();
    for docid in candidates {
        if excluded.contains(docid) {
            continue;
        }
        distinct_single_docid(ctx.index, ctx.txn, field_id, docid, &mut excluded)?;
        remaining.push(docid);
    }
    Ok(DistinctOutput { remaining, excluded })
}

/// Apply the distinct rule defined by [`apply_distinct_rule`] for a single document id.
pub fn distinct_single_docid(
    index: &Index,
    txn: &RoTxn<'_>,
    field_id: u16,
    docid: u32,
    excluded: &mut RoaringBitmap,
) -> Result<()> {
    for item in facet_string_values(docid, field_id, index, txn)? {
        let ((_, _, facet_value), _) = item?;
        if let Some(facet_docids) = facet_value_docids(
            index.facet_id_string_docids.remap_types(),
            txn,
            field_id,
            facet_value,
        )? {
            *excluded |= facet_docids;
        }
    }
    for item in facet_number_values(docid, field_id, index, txn)? {
        let ((_, _, facet_value), _) = item?;
        if let Some(facet_docids) =
            facet_value_docids(index.facet_id_f64_docids.remap_types(), txn, field_id, facet_value)?
        {
            *excluded |= facet_docids;
        }
    }
    Ok(())
}

/// Return all the docids containing the given value in the given field
fn facet_value_docids(
    database: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    txn: &RoTxn<'_>,
    field_id: u16,
    facet_value: &[u8],
) -> heed::Result<Option<RoaringBitmap>> {
    database
        .get(txn, &FacetGroupKey { field_id, level: 0, left_bound: facet_value })
        .map(|opt| opt.map(|v| v.bitmap))
}

/// Return an iterator over each number value in the given field of the given document.
fn facet_number_values<'a>(
    docid: u32,
    field_id: u16,
    index: &Index,
    txn: &'a RoTxn<'a>,
) -> Result<RoPrefix<'a, FieldDocIdFacetCodec<BytesRefCodec>, Unit>> {
    let key = facet_values_prefix_key(field_id, docid);

    let iter = index
        .field_id_docid_facet_f64s
        .remap_key_type::<Bytes>()
        .prefix_iter(txn, &key)?
        .remap_key_type();

    Ok(iter)
}

/// Return an iterator over each string value in the given field of the given document.
pub fn facet_string_values<'a>(
    docid: u32,
    field_id: u16,
    index: &Index,
    txn: &'a RoTxn<'a>,
) -> Result<RoPrefix<'a, FieldDocIdFacetCodec<BytesRefCodec>, Str>> {
    let key = facet_values_prefix_key(field_id, docid);

    let iter = index
        .field_id_docid_facet_strings
        .remap_key_type::<Bytes>()
        .prefix_iter(txn, &key)?
        .remap_types();

    Ok(iter)
}

#[allow(clippy::drop_non_drop)]
fn facet_values_prefix_key(distinct: u16, id: u32) -> [u8; FID_SIZE + DOCID_SIZE] {
    concat_arrays::concat_arrays!(distinct.to_be_bytes(), id.to_be_bytes())
}

use heed::{
    types::{ByteSlice, Str, Unit},
    Database, RoPrefix, RoTxn,
};
use roaring::RoaringBitmap;

const FID_SIZE: usize = 2;
const DOCID_SIZE: usize = 4;

use crate::{
    heed_codec::{
        facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec, FieldDocIdFacetCodec},
        ByteSliceRefCodec,
    },
    Index, Result, SearchContext,
};

pub struct DistinctOutput {
    pub remaining: RoaringBitmap,
    pub excluded: RoaringBitmap,
}

pub fn apply_distinct_rule<'ctx>(
    ctx: &mut SearchContext<'ctx>,
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

fn distinct_single_docid(
    index: &Index,
    txn: &RoTxn,
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
        if let Some(facet_docids) = facet_value_docids(
            index.facet_id_string_docids.remap_types(),
            txn,
            field_id,
            facet_value,
        )? {
            *excluded |= facet_docids;
        }
    }
    Ok(())
}

fn facet_value_docids(
    database: Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
    txn: &RoTxn,
    field_id: u16,
    facet_value: &[u8],
) -> heed::Result<Option<RoaringBitmap>> {
    database
        .get(txn, &FacetGroupKey { field_id, level: 0, left_bound: facet_value })
        .map(|opt| opt.map(|v| v.bitmap))
}
fn facet_number_values<'a>(
    id: u32,
    distinct: u16,
    index: &Index,
    txn: &'a RoTxn,
) -> Result<RoPrefix<'a, FieldDocIdFacetCodec<ByteSliceRefCodec>, Unit>> {
    let key = facet_values_prefix_key(distinct, id);

    let iter = index
        .field_id_docid_facet_f64s
        .remap_key_type::<ByteSlice>()
        .prefix_iter(txn, &key)?
        .remap_key_type();

    Ok(iter)
}

fn facet_string_values<'a>(
    docid: u32,
    distinct: u16,
    index: &Index,
    txn: &'a RoTxn,
) -> Result<RoPrefix<'a, FieldDocIdFacetCodec<ByteSliceRefCodec>, Str>> {
    let key = facet_values_prefix_key(distinct, docid);

    let iter = index
        .field_id_docid_facet_strings
        .remap_key_type::<ByteSlice>()
        .prefix_iter(txn, &key)?
        .remap_types();

    Ok(iter)
}

#[allow(clippy::drop_non_drop)]
fn facet_values_prefix_key(distinct: u16, id: u32) -> [u8; FID_SIZE + DOCID_SIZE] {
    concat_arrays::concat_arrays!(distinct.to_be_bytes(), id.to_be_bytes())
}

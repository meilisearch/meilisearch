use bumpalo::Bump;
use heed::{Database, RoTxn};
use roaring::RoaringBitmap;

use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec, OrderedF64Codec,
};
use crate::heed_codec::BytesRefCodec;
use crate::update::new::document::RawFacetValue;
use crate::{Document, DocumentFromDb, FieldId, FieldsIdsMap, Index, Result, SearchContext};

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
    field_name: &str,
    candidates: &RoaringBitmap,
) -> Result<DistinctOutput> {
    let mut excluded = RoaringBitmap::new();
    let mut remaining = RoaringBitmap::new();
    for docid in candidates {
        if excluded.contains(docid) {
            continue;
        }
        distinct_single_docid(
            ctx.index,
            ctx.txn,
            field_id,
            field_name,
            &ctx.fields_ids_map,
            docid,
            &mut excluded,
        )?;
        remaining.push(docid);
    }
    Ok(DistinctOutput { remaining, excluded })
}

/// Apply the distinct rule defined by [`apply_distinct_rule`] for a single document id.
pub fn distinct_single_docid(
    index: &Index,
    txn: &RoTxn<'_>,
    field_id: u16,
    field_name: &str,
    fields_ids_map: &FieldsIdsMap,
    docid: u32,
    excluded: &mut RoaringBitmap,
) -> Result<()> {
    let bump = Bump::new();
    let Some(doc) = DocumentFromDb::new(docid, txn, index, fields_ids_map)? else { return Ok(()) };

    doc.facet_values(
        field_name,
        &bump,
        |facet_value| {
            let mut bytes = [0; 16];
            let normalized;
            let (facet_value, db) = match facet_value {
                RawFacetValue::Bool(b) => {
                    normalized = b.to_string();
                    (normalized.as_bytes(), index.facet_id_string_docids.remap_types())
                }
                RawFacetValue::Number(number) => {
                    // unwrap: as number was obtained from JSON parsing, it is a finite-non-NaN f64
                    // so the OrderedF64Codec cannot fail
                    OrderedF64Codec::serialize_into(number.to_f64(), &mut bytes).unwrap();
                    (bytes.as_slice(), index.facet_id_f64_docids.remap_types())
                }
                RawFacetValue::OriginalString(original_facet_value) => {
                    normalized = crate::normalize_facet(original_facet_value);
                    (normalized.as_bytes(), index.facet_id_string_docids.remap_types())
                }
            };
            if let Some(facet_docids) = facet_value_docids(db, txn, field_id, facet_value)? {
                *excluded |= facet_docids;
            }

            Ok(())
        },
        |err| crate::InternalError::SerdeJson(err).into(),
    )?;

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

pub fn distinct_fid<'a>(
    query_distinct_field: Option<&'a str>,
    index: &Index,
    rtxn: &'a RoTxn<'a>,
    fields_ids_map: &FieldsIdsMap,
) -> Result<Option<(FieldId, &'a str)>> {
    let Some(distinct_field) = (match query_distinct_field {
        Some(distinct) => Some(distinct),
        None => index.distinct_field(rtxn)?,
    }) else {
        return Ok(None);
    };

    let Some(distinct_field_id) = fields_ids_map.id(distinct_field) else { return Ok(None) };

    Ok(Some((distinct_field_id, distinct_field)))
}

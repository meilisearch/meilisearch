use roaring::RoaringBitmap;
use heed::Database;
use crate::{facet::{ascending_facet_sort, descending_facet_sort}, heed_codec::{facet::{FacetGroupKeyCodec, FacetGroupValueCodec}, BytesRefCodec}};

pub fn recursive_facet_sort<'t>(
    rtxn: &'t heed::RoTxn<'t>,
    number_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    string_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    fields: &[(u16, bool)],
    candidates: RoaringBitmap,
) -> heed::Result<RoaringBitmap> {
    let (field_id, ascending) = match fields.first() {
        Some(first) => *first,
        None => return Ok(candidates),
    };

    let (number_iter, string_iter) = if ascending {
        let number_iter = ascending_facet_sort(
            rtxn,
            number_db,
            field_id,
            candidates.clone(),
        )?;
        let string_iter = ascending_facet_sort(
            rtxn,
            string_db,
            field_id,
            candidates,
        )?;

        (itertools::Either::Left(number_iter), itertools::Either::Left(string_iter))
    } else {
        let number_iter = descending_facet_sort(
            rtxn,
            number_db,
            field_id,
            candidates.clone(),
        )?;
        let string_iter = descending_facet_sort(
            rtxn,
            string_db,
            field_id,
            candidates,
        )?;

        (itertools::Either::Right(number_iter), itertools::Either::Right(string_iter))
    };

    let chained_iter = number_iter.chain(string_iter);
    let mut result = RoaringBitmap::new();
    for part in chained_iter {
        let (inner_candidates, _) = part?;
        if inner_candidates.len() <= 1 || fields.len() <= 1 {
            result |= inner_candidates;
        } else {
            let inner_candidates = recursive_facet_sort(
                rtxn,
                number_db,
                string_db,
                &fields[1..],
                inner_candidates,
            )?;
            result |= inner_candidates;
        }
    }

    Ok(result)
}

use roaring::RoaringBitmap;
use heed::Database;
use crate::{heed_codec::{facet::{FacetGroupKeyCodec, FacetGroupValueCodec}, BytesRefCodec}, search::{facet::{ascending_facet_sort, descending_facet_sort}, new::check_sort_criteria}, AscDesc, Member};

fn recursive_facet_sort_inner<'t>(
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
            let inner_candidates = recursive_facet_sort_inner(
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

pub fn recursive_facet_sort<'t>(
    index: &crate::Index,
    rtxn: &'t heed::RoTxn<'t>,
    sort: &[AscDesc],
    candidates: RoaringBitmap,
) -> crate::Result<RoaringBitmap> {
    check_sort_criteria(index, rtxn, Some(sort))?;

    let mut fields = Vec::new();
    let fields_ids_map = index.fields_ids_map(rtxn)?;
    for sort in sort {
        let (field_id, ascending) = match sort {
            AscDesc::Asc(Member::Field(field)) => (fields_ids_map.id(field), true),
            AscDesc::Desc(Member::Field(field)) => (fields_ids_map.id(field), false),
            AscDesc::Asc(Member::Geo(_)) => todo!(),
            AscDesc::Desc(Member::Geo(_)) => todo!(),
        };
        if let Some(field_id) = field_id {
            fields.push((field_id, ascending)); // FIXME: Should this return an error if the field is not found?
        }
    }
    
    let number_db = index
        .facet_id_f64_docids
        .remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
    let string_db = index
        .facet_id_string_docids
        .remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();

    let candidates = recursive_facet_sort_inner(rtxn, number_db, string_db, &fields, candidates)?;
    Ok(candidates)
}

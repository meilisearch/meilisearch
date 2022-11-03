use std::ops::ControlFlow;

use heed::Result;
use roaring::RoaringBitmap;

use super::{get_first_facet_value, get_highest_level};
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::heed_codec::ByteSliceRefCodec;
use crate::DocumentId;

/// Call the given closure on the facet distribution of the candidate documents.
///
/// The arguments to the closure are:
/// - the facet value, as a byte slice
/// - the number of documents among the candidates that contain this facet value
/// - the id of a document which contains the facet value. Note that this document
///   is not necessarily from the list of candidates, it is simply *any* document which
///   contains this facet value.
///
/// The return value of the closure is a `ControlFlow<()>` which indicates whether we should
/// keep iterating over the different facet values or stop.
pub fn iterate_over_facet_distribution<'t, CB>(
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
    field_id: u16,
    candidates: &RoaringBitmap,
    callback: CB,
) -> Result<()>
where
    CB: FnMut(&'t [u8], u64, DocumentId) -> Result<ControlFlow<()>>,
{
    let mut fd = FacetDistribution { rtxn, db, field_id, callback };
    let highest_level = get_highest_level(
        rtxn,
        db.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
        field_id,
    )?;

    if let Some(first_bound) = get_first_facet_value::<ByteSliceRefCodec>(rtxn, db, field_id)? {
        fd.iterate(candidates, highest_level, first_bound, usize::MAX)?;
        Ok(())
    } else {
        Ok(())
    }
}

struct FacetDistribution<'t, CB>
where
    CB: FnMut(&'t [u8], u64, DocumentId) -> Result<ControlFlow<()>>,
{
    rtxn: &'t heed::RoTxn<'t>,
    db: heed::Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
    field_id: u16,
    callback: CB,
}

impl<'t, CB> FacetDistribution<'t, CB>
where
    CB: FnMut(&'t [u8], u64, DocumentId) -> Result<ControlFlow<()>>,
{
    fn iterate_level_0(
        &mut self,
        candidates: &RoaringBitmap,
        starting_bound: &'t [u8],
        group_size: usize,
    ) -> Result<ControlFlow<()>> {
        let starting_key =
            FacetGroupKey { field_id: self.field_id, level: 0, left_bound: starting_bound };
        let iter = self.db.range(self.rtxn, &(starting_key..))?.take(group_size);
        for el in iter {
            let (key, value) = el?;
            // The range is unbounded on the right and the group size for the highest level is MAX,
            // so we need to check that we are not iterating over the next field id
            if key.field_id != self.field_id {
                return Ok(ControlFlow::Break(()));
            }
            let docids_in_common = value.bitmap & candidates;
            if !docids_in_common.is_empty() {
                let any_docid_in_common = docids_in_common.min().unwrap();
                match (self.callback)(key.left_bound, docids_in_common.len(), any_docid_in_common)?
                {
                    ControlFlow::Continue(_) => (),
                    ControlFlow::Break(_) => return Ok(ControlFlow::Break(())),
                }
            }
        }
        Ok(ControlFlow::Continue(()))
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
        let starting_key =
            FacetGroupKey { field_id: self.field_id, level, left_bound: starting_bound };
        let iter = self.db.range(self.rtxn, &(&starting_key..)).unwrap().take(group_size);

        for el in iter {
            let (key, value) = el.unwrap();
            // The range is unbounded on the right and the group size for the highest level is MAX,
            // so we need to check that we are not iterating over the next field id
            if key.field_id != self.field_id {
                return Ok(ControlFlow::Break(()));
            }
            let docids_in_common = value.bitmap & candidates;
            if !docids_in_common.is_empty() {
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
        Ok(ControlFlow::Continue(()))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;

    use heed::BytesDecode;
    use roaring::RoaringBitmap;

    use super::iterate_over_facet_distribution;
    use crate::heed_codec::facet::OrderedF64Codec;
    use crate::milli_snap;
    use crate::search::facet::tests::{get_random_looking_index, get_simple_index};

    #[test]
    fn filter_distribution_all() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = (0..=255).into_iter().collect::<RoaringBitmap>();
            let mut results = String::new();
            iterate_over_facet_distribution(
                &txn,
                index.content,
                0,
                &candidates,
                |facet, count, _| {
                    let facet = OrderedF64Codec::bytes_decode(facet).unwrap();
                    results.push_str(&format!("{facet}: {count}\n"));
                    Ok(ControlFlow::Continue(()))
                },
            )
            .unwrap();
            milli_snap!(results, i);

            txn.commit().unwrap();
        }
    }
    #[test]
    fn filter_distribution_all_stop_early() {
        let indexes = [get_simple_index(), get_random_looking_index()];
        for (i, index) in indexes.iter().enumerate() {
            let txn = index.env.read_txn().unwrap();
            let candidates = (0..=255).into_iter().collect::<RoaringBitmap>();
            let mut results = String::new();
            let mut nbr_facets = 0;
            iterate_over_facet_distribution(
                &txn,
                index.content,
                0,
                &candidates,
                |facet, count, _| {
                    let facet = OrderedF64Codec::bytes_decode(facet).unwrap();
                    if nbr_facets == 100 {
                        return Ok(ControlFlow::Break(()));
                    } else {
                        nbr_facets += 1;
                        results.push_str(&format!("{facet}: {count}\n"));

                        Ok(ControlFlow::Continue(()))
                    }
                },
            )
            .unwrap();
            milli_snap!(results, i);

            txn.commit().unwrap();
        }
    }
}

use std::fmt::Debug;
use std::ops::Bound::{self, Included, Excluded, Unbounded};

use heed::types::DecodeIgnore;
use heed::{BytesEncode, BytesDecode};
use heed::{Database, RoRange, LazyDecode};
use num_traits::Bounded;
use roaring::RoaringBitmap;

use crate::heed_codec::CboRoaringBitmapCodec;
use crate::{Index, FieldId};

pub use self::facet_condition::{FacetCondition, FacetNumberOperator, FacetStringOperator};

mod facet_condition;
mod parser;

struct FacetRange<'t, T: 't, KC> {
    iter: RoRange<'t, KC, LazyDecode<CboRoaringBitmapCodec>>,
    end: Bound<T>,
}

impl<'t, T: 't, KC> FacetRange<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded,
{
    fn new(
        rtxn: &'t heed::RoTxn,
        db: Database<KC, CboRoaringBitmapCodec>,
        field_id: FieldId,
        level: u8,
        left: Bound<T>,
        right: Bound<T>,
    ) -> heed::Result<FacetRange<'t, T, KC>>
    {
        let left_bound = match left {
            Included(left) => Included((field_id, level, left, T::min_value())),
            Excluded(left) => Excluded((field_id, level, left, T::min_value())),
            Unbounded => Included((field_id, level, T::min_value(), T::min_value())),
        };
        let right_bound = Included((field_id, level, T::max_value(), T::max_value()));
        let iter = db.lazily_decode_data().range(rtxn, &(left_bound, right_bound))?;
        Ok(FacetRange { iter, end: right })
    }
}

impl<'t, T, KC> Iterator for FacetRange<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    KC: BytesDecode<'t, DItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy,
{
    type Item = heed::Result<((FieldId, u8, T, T), RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(((fid, level, left, right), docids))) => {
                let must_be_returned = match self.end {
                    Included(end) => right <= end,
                    Excluded(end) => right < end,
                    Unbounded => true,
                };
                if must_be_returned {
                    match docids.decode() {
                        Ok(docids) => Some(Ok(((fid, level, left, right), docids))),
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    None
                }
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

pub struct FacetIter<'t, T: 't, KC> {
    rtxn: &'t heed::RoTxn<'t>,
    db: Database<KC, CboRoaringBitmapCodec>,
    field_id: FieldId,
    documents_ids: RoaringBitmap,
    level_iters: Vec<FacetRange<'t, T, KC>>,
}

impl<'t, T, KC> FacetIter<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded,
{
    pub fn new(
        rtxn: &'t heed::RoTxn,
        index: &'t Index,
        field_id: FieldId,
        documents_ids: RoaringBitmap,
    ) -> heed::Result<FacetIter<'t, T, KC>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<KC>();
        let level_0_iter = FacetRange::new(rtxn, db, field_id, 0, Unbounded, Unbounded)?;
        Ok(FacetIter { rtxn, db, field_id, documents_ids, level_iters: vec![level_0_iter] })
    }
}

impl<'t, T: 't, KC> Iterator for FacetIter<'t, T, KC>
where
    KC: heed::BytesDecode<'t, DItem = (FieldId, u8, T, T)>,
    KC: for<'x> heed::BytesEncode<'x, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded,
{
    type Item = heed::Result<(T, RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let last = self.level_iters.last_mut()?;
            for result in last {
                match result {
                    Ok(((_fid, level, left, right), mut docids)) => {
                        if level == 0 {
                            docids.intersect_with(&self.documents_ids);
                            if !docids.is_empty() {
                                self.documents_ids.difference_with(&docids);
                                return Some(Ok((left, docids)));
                            }
                        } else if !docids.is_disjoint(&self.documents_ids) {
                            let result = FacetRange::new(
                                self.rtxn,
                                self.db,
                                self.field_id,
                                level - 1,
                                Included(left),
                                Included(right),
                            );
                            match result {
                                Ok(iter) => {
                                    self.level_iters.push(iter);
                                    break;
                                },
                                Err(e) => return Some(Err(e)),
                            }
                        }
                    },
                    Err(e) => return Some(Err(e)),
                }
            }
            self.level_iters.pop();
        }
    }
}

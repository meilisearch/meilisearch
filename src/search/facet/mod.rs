use std::fmt::Debug;
use std::ops::Bound::{self, Included, Excluded, Unbounded};

use either::Either::{self, Left, Right};
use heed::types::{DecodeIgnore, ByteSlice};
use heed::{BytesEncode, BytesDecode};
use heed::{Database, RoRange, RoRevRange, LazyDecode};
use log::debug;
use num_traits::Bounded;
use roaring::RoaringBitmap;

use crate::heed_codec::CboRoaringBitmapCodec;
use crate::{Index, FieldId};

pub use self::facet_condition::{FacetCondition, FacetNumberOperator, FacetStringOperator};
pub use self::facet_distribution::FacetDistribution;

mod facet_condition;
mod facet_distribution;
mod parser;

pub struct FacetRange<'t, T: 't, KC> {
    iter: RoRange<'t, KC, LazyDecode<CboRoaringBitmapCodec>>,
    end: Bound<T>,
}

impl<'t, T: 't, KC> FacetRange<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded,
{
    pub fn new(
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

pub struct FacetRevRange<'t, T: 't, KC> {
    iter: RoRevRange<'t, KC, LazyDecode<CboRoaringBitmapCodec>>,
    end: Bound<T>,
}

impl<'t, T: 't, KC> FacetRevRange<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded,
{
    pub fn new(
        rtxn: &'t heed::RoTxn,
        db: Database<KC, CboRoaringBitmapCodec>,
        field_id: FieldId,
        level: u8,
        left: Bound<T>,
        right: Bound<T>,
    ) -> heed::Result<FacetRevRange<'t, T, KC>>
    {
        let left_bound = match left {
            Included(left) => Included((field_id, level, left, T::min_value())),
            Excluded(left) => Excluded((field_id, level, left, T::min_value())),
            Unbounded => Included((field_id, level, T::min_value(), T::min_value())),
        };
        let right_bound = Included((field_id, level, T::max_value(), T::max_value()));
        let iter = db.lazily_decode_data().rev_range(rtxn, &(left_bound, right_bound))?;
        Ok(FacetRevRange { iter, end: right })
    }
}

impl<'t, T, KC> Iterator for FacetRevRange<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    KC: BytesDecode<'t, DItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy,
{
    type Item = heed::Result<((FieldId, u8, T, T), RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Ok(((fid, level, left, right), docids))) => {
                    let must_be_returned = match self.end {
                        Included(end) => right <= end,
                        Excluded(end) => right < end,
                        Unbounded => true,
                    };
                    if must_be_returned {
                        match docids.decode() {
                            Ok(docids) => return Some(Ok(((fid, level, left, right), docids))),
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    continue;
                },
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

pub struct FacetIter<'t, T: 't, KC> {
    rtxn: &'t heed::RoTxn<'t>,
    db: Database<KC, CboRoaringBitmapCodec>,
    field_id: FieldId,
    level_iters: Vec<(RoaringBitmap, Either<FacetRange<'t, T, KC>, FacetRevRange<'t, T, KC>>)>,
    must_reduce: bool,
}

impl<'t, T, KC> FacetIter<'t, T, KC>
where
    KC: heed::BytesDecode<'t, DItem = (FieldId, u8, T, T)>,
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded,
{
    /// Create a `FacetIter` that will iterate on the different facet entries
    /// (facet value + documents ids) and that will reduce the given documents ids
    /// while iterating on the different facet levels.
    pub fn new_reducing(
        rtxn: &'t heed::RoTxn,
        index: &'t Index,
        field_id: FieldId,
        documents_ids: RoaringBitmap,
    ) -> heed::Result<FacetIter<'t, T, KC>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<KC>();
        let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
        let highest_iter = FacetRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
        let level_iters = vec![(documents_ids, Left(highest_iter))];
        Ok(FacetIter { rtxn, db, field_id, level_iters, must_reduce: true })
    }

    /// Create a `FacetIter` that will iterate on the different facet entries in reverse
    /// (facet value + documents ids) and that will reduce the given documents ids
    /// while iterating on the different facet levels.
    pub fn new_reverse_reducing(
        rtxn: &'t heed::RoTxn,
        index: &'t Index,
        field_id: FieldId,
        documents_ids: RoaringBitmap,
    ) -> heed::Result<FacetIter<'t, T, KC>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<KC>();
        let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
        let highest_iter = FacetRevRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
        let level_iters = vec![(documents_ids, Right(highest_iter))];
        Ok(FacetIter { rtxn, db, field_id, level_iters, must_reduce: true })
    }

    /// Create a `FacetIter` that will iterate on the different facet entries
    /// (facet value + documents ids) and that will not reduce the given documents ids
    /// while iterating on the different facet levels, possibly returning multiple times
    /// a document id associated with multiple facet values.
    pub fn new_non_reducing(
        rtxn: &'t heed::RoTxn,
        index: &'t Index,
        field_id: FieldId,
        documents_ids: RoaringBitmap,
    ) -> heed::Result<FacetIter<'t, T, KC>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<KC>();
        let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
        let highest_iter = FacetRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
        let level_iters = vec![(documents_ids, Left(highest_iter))];
        Ok(FacetIter { rtxn, db, field_id, level_iters, must_reduce: false })
    }

    fn highest_level<X>(rtxn: &'t heed::RoTxn, db: Database<KC, X>, fid: FieldId) -> heed::Result<Option<u8>> {
        let level = db.remap_types::<ByteSlice, DecodeIgnore>()
            .prefix_iter(rtxn, &[fid][..])?
            .remap_key_type::<KC>()
            .last().transpose()?
            .map(|((_, level, _, _), _)| level);
        Ok(level)
    }
}

impl<'t, T: 't, KC> Iterator for FacetIter<'t, T, KC>
where
    KC: heed::BytesDecode<'t, DItem = (FieldId, u8, T, T)>,
    KC: for<'x> heed::BytesEncode<'x, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded + Debug,
{
    type Item = heed::Result<(T, RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            let (documents_ids, last) = self.level_iters.last_mut()?;
            let is_ascending = last.is_left();
            for result in last {
                // If the last iterator must find an empty set of documents it means
                // that we found all the documents in the sub level iterations already,
                // we can pop this level iterator.
                if documents_ids.is_empty() {
                    break;
                }

                match result {
                    Ok(((_fid, level, left, right), mut docids)) => {

                        docids.intersect_with(&documents_ids);
                        if !docids.is_empty() {
                            if self.must_reduce {
                                documents_ids.difference_with(&docids);
                            }

                            if level == 0 {
                                debug!("found {:?} at {:?}",  docids, left);
                                return Some(Ok((left, docids)));
                            }

                            let rtxn = self.rtxn;
                            let db = self.db;
                            let fid = self.field_id;
                            let left = Included(left);
                            let right = Included(right);

                            debug!("calling with {:?} to {:?} (level {}) to find {:?}",
                                left, right, level - 1, docids,
                            );

                            let result = if is_ascending {
                                FacetRange::new(rtxn, db, fid, level - 1, left, right).map(Left)
                            } else {
                                FacetRevRange::new(rtxn, db, fid, level - 1, left, right).map(Right)
                            };

                            match result {
                                Ok(iter) => {
                                    self.level_iters.push((docids, iter));
                                    continue 'outer;
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

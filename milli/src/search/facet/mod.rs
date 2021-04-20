use std::ops::Bound::{self, Included, Excluded, Unbounded};

use either::Either::{self, Left, Right};
use heed::types::{DecodeIgnore, ByteSlice};
use heed::{Database, RoRange, RoRevRange, LazyDecode};
use roaring::RoaringBitmap;

use crate::heed_codec::CboRoaringBitmapCodec;
use crate::heed_codec::facet::FacetLevelValueF64Codec;
use crate::{Index, FieldId};

pub use self::facet_condition::{FacetCondition, FacetNumberOperator, FacetStringOperator};
pub use self::facet_distribution::FacetDistribution;

mod facet_condition;
mod facet_distribution;
mod parser;

pub struct FacetRange<'t> {
    iter: RoRange<'t, FacetLevelValueF64Codec, LazyDecode<CboRoaringBitmapCodec>>,
    end: Bound<f64>,
}

impl<'t> FacetRange<'t> {
    pub fn new(
        rtxn: &'t heed::RoTxn,
        db: Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
        field_id: FieldId,
        level: u8,
        left: Bound<f64>,
        right: Bound<f64>,
    ) -> heed::Result<FacetRange<'t>>
    {
        let left_bound = match left {
            Included(left) => Included((field_id, level, left, f64::MIN)),
            Excluded(left) => Excluded((field_id, level, left, f64::MIN)),
            Unbounded => Included((field_id, level, f64::MIN, f64::MIN)),
        };
        let right_bound = Included((field_id, level, f64::MAX, f64::MAX));
        let iter = db.lazily_decode_data().range(rtxn, &(left_bound, right_bound))?;
        Ok(FacetRange { iter, end: right })
    }
}

impl<'t> Iterator for FacetRange<'t> {
    type Item = heed::Result<((FieldId, u8, f64, f64), RoaringBitmap)>;

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

pub struct FacetRevRange<'t> {
    iter: RoRevRange<'t, FacetLevelValueF64Codec, LazyDecode<CboRoaringBitmapCodec>>,
    end: Bound<f64>,
}

impl<'t> FacetRevRange<'t> {
    pub fn new(
        rtxn: &'t heed::RoTxn,
        db: Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
        field_id: FieldId,
        level: u8,
        left: Bound<f64>,
        right: Bound<f64>,
    ) -> heed::Result<FacetRevRange<'t>>
    {
        let left_bound = match left {
            Included(left) => Included((field_id, level, left, f64::MIN)),
            Excluded(left) => Excluded((field_id, level, left, f64::MIN)),
            Unbounded => Included((field_id, level, f64::MIN, f64::MIN)),
        };
        let right_bound = Included((field_id, level, f64::MAX, f64::MAX));
        let iter = db.lazily_decode_data().rev_range(rtxn, &(left_bound, right_bound))?;
        Ok(FacetRevRange { iter, end: right })
    }
}

impl<'t> Iterator for FacetRevRange<'t> {
    type Item = heed::Result<((FieldId, u8, f64, f64), RoaringBitmap)>;

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

pub struct FacetIter<'t> {
    rtxn: &'t heed::RoTxn<'t>,
    db: Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
    field_id: FieldId,
    level_iters: Vec<(RoaringBitmap, Either<FacetRange<'t>, FacetRevRange<'t>>)>,
    must_reduce: bool,
}

impl<'t> FacetIter<'t> {
    /// Create a `FacetIter` that will iterate on the different facet entries
    /// (facet value + documents ids) and that will reduce the given documents ids
    /// while iterating on the different facet levels.
    pub fn new_reducing(
        rtxn: &'t heed::RoTxn,
        index: &'t Index,
        field_id: FieldId,
        documents_ids: RoaringBitmap,
    ) -> heed::Result<FacetIter<'t>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<FacetLevelValueF64Codec>();
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
    ) -> heed::Result<FacetIter<'t>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<FacetLevelValueF64Codec>();
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
    ) -> heed::Result<FacetIter<'t>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<FacetLevelValueF64Codec>();
        let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
        let highest_iter = FacetRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
        let level_iters = vec![(documents_ids, Left(highest_iter))];
        Ok(FacetIter { rtxn, db, field_id, level_iters, must_reduce: false })
    }

    fn highest_level<X>(
        rtxn: &'t heed::RoTxn,
        db: Database<FacetLevelValueF64Codec, X>,
        fid: FieldId,
    ) -> heed::Result<Option<u8>>
    {
        let level = db.remap_types::<ByteSlice, DecodeIgnore>()
            .prefix_iter(rtxn, &[fid][..])?
            .remap_key_type::<FacetLevelValueF64Codec>()
            .last().transpose()?
            .map(|((_, level, _, _), _)| level);
        Ok(level)
    }
}

impl<'t> Iterator for FacetIter<'t> {
    type Item = heed::Result<(f64, RoaringBitmap)>;

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
                                return Some(Ok((left, docids)));
                            }

                            let rtxn = self.rtxn;
                            let db = self.db;
                            let fid = self.field_id;
                            let left = Included(left);
                            let right = Included(right);

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

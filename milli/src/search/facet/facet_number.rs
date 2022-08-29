// use std::ops::Bound::{self, Excluded, Included, Unbounded};

// use either::Either::{self, Left, Right};
// use heed::types::{ByteSlice, DecodeIgnore};
// use heed::{BytesDecode, BytesEncode, Database, Lazy, LazyDecode, RoRange, RoRevRange};
// use obkv::Key;
// use roaring::RoaringBitmap;

// use crate::heed_codec::facet::new::ordered_f64_codec::OrderedF64Codec;
// use crate::heed_codec::facet::new::{FacetGroupValueCodec, FacetKey, FacetKeyCodec};
// use crate::heed_codec::CboRoaringBitmapCodec;
// use crate::{FieldId, Index};

// pub struct FacetNumberRange<'t, 'e> {
//     rtxn: &'t heed::RoTxn<'e>,
//     db: Database<FacetKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
//     iter: RoRange<'t, FacetKeyCodec<OrderedF64Codec>, LazyDecode<FacetGroupValueCodec>>,
//     max_bound: f64,
//     previous: Option<(FacetKey<f64>, Lazy<'t, FacetGroupValueCodec>)>,
//     field_id: FieldId,
//     end: Bound<f64>,
// }

// impl<'t, 'e> FacetNumberRange<'t, 'e> {
//     pub fn new(
//         rtxn: &'t heed::RoTxn<'e>,
//         db: Database<FacetKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
//         field_id: FieldId,
//         level: u8,
//         left: Bound<f64>,
//         right: Bound<f64>,
//     ) -> heed::Result<FacetNumberRange<'t, 'e>> {
//         let left_bound = match left {
//             Included(left_bound) => Included(FacetKey { field_id, level, left_bound }),
//             Excluded(left_bound) => Excluded(FacetKey { field_id, level, left_bound }),
//             Unbounded => Included(FacetKey { field_id, level, left_bound: f64::MIN }),
//         };

//         let mut iter = db.lazily_decode_data().range(rtxn, &(left_bound, Unbounded))?;
//         let mut previous = iter.next().transpose()?;

//         // Compute the maximum end bound by looking at the key of the last element in level 0
//         let mut prefix_level_0 = vec![];
//         prefix_level_0.extend_from_slice(&field_id.to_be_bytes());
//         prefix_level_0.push(level);

//         let mut rev_iter =
//             db.as_polymorph().rev_prefix_iter::<_, ByteSlice, ByteSlice>(rtxn, &prefix_level_0)?;

//         let rev_iter_first = rev_iter.next().transpose()?;
//         let max_bound = if let Some((max_bound_key, _)) = rev_iter_first {
//             let max_bound_key =
//                 FacetKeyCodec::<OrderedF64Codec>::bytes_decode(max_bound_key).unwrap();
//             max_bound_key.left_bound
//         } else {
//             // I can't imagine when that would happen, but let's handle it correctly anyway
//             // by making the iterator empty
//             previous = None;
//             0.0 // doesn't matter since previous = None so the iterator will always early exit
//                 // and return None itself
//         };

//         Ok(FacetNumberRange { rtxn, db, iter, field_id, previous, max_bound, end: right })
//     }
// }

// impl<'t, 'e> Iterator for FacetNumberRange<'t, 'e> {
//     type Item = heed::Result<(FacetKey<f64>, RoaringBitmap)>;

//     fn next(&mut self) -> Option<Self::Item> {
//         // The idea here is to return the **previous** element only if the left
//         // bound of the current key fits within the range given to the iter
//         // if it doesn't, then there is still a chance that it must be returned,
//         // but we need to check the actual right bound of the group by looking for
//         // the key preceding the first key of the next group in level 0

//         let (prev_key, prev_value) = self.previous?;

//         let (next_left_bound, next_previous) = if let Some(next) = self.iter.next() {
//             let (key, group_value) = match next {
//                 Ok(n) => n,
//                 Err(e) => return Some(Err(e)),
//             };
//             (key.left_bound, Some((key, group_value)))
//         } else {
//             // we're at the end of the level iter, so we need to fetch the max bound instead
//             (self.max_bound, None)
//         };
//         let must_be_returned = match self.end {
//             Included(end) => next_left_bound <= end,
//             Excluded(end) => next_left_bound < end,
//             Unbounded => true,
//         };
//         if must_be_returned {
//             match prev_value.decode() {
//                 Ok(group_value) => {
//                     self.previous = next_previous;
//                     Some(Ok((prev_key, group_value.bitmap)))
//                 }
//                 Err(e) => Some(Err(e)),
//             }
//         } else {
//             // it still possible that we want to return the value (one last time)
//             // but to do so, we need to fetch the right bound of the current group
//             // this is done by getting the first element at level 0 of the next group
//             // then iterating in reverse from it
//             // once we have the right bound, we can compare it, and then return or not
//             // then we still set self.previous to None so that no other element can return
//             // from it?
//             let mut level_0_key_prefix = vec![];
//             level_0_key_prefix.extend_from_slice(&self.field_id.to_be_bytes());
//             level_0_key_prefix.push(0);
//             let key =
//                 FacetKey::<f64> { field_id: self.field_id, level: 0, left_bound: next_left_bound };
//             let key_bytes = FacetKeyCodec::<OrderedF64Codec>::bytes_encode(&key).unwrap();
//             level_0_key_prefix.extend_from_slice(&key_bytes);

//             let mut rev_iter_next_group_level_0 = self
//                 .db
//                 .as_polymorph()
//                 .rev_prefix_iter::<_, ByteSlice, ByteSlice>(&self.rtxn, &level_0_key_prefix)
//                 .unwrap();
//             let (key_for_right_bound, _) = rev_iter_next_group_level_0.next().unwrap().unwrap();
//             let key_for_right_bound =
//                 FacetKeyCodec::<OrderedF64Codec>::bytes_decode(key_for_right_bound).unwrap();
//             let right_bound = key_for_right_bound.left_bound;
//             let must_be_returned = match self.end {
//                 Included(end) => right_bound <= end,
//                 Excluded(end) => right_bound < end,
//                 Unbounded => unreachable!(),
//             };
//             self.previous = None;
//             if must_be_returned {
//                 match prev_value.decode() {
//                     Ok(group_value) => Some(Ok((prev_key, group_value.bitmap))),
//                     Err(e) => Some(Err(e)),
//                 }
//             } else {
//                 None
//             }
//         }
//     }
// }

// pub struct FacetNumberRevRange<'t> {
//     iter: RoRevRange<'t, FacetKeyCodec<OrderedF64Codec>, LazyDecode<FacetGroupValueCodec>>,
//     end: Bound<f64>,
// }

// impl<'t> FacetNumberRevRange<'t> {
//     pub fn new(
//         rtxn: &'t heed::RoTxn,
//         db: Database<FacetKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
//         field_id: FieldId,
//         level: u8,
//         left: Bound<f64>,
//         right: Bound<f64>,
//     ) -> heed::Result<FacetNumberRevRange<'t>> {
//         let left_bound = match left {
//             Included(left) => Included(FacetKey { field_id, level, left_bound: left }),
//             Excluded(left) => Excluded(FacetKey { field_id, level, left_bound: left }),
//             Unbounded => Included(FacetKey { field_id, level, left_bound: f64::MIN }),
//         };
//         let right_bound = Included(FacetKey { field_id, level, left_bound: f64::MAX });
//         let iter = db.lazily_decode_data().rev_range(rtxn, &(left_bound, right_bound))?;
//         Ok(FacetNumberRevRange { iter, end: right })
//     }
// }

// impl<'t> Iterator for FacetNumberRevRange<'t> {
//     type Item = heed::Result<(FacetKey<f64>, RoaringBitmap)>;

//     fn next(&mut self) -> Option<Self::Item> {
//         loop {
//             match self.iter.next() {
//                 Some(Ok((FacetKey { field_id, level, left_bound }, docids))) => {
//                     let must_be_returned = match self.end {
//                         Included(end) => todo!(), //right <= end,
//                         Excluded(end) => todo!(), //right < end,
//                         Unbounded => true,
//                     };
//                     if must_be_returned {
//                         match docids.decode() {
//                             Ok(docids) => {
//                                 return Some(Ok((
//                                     FacetKey { field_id, level, left_bound },
//                                     docids.bitmap,
//                                 )))
//                             }
//                             Err(e) => return Some(Err(e)),
//                         }
//                     }
//                     continue;
//                 }
//                 Some(Err(e)) => return Some(Err(e)),
//                 None => return None,
//             }
//         }
//     }
// }

// pub struct FacetNumberIter<'t, 'e> {
//     rtxn: &'t heed::RoTxn<'t>,
//     db: Database<FacetKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
//     field_id: FieldId,
//     level_iters: Vec<(RoaringBitmap, Either<FacetNumberRange<'t, 'e>, FacetNumberRevRange<'t>>)>,
//     must_reduce: bool,
// }

// impl<'t, 'e> FacetNumberIter<'t, 'e> {
//     /// Create a `FacetNumberIter` that will iterate on the different facet entries
//     /// (facet value + documents ids) and that will reduce the given documents ids
//     /// while iterating on the different facet levels.
//     pub fn new_reducing(
//         rtxn: &'t heed::RoTxn<'e>,
//         index: &'t Index,
//         field_id: FieldId,
//         documents_ids: RoaringBitmap,
//     ) -> heed::Result<FacetNumberIter<'t, 'e>> {
//         let db = index.facet_id_f64_docids;
//         let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
//         let highest_iter =
//             FacetNumberRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
//         let level_iters = vec![(documents_ids, Left(highest_iter))];
//         Ok(FacetNumberIter { rtxn, db, field_id, level_iters, must_reduce: true })
//     }

//     /// Create a `FacetNumberIter` that will iterate on the different facet entries in reverse
//     /// (facet value + documents ids) and that will reduce the given documents ids
//     /// while iterating on the different facet levels.
//     pub fn new_reverse_reducing(
//         rtxn: &'t heed::RoTxn<'e>,
//         index: &'t Index,
//         field_id: FieldId,
//         documents_ids: RoaringBitmap,
//     ) -> heed::Result<FacetNumberIter<'t, 'e>> {
//         let db = index.facet_id_f64_docids;
//         let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
//         let highest_iter =
//             FacetNumberRevRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
//         let level_iters = vec![(documents_ids, Right(highest_iter))];
//         Ok(FacetNumberIter { rtxn, db, field_id, level_iters, must_reduce: true })
//     }

//     /// Create a `FacetNumberIter` that will iterate on the different facet entries
//     /// (facet value + documents ids) and that will not reduce the given documents ids
//     /// while iterating on the different facet levels, possibly returning multiple times
//     /// a document id associated with multiple facet values.
//     pub fn new_non_reducing(
//         rtxn: &'t heed::RoTxn<'e>,
//         index: &'t Index,
//         field_id: FieldId,
//         documents_ids: RoaringBitmap,
//     ) -> heed::Result<FacetNumberIter<'t, 'e>> {
//         let db = index.facet_id_f64_docids;
//         let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
//         let highest_iter =
//             FacetNumberRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
//         let level_iters = vec![(documents_ids, Left(highest_iter))];
//         Ok(FacetNumberIter { rtxn, db, field_id, level_iters, must_reduce: false })
//     }

//     fn highest_level<X>(
//         rtxn: &'t heed::RoTxn,
//         db: Database<FacetKeyCodec<OrderedF64Codec>, X>,
//         fid: FieldId,
//     ) -> heed::Result<Option<u8>> {
//         let level = db
//             .remap_types::<ByteSlice, DecodeIgnore>()
//             .prefix_iter(rtxn, &fid.to_be_bytes())?
//             .remap_key_type::<FacetKeyCodec<OrderedF64Codec>>()
//             .last()
//             .transpose()?
//             .map(|(key, _)| key.level);
//         Ok(level)
//     }
// }

// impl<'t, 'e> Iterator for FacetNumberIter<'t, 'e> {
//     type Item = heed::Result<(f64, RoaringBitmap)>;

//     fn next(&mut self) -> Option<Self::Item> {
//         'outer: loop {
//             let (documents_ids, last) = self.level_iters.last_mut()?;
//             let is_ascending = last.is_left();
//             for result in last {
//                 // If the last iterator must find an empty set of documents it means
//                 // that we found all the documents in the sub level iterations already,
//                 // we can pop this level iterator.
//                 if documents_ids.is_empty() {
//                     break;
//                 }

//                 match result {
//                     Ok((key, mut docids)) => {
//                         docids &= &*documents_ids;
//                         if !docids.is_empty() {
//                             if self.must_reduce {
//                                 *documents_ids -= &docids;
//                             }

//                             if level == 0 {
//                                 return Some(Ok((left, docids)));
//                             }

//                             let rtxn = self.rtxn;
//                             let db = self.db;
//                             let fid = self.field_id;
//                             let left = Included(left);
//                             let right = Included(right);

//                             let result = if is_ascending {
//                                 FacetNumberRange::new(rtxn, db, fid, level - 1, left, right)
//                                     .map(Left)
//                             } else {
//                                 FacetNumberRevRange::new(rtxn, db, fid, level - 1, left, right)
//                                     .map(Right)
//                             };

//                             match result {
//                                 Ok(iter) => {
//                                     self.level_iters.push((docids, iter));
//                                     continue 'outer;
//                                 }
//                                 Err(e) => return Some(Err(e)),
//                             }
//                         }
//                     }
//                     Err(e) => return Some(Err(e)),
//                 }
//             }
//             self.level_iters.pop();
//         }
//     }
// }

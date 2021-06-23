//! This module contains helpers iterators for facet strings.
//!
//! The purpose is to help iterate over the quite complex system of facets strings. A simple
//! description of the system would be that every facet string value is stored into an LMDB database
//! and that every value is associated with the document ids which are associated with this facet
//! string value.
//!
//! In reality it is a little bit more complex as we have to create aggregations of runs of facet
//! string values, those aggregations helps in choosing the right groups of facets to follow.
//!
//! ## A typical algorithm run
//!
//! If a group of aggregated facets values contains one of the documents ids, we must continue
//! iterating over the sub-groups.
//!
//! If this group is the lowest level and contain at least one document id we yield the associated
//! facet documents ids.
//!
//! If the group doesn't contain one of our documents ids, we continue to the next group at this
//! same level.
//!
//! ## The complexity comes from the strings
//!
//! This algorithm is exactly the one that we use for facet numbers. It is quite easy to create
//! aggregated facet number, groups of facets are easy to define in the LMDB key, we just put the
//! two numbers bounds, the left and the right bound of the group, both inclusive.
//!
//! It is easy to make sure that the groups are ordered, LMDB sort its keys lexicographically and
//! puting two numbers big-endian encoded one after the other gives us ordered groups. The values
//! are simple unions of the documents ids coming from the groups below.
//!
//! ### Example of what a facet number LMDB database contain
//!
//! | level | left-bound | right-bound | documents ids    |
//! |-------|------------|-------------|------------------|
//! | 0     | 0          | _skipped_   | 1, 2             |
//! | 0     | 1          | _skipped_   | 6, 7             |
//! | 0     | 3          | _skipped_   | 4, 7             |
//! | 0     | 5          | _skipped_   | 2, 3, 4          |
//! | 1     | 0          | 1           | 1, 2, 6, 7       |
//! | 1     | 3          | 5           | 2, 3, 4, 7       |
//! | 2     | 0          | 5           | 1, 2, 3, 4, 6, 7 |
//!
//! As you can see the level 0 have two equal bounds, therefore we skip serializing the second
//! bound, that's the base level where you can directly fetch the documents ids associated with an
//! exact number.
//!
//! The next levels have two different bounds and the associated documents ids are simply the result
//! of an union of all the documents ids associated with the aggregated groups above.
//!
//! ## The complexity of defining groups for facet strings
//!
//! As explained above, defining groups of facet numbers is easy, LMDB stores the keys in
//! lexicographical order, it means that whatever the key represent the bytes are read in their raw
//! form and a simple `strcmp` will define the order in which keys will be read from the store.
//!
//! That's easy for types with a known size, like floats or integers, they are 64 bytes long and
//! appending one after the other in big-endian is consistent. LMDB will simply sort the keys by the
//! first number then by the second if the the first number is equal on two keys.
//!
//! For strings it is a lot more complex as those types are unsized, it means that the size of facet
//! strings is different for each facet value.
//!
//! ### Basic approach: padding the keys
//!
//! A first approach would be to simply define the maximum size of a facet string and pad the keys
//! with zeroes. The big problem of this approach is that it:
//!  1. reduces the maximum size of facet strings by half, as we need to put two keys one after the
//!     other.
//!  2. makes the keys of facet strings very big (approximately 250 bytes), impacting a lot LMDB
//!     performances.
//!
//! ### Better approach: number the facet groups
//!
//! A better approach would be to number the groups, this way we don't have the downsides of the
//! previously described approach but we need to be able to describe the groups by using a number.
//!
//! #### Example of facet strings with numbered groups
//!
//! | level | left-bound | right-bound | left-string | right-string | documents ids    |
//! |-------|------------|-------------|-------------|--------------|------------------|
//! | 0     | alpha      | _skipped_   | _skipped_   | _skipped_    | 1, 2             |
//! | 0     | beta       | _skipped_   | _skipped_   | _skipped_    | 6, 7             |
//! | 0     | gamma      | _skipped_   | _skipped_   | _skipped_    | 4, 7             |
//! | 0     | omega      | _skipped_   | _skipped_   | _skipped_    | 2, 3, 4          |
//! | 1     | 0          | 1           | alpha       | beta         | 1, 2, 6, 7       |
//! | 1     | 2          | 3           | gamma       | omega        | 2, 3, 4, 7       |
//! | 2     | 0          | 3           | _skipped_   | _skipped_    | 1, 2, 3, 4, 6, 7 |
//!
//! As you can see the level 0 doesn't actually change much, we skip nearly everything, we do not
//! need to store the facet string value two times.
//!
//! The number in the left-bound and right-bound columns are incremental numbers representing the
//! level 0 strings, .i.e. alpha is 0, beta is 1. Those numbers are just here to keep the ordering
//! of the LMDB keys.
//!
//! In the value, not in the key, you can see that we added two new values: the left-string and the
//! right-string, which defines the original facet strings associated with the given group.
//!
//! We put those two strings inside of the value, this way we do not limit the maximum size of the
//! facet string values, and the impact on performances is not important as, IIRC, LMDB put big
//! values on another page, this helps in iterating over keys fast enough and only fetch the page
//! with the values when required.
//!
//! The other little advantage with this solution is that there is no a big overhead, compared with
//! the facet number levels, we only duplicate the facet strings once for the level 1.
//!
//! #### A typical algorithm run
//!
//! Note that the algorithm is always moving from the highest level to the lowest one, one level
//! by one level, this is why it is ok to only store the facets string on the level 1.
//!
//! If a group of aggregated facets values, a group with numbers contains one of the documents ids,
//! we must continue iterating over the sub-groups. To do so:
//!   - If we are at a level >= 2, we just do the same as with the facet numbers, get both bounds
//!     and iterate over the facet groups defined by these numbers over the current level - 1.
//!   - If we are at level 1, we retrieve both keys, the left-string and right-string, from the
//!     value and just do the same as with the facet numbers but with strings: iterate over the
//!     current level - 1 with both keys.
//!
//! If this group is the lowest level (level 0) and contain at least one document id we yield the
//! associated facet documents ids.
//!
//! If the group doesn't contain one of our documents ids, we continue to the next group at this
//! same level.
//!

use std::num::NonZeroU8;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};

use either::{Either, Left, Right};
use heed::types::{ByteSlice, DecodeIgnore};
use heed::{Database, LazyDecode, RoRange};
use roaring::RoaringBitmap;

use crate::heed_codec::facet::{
    FacetLevelValueU32Codec, FacetStringLevelZeroCodec, FacetStringZeroBoundsValueCodec,
};
use crate::heed_codec::CboRoaringBitmapCodec;
use crate::{FieldId, Index};

/// An iterator that is used to explore the facets level strings
/// from the level 1 to infinity.
///
/// It yields the level, group id that an entry covers, the optional group strings
/// that it covers of the level 0 only if it is an entry from the level 1 and
/// the roaring bitmap associated.
pub struct FacetStringGroupRange<'t> {
    iter: RoRange<
        't,
        FacetLevelValueU32Codec,
        LazyDecode<FacetStringZeroBoundsValueCodec<CboRoaringBitmapCodec>>,
    >,
    end: Bound<u32>,
}

impl<'t> FacetStringGroupRange<'t> {
    pub fn new<X, Y>(
        rtxn: &'t heed::RoTxn,
        db: Database<X, Y>,
        field_id: FieldId,
        level: NonZeroU8,
        left: Bound<u32>,
        right: Bound<u32>,
    ) -> heed::Result<FacetStringGroupRange<'t>> {
        let db = db.remap_types::<
            FacetLevelValueU32Codec,
            FacetStringZeroBoundsValueCodec<CboRoaringBitmapCodec>,
        >();
        let left_bound = match left {
            Included(left) => Included((field_id, level, left, u32::MIN)),
            Excluded(left) => Excluded((field_id, level, left, u32::MIN)),
            Unbounded => Included((field_id, level, u32::MIN, u32::MIN)),
        };
        let right_bound = Included((field_id, level, u32::MAX, u32::MAX));
        let iter = db.lazily_decode_data().range(rtxn, &(left_bound, right_bound))?;
        Ok(FacetStringGroupRange { iter, end: right })
    }
}

impl<'t> Iterator for FacetStringGroupRange<'t> {
    type Item = heed::Result<((NonZeroU8, u32, u32), (Option<(&'t str, &'t str)>, RoaringBitmap))>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(((_fid, level, left, right), docids))) => {
                let must_be_returned = match self.end {
                    Included(end) => right <= end,
                    Excluded(end) => right < end,
                    Unbounded => true,
                };
                if must_be_returned {
                    match docids.decode() {
                        Ok(docids) => Some(Ok(((level, left, right), docids))),
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    None
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// An iterator that is used to explore the level 0 of the facets string database.
///
/// It yields the facet string and the roaring bitmap associated with it.
pub struct FacetStringLevelZeroRange<'t> {
    iter: RoRange<'t, FacetStringLevelZeroCodec, CboRoaringBitmapCodec>,
}

impl<'t> FacetStringLevelZeroRange<'t> {
    pub fn new<X, Y>(
        rtxn: &'t heed::RoTxn,
        db: Database<X, Y>,
        field_id: FieldId,
        left: Bound<&str>,
        right: Bound<&str>,
    ) -> heed::Result<FacetStringLevelZeroRange<'t>> {
        let db = db.remap_types::<FacetStringLevelZeroCodec, CboRoaringBitmapCodec>();
        let left_bound = match left {
            Included(left) => Included((field_id, left)),
            Excluded(left) => Excluded((field_id, left)),
            Unbounded => Included((field_id, "")),
        };

        let right_bound = match right {
            Included(right) => Included((field_id, right)),
            Excluded(right) => Excluded((field_id, right)),
            Unbounded => Excluded((field_id + 1, "")),
        };

        db.range(rtxn, &(left_bound, right_bound)).map(|iter| FacetStringLevelZeroRange { iter })
    }
}

impl<'t> Iterator for FacetStringLevelZeroRange<'t> {
    type Item = heed::Result<(&'t str, RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(((_fid, value), docids))) => Some(Ok((value, docids))),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// An iterator that is used to explore the facet strings level by level,
/// it will only return facets strings that are associated with the
/// candidates documents ids given.
pub struct FacetStringIter<'t> {
    rtxn: &'t heed::RoTxn<'t>,
    db: Database<ByteSlice, ByteSlice>,
    field_id: FieldId,
    level_iters:
        Vec<(RoaringBitmap, Either<FacetStringGroupRange<'t>, FacetStringLevelZeroRange<'t>>)>,
}

impl<'t> FacetStringIter<'t> {
    pub fn new_non_reducing(
        rtxn: &'t heed::RoTxn,
        index: &'t Index,
        field_id: FieldId,
        documents_ids: RoaringBitmap,
    ) -> heed::Result<FacetStringIter<'t>> {
        // TODO make sure that we change the database before using it, or merging the PR.
        let db = index.facet_id_string_docids.remap_types::<ByteSlice, ByteSlice>();
        let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
        let highest_iter = match NonZeroU8::new(highest_level) {
            Some(highest_level) => Left(FacetStringGroupRange::new(
                rtxn,
                index.facet_id_string_docids,
                field_id,
                highest_level,
                Unbounded,
                Unbounded,
            )?),
            None => Right(FacetStringLevelZeroRange::new(
                rtxn,
                index.facet_id_string_docids,
                field_id,
                Unbounded,
                Unbounded,
            )?),
        };

        Ok(FacetStringIter { rtxn, db, field_id, level_iters: vec![(documents_ids, highest_iter)] })
    }

    fn highest_level<X, Y>(
        rtxn: &'t heed::RoTxn,
        db: Database<X, Y>,
        fid: FieldId,
    ) -> heed::Result<Option<u8>> {
        Ok(db
            .remap_types::<ByteSlice, DecodeIgnore>()
            .prefix_iter(rtxn, &fid.to_be_bytes())? // the field id is the first two bits
            .last()
            .transpose()?
            .map(|(key_bytes, _)| key_bytes[2])) // the level is the third bit
    }
}

impl<'t> Iterator for FacetStringIter<'t> {
    type Item = heed::Result<(&'t str, RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            let (documents_ids, last) = self.level_iters.last_mut()?;
            match last {
                Left(last) => {
                    for result in last {
                        match result {
                            Ok(((level, left, right), (string_bounds, mut docids))) => {
                                docids &= &*documents_ids;
                                if !docids.is_empty() {
                                    *documents_ids -= &docids;

                                    let result = match string_bounds {
                                        Some((left, right)) => FacetStringLevelZeroRange::new(
                                            self.rtxn,
                                            self.db,
                                            self.field_id,
                                            Included(left),
                                            Included(right),
                                        )
                                        .map(Right),
                                        None => FacetStringGroupRange::new(
                                            self.rtxn,
                                            self.db,
                                            self.field_id,
                                            NonZeroU8::new(level.get() - 1).unwrap(),
                                            Included(left),
                                            Included(right),
                                        )
                                        .map(Left),
                                    };

                                    match result {
                                        Ok(iter) => {
                                            self.level_iters.push((docids, iter));
                                            continue 'outer;
                                        }
                                        Err(e) => return Some(Err(e)),
                                    }
                                }
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                }
                Right(last) => {
                    // level zero only
                    for result in last {
                        match result {
                            Ok((value, mut docids)) => {
                                docids &= &*documents_ids;
                                if !docids.is_empty() {
                                    *documents_ids -= &docids;
                                    return Some(Ok((value, docids)));
                                }
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                }
            }

            self.level_iters.pop();
        }
    }
}

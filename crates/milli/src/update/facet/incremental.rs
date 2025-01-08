use std::fs::File;
use std::io::BufReader;

use grenad::Merger;
use heed::types::{Bytes, DecodeIgnore};
use heed::{BytesDecode, Error, RoTxn, RwTxn};
use obkv::KvReader;
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValue, FacetGroupValueCodec,
};
use crate::heed_codec::BytesRefCodec;
use crate::search::facet::get_highest_level;
use crate::update::del_add::DelAdd;
use crate::update::index_documents::valid_lmdb_key;
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{CboRoaringBitmapCodec, Index, Result};

/// Enum used as a return value for the facet incremental indexing.
///
/// - `ModificationResult::InPlace` means that modifying the `facet_value` into the `level` did not have
///   an effect on the number of keys in that level. Therefore, it did not increase the number of children
///   of the parent node.
///
/// - `ModificationResult::Insert` means that modifying the `facet_value` into the `level` resulted
///   in the addition of a new key in that level, and that therefore the number of children
///   of the parent node should be incremented.
///
/// - `ModificationResult::Remove` means that modifying the `facet_value` into the `level` resulted in a change in the
///   number of keys in the level. For example, removing a document id from the facet value `3` could
///   cause it to have no corresponding document in level 0 anymore, and therefore the key was deleted
///   entirely. In that case, `ModificationResult::Remove` is returned. The parent of the deleted key must
///   then adjust its group size. If its group size falls to 0, then it will need to be deleted as well.
///
/// - `ModificationResult::Reduce/Expand` means that modifying the `facet_value` into the `level` resulted in a change in the
///   bounds of the keys of the level. For example, removing a document id from the facet value
///   `3` might have caused the facet value `3` to have no corresponding document in level 0. Therefore,
///   in level 1, the key with the left bound `3` had to be changed to the next facet value (e.g. 4).
///   In that case `ModificationResult::Reduce` is returned. The parent of the reduced key may need to adjust
///   its left bound as well.
///
/// - `ModificationResult::Nothing` means that modifying the `facet_value` didn't have any impact into the `level`.
///   This case is reachable when a document id is removed from a sub-level node but is still present in another one.
///   For example, removing `2` from a document containing `2` and `3`, the document id will removed form the `level 0`
///   but should remain in the group node [1..4] in `level 1`.
enum ModificationResult {
    InPlace,
    Expand,
    Insert,
    Reduce { next: Option<Vec<u8>> },
    Remove { next: Option<Vec<u8>> },
    Nothing,
}

/// Algorithm to incrementally insert and delete elememts into the
/// `facet_id_(string/f64)_docids` databases.
pub struct FacetsUpdateIncremental {
    inner: FacetsUpdateIncrementalInner,
    delta_data: Merger<BufReader<File>, MergeDeladdCboRoaringBitmaps>,
}

impl FacetsUpdateIncremental {
    pub fn new(
        index: &Index,
        facet_type: FacetType,
        delta_data: Merger<BufReader<File>, MergeDeladdCboRoaringBitmaps>,
        group_size: u8,
        min_level_size: u8,
        max_group_size: u8,
    ) -> Self {
        FacetsUpdateIncremental {
            inner: FacetsUpdateIncrementalInner {
                db: match facet_type {
                    FacetType::String => index
                        .facet_id_string_docids
                        .remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>(),
                    FacetType::Number => index
                        .facet_id_f64_docids
                        .remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>(),
                },
                group_size,
                max_group_size,
                min_level_size,
            },
            delta_data,
        }
    }

    #[tracing::instrument(level = "trace", skip_all, target = "indexing::facets::incremental")]
    pub fn execute(self, wtxn: &mut RwTxn<'_>) -> crate::Result<()> {
        let mut current_field_id = None;
        let mut facet_level_may_be_updated = false;
        let mut iter = self.delta_data.into_stream_merger_iter()?;
        while let Some((key, value)) = iter.next()? {
            if !valid_lmdb_key(key) {
                continue;
            }

            let key = FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(key)
                .map_err(heed::Error::Encoding)?;

            if facet_level_may_be_updated
                && current_field_id.map_or(false, |fid| fid != key.field_id)
            {
                // Only add or remove a level after making all the field modifications.
                self.inner.add_or_delete_level(wtxn, current_field_id.unwrap())?;
                facet_level_may_be_updated = false;
            }
            current_field_id = Some(key.field_id);

            let value = KvReader::from_slice(value);
            let docids_to_delete = value
                .get(DelAdd::Deletion)
                .map(CboRoaringBitmapCodec::bytes_decode)
                .map(|o| o.map_err(heed::Error::Encoding))
                .transpose()?;

            let docids_to_add = value
                .get(DelAdd::Addition)
                .map(CboRoaringBitmapCodec::bytes_decode)
                .map(|o| o.map_err(heed::Error::Encoding))
                .transpose()?;

            let level_size_changed = self.inner.modify(
                wtxn,
                key.field_id,
                key.left_bound,
                docids_to_add.as_ref(),
                docids_to_delete.as_ref(),
            )?;

            if level_size_changed {
                // if a node has been added or removed from the highest level,
                // we may have to update the facet level.
                facet_level_may_be_updated = true;
            }
        }

        if let Some(field_id) = current_field_id {
            if facet_level_may_be_updated {
                self.inner.add_or_delete_level(wtxn, field_id)?;
            }
        }

        Ok(())
    }
}

/// Implementation of `FacetsUpdateIncremental` that is independent of milli's `Index` type
pub struct FacetsUpdateIncrementalInner {
    pub db: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    pub group_size: u8,
    pub min_level_size: u8,
    pub max_group_size: u8,
}
impl FacetsUpdateIncrementalInner {
    /// Find the `FacetGroupKey`/`FacetGroupValue` in the database that
    /// should be used to insert the new `facet_value` for the given `field_id` and `level`
    /// where `level` must be strictly greater than 0.
    ///
    /// For example, when inserting the facet value `4`, there are two possibilities:
    ///
    /// 1. We find a key whose lower bound is 3 followed by a key whose lower bound is 6. Therefore,
    ///    we know that the implicit range of the first key is 3..6, which contains 4.
    ///    So the new facet value belongs in that first key/value pair.
    ///
    /// 2. The first key of the level has a lower bound of `5`. We return this key/value pair
    ///    but will need to change the lowerbound of this key to `4` in order to insert this facet value.
    fn find_insertion_key_value(
        &self,
        field_id: u16,
        level: u8,
        facet_value: &[u8],
        txn: &RoTxn<'_>,
    ) -> Result<(FacetGroupKey<Vec<u8>>, FacetGroupValue)> {
        assert!(level > 0);
        match self.db.get_lower_than_or_equal_to(
            txn,
            &FacetGroupKey { field_id, level, left_bound: facet_value },
        )? {
            Some((key, value)) => {
                if key.level != level {
                    let mut prefix = vec![];
                    prefix.extend_from_slice(&field_id.to_be_bytes());
                    prefix.push(level);

                    let mut iter = self
                        .db
                        .remap_types::<Bytes, FacetGroupValueCodec>()
                        .prefix_iter(txn, prefix.as_slice())?;
                    let (key_bytes, value) = iter.next().unwrap()?;
                    Ok((
                        FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(key_bytes)
                            .map_err(Error::Encoding)?
                            .into_owned(),
                        value,
                    ))
                } else {
                    Ok((key.into_owned(), value))
                }
            }
            None => {
                // We checked that the level is > 0
                // Since all keys of level 1 are greater than those of level 0,
                // we are guaranteed that db.get_lower_than_or_equal_to(key) exists
                panic!()
            }
        }
    }

    /// Insert the given facet value and corresponding document ids in the level 0 of the database
    ///
    /// ## Return
    /// See documentation of `insert_in_level`
    fn modify_in_level_0(
        &self,
        txn: &mut RwTxn<'_>,
        field_id: u16,
        facet_value: &[u8],
        add_docids: Option<&RoaringBitmap>,
        del_docids: Option<&RoaringBitmap>,
    ) -> Result<ModificationResult> {
        let key = FacetGroupKey { field_id, level: 0, left_bound: facet_value };

        let old_value = self.db.get(txn, &key)?;
        match (old_value, add_docids, del_docids) {
            // Addition + deletion on an existing value
            (Some(FacetGroupValue { bitmap, .. }), Some(add_docids), Some(del_docids)) => {
                let value = FacetGroupValue { bitmap: (bitmap - del_docids) | add_docids, size: 1 };
                self.db.put(txn, &key, &value)?;
                Ok(ModificationResult::InPlace)
            }
            // Addition on an existing value
            (Some(FacetGroupValue { bitmap, .. }), Some(add_docids), None) => {
                let value = FacetGroupValue { bitmap: bitmap | add_docids, size: 1 };
                self.db.put(txn, &key, &value)?;
                Ok(ModificationResult::InPlace)
            }
            // Addition of a new value (ignore deletion)
            (None, Some(add_docids), _) => {
                let value = FacetGroupValue { bitmap: add_docids.clone(), size: 1 };
                self.db.put(txn, &key, &value)?;
                Ok(ModificationResult::Insert)
            }
            // Deletion on an existing value, fully delete the key if the resulted value is empty.
            (Some(FacetGroupValue { mut bitmap, .. }), None, Some(del_docids)) => {
                bitmap -= del_docids;
                if bitmap.is_empty() {
                    // Full deletion
                    let mut next_key = None;
                    if let Some((next, _)) =
                        self.db.remap_data_type::<DecodeIgnore>().get_greater_than(txn, &key)?
                    {
                        if next.field_id == field_id && next.level == 0 {
                            next_key = Some(next.left_bound.to_vec());
                        }
                    }
                    self.db.delete(txn, &key)?;
                    Ok(ModificationResult::Remove { next: next_key })
                } else {
                    // Partial deletion
                    let value = FacetGroupValue { bitmap, size: 1 };
                    self.db.put(txn, &key, &value)?;
                    Ok(ModificationResult::InPlace)
                }
            }
            // Otherwise do nothing (None + no addition + deletion == Some + no addition + no deletion == Nothing),
            // may be unreachable at some point.
            (None, None, _) | (Some(_), None, None) => Ok(ModificationResult::Nothing),
        }
    }

    /// Split a level node into two balanced nodes.
    ///
    /// # Return
    /// Returns `ModificationResult::Insert` if the split is successful.
    fn split_group(
        &self,
        txn: &mut RwTxn<'_>,
        field_id: u16,
        level: u8,
        insertion_key: FacetGroupKey<Vec<u8>>,
        insertion_value: FacetGroupValue,
    ) -> Result<ModificationResult> {
        let size_left = insertion_value.size / 2;
        let size_right = insertion_value.size - size_left;

        let level_below = level - 1;

        let start_key = FacetGroupKey {
            field_id,
            level: level_below,
            left_bound: insertion_key.left_bound.as_slice(),
        };

        let mut iter =
            self.db.range(txn, &(start_key..))?.take((size_left as usize) + (size_right as usize));

        let group_left = {
            let mut values_left = RoaringBitmap::new();

            let mut i = 0;
            for next in iter.by_ref() {
                let (_key, value) = next?;
                i += 1;
                values_left |= &value.bitmap;
                if i == size_left {
                    break;
                }
            }

            let key =
                FacetGroupKey { field_id, level, left_bound: insertion_key.left_bound.clone() };
            let value = FacetGroupValue { size: size_left, bitmap: values_left };
            (key, value)
        };

        let group_right = {
            let (
                FacetGroupKey { left_bound: right_left_bound, .. },
                FacetGroupValue { bitmap: mut values_right, .. },
            ) = iter.next().unwrap()?;

            for next in iter.by_ref() {
                let (_, value) = next?;
                values_right |= &value.bitmap;
            }

            let key = FacetGroupKey { field_id, level, left_bound: right_left_bound.to_vec() };
            let value = FacetGroupValue { size: size_right, bitmap: values_right };
            (key, value)
        };
        drop(iter);

        let _ = self.db.delete(txn, &insertion_key.as_ref())?;

        self.db.put(txn, &group_left.0.as_ref(), &group_left.1)?;
        self.db.put(txn, &group_right.0.as_ref(), &group_right.1)?;

        Ok(ModificationResult::Insert)
    }

    /// Remove the docids still present in the related sub-level nodes from the del_docids.
    ///
    /// This process is needed to avoid removing docids from a group node where the docid is present in several sub-nodes.
    fn trim_del_docids<'a>(
        &self,
        txn: &mut RwTxn<'_>,
        field_id: u16,
        level: u8,
        insertion_key: &FacetGroupKey<Vec<u8>>,
        insertion_value_size: usize,
        del_docids: &'a RoaringBitmap,
    ) -> Result<std::borrow::Cow<'a, RoaringBitmap>> {
        let level_below = level - 1;

        let start_key = FacetGroupKey {
            field_id,
            level: level_below,
            left_bound: insertion_key.left_bound.as_slice(),
        };

        let mut del_docids = std::borrow::Cow::Borrowed(del_docids);
        let iter = self.db.range(txn, &(start_key..))?.take(insertion_value_size);
        for next in iter {
            let (_, value) = next?;
            // if a sublevel bitmap as common docids with del_docids,
            // then these docids shouldn't be removed and so, remove them from the deletion list.
            if !value.bitmap.is_disjoint(&del_docids) {
                *del_docids.to_mut() -= value.bitmap;
            }
        }

        Ok(del_docids)
    }

    /// Modify the given facet value and corresponding document ids in all the levels of the database up to the given `level`.
    /// This function works recursively.
    ///
    /// ## Return
    /// Returns the effect of modifying the facet value to the database on the given `level`.
    ///
    fn modify_in_level(
        &self,
        txn: &mut RwTxn<'_>,
        field_id: u16,
        level: u8,
        facet_value: &[u8],
        add_docids: Option<&RoaringBitmap>,
        del_docids: Option<&RoaringBitmap>,
    ) -> Result<ModificationResult> {
        if level == 0 {
            return self.modify_in_level_0(txn, field_id, facet_value, add_docids, del_docids);
        }

        let result =
            self.modify_in_level(txn, field_id, level - 1, facet_value, add_docids, del_docids)?;
        // level below inserted an element

        if let ModificationResult::Nothing = result {
            // if the previous level has not been modified,
            // early return ModificationResult::Nothing.
            return Ok(ModificationResult::Nothing);
        }

        let (insertion_key, insertion_value) =
            self.find_insertion_key_value(field_id, level, facet_value, txn)?;
        let insertion_value_size = insertion_value.size as usize;

        let mut insertion_value_was_modified = false;
        let mut updated_value = insertion_value;

        if let ModificationResult::Insert = result {
            // if a key has been inserted in the sub-level raise the value size.
            updated_value.size += 1;
            insertion_value_was_modified = true;
        } else if let ModificationResult::Remove { .. } = result {
            if updated_value.size <= 1 {
                // if the only remaining node is the one to delete,
                // delete the key instead and early return.
                let is_deleted = self.db.delete(txn, &insertion_key.as_ref())?;
                assert!(is_deleted);
                return Ok(result);
            } else {
                // Reduce the value size
                updated_value.size -= 1;
                insertion_value_was_modified = true;
            }
        }

        let (insertion_key, insertion_key_modification) =
            if let ModificationResult::InPlace = result {
                (insertion_key, ModificationResult::InPlace)
            } else {
                // Inserting or deleting the facet value in the level below resulted in the creation
                // of a new key. Therefore, it may be the case that we need to modify the left bound of the
                // insertion key (see documentation of `find_insertion_key_value` for an example of when that
                // could happen).
                let mut new_insertion_key = insertion_key.clone();
                let mut key_modification = ModificationResult::InPlace;

                if let ModificationResult::Remove { next } | ModificationResult::Reduce { next } =
                    result
                {
                    // if the deleted facet_value is the left_bound of the current node,
                    // the left_bound should be updated reducing the current node.
                    let reduced_range = facet_value == insertion_key.left_bound;
                    if reduced_range {
                        new_insertion_key.left_bound = next.clone().unwrap();
                        key_modification = ModificationResult::Reduce { next };
                    }
                } else if facet_value < insertion_key.left_bound.as_slice() {
                    // if the added facet_value is the under the left_bound of the current node,
                    // the left_bound should be updated expanding the current node.
                    new_insertion_key.left_bound = facet_value.to_vec();
                    key_modification = ModificationResult::Expand;
                }

                if matches!(
                    key_modification,
                    ModificationResult::Expand | ModificationResult::Reduce { .. }
                ) {
                    // if the node should be updated, delete it, it will be recreated using a new key later.
                    let is_deleted = self.db.delete(txn, &insertion_key.as_ref())?;
                    assert!(is_deleted);
                }
                (new_insertion_key, key_modification)
            };

        if updated_value.size < self.max_group_size {
            // If there are docids to delete, trim them avoiding unexpected removal.
            if let Some(del_docids) = del_docids
                .map(|ids| {
                    self.trim_del_docids(
                        txn,
                        field_id,
                        level,
                        &insertion_key,
                        insertion_value_size,
                        ids,
                    )
                })
                .transpose()?
                .filter(|ids| !ids.is_empty())
            {
                updated_value.bitmap -= &*del_docids;
                insertion_value_was_modified = true;
            }

            if let Some(add_docids) = add_docids {
                updated_value.bitmap |= add_docids;
                insertion_value_was_modified = true;
            }

            if insertion_value_was_modified
                || matches!(
                    insertion_key_modification,
                    ModificationResult::Expand | ModificationResult::Reduce { .. }
                )
            {
                // if any modification occurred, insert it in the database.
                self.db.put(txn, &insertion_key.as_ref(), &updated_value)?;
                Ok(insertion_key_modification)
            } else {
                // this case is reachable when a docid is removed from a sub-level node but is still present in another one.
                // For instance, a document containing 2 and 3, if 2 is removed, the docid should remain in the group node [1..4].
                Ok(ModificationResult::Nothing)
            }
        } else {
            // We've increased the group size of the value and realised it has become greater than or equal to `max_group_size`
            // Therefore it must be split into two nodes.
            self.split_group(txn, field_id, level, insertion_key, updated_value)
        }
    }

    /// Modify the given facet value and corresponding document ids in the database.
    /// If no more document ids correspond to the facet value, delete it completely.
    ///
    /// ## Return
    /// Returns `true` if some tree-nodes of the highest level have been removed or added implying a potential
    /// addition or deletion of a facet level.
    /// Otherwise returns `false` if the tree-nodes have been modified in place.
    pub fn modify(
        &self,
        txn: &mut RwTxn<'_>,
        field_id: u16,
        facet_value: &[u8],
        add_docids: Option<&RoaringBitmap>,
        del_docids: Option<&RoaringBitmap>,
    ) -> Result<bool> {
        if add_docids.map_or(true, RoaringBitmap::is_empty)
            && del_docids.map_or(true, RoaringBitmap::is_empty)
        {
            return Ok(false);
        }

        let highest_level = get_highest_level(txn, self.db, field_id)?;

        let result = self.modify_in_level(
            txn,
            field_id,
            highest_level,
            facet_value,
            add_docids,
            del_docids,
        )?;
        match result {
            ModificationResult::InPlace
            | ModificationResult::Expand
            | ModificationResult::Nothing
            | ModificationResult::Reduce { .. } => Ok(false),
            ModificationResult::Insert | ModificationResult::Remove { .. } => Ok(true),
        }
    }

    /// Check whether the highest level has exceeded `min_level_size` * `self.group_size`.
    /// If it has, we must build an addition level above it.
    /// Then check whether the highest level is under `min_level_size`.
    /// If it has, we must remove the complete level.
    pub(crate) fn add_or_delete_level(&self, txn: &mut RwTxn<'_>, field_id: u16) -> Result<()> {
        let highest_level = get_highest_level(txn, self.db, field_id)?;
        let mut highest_level_prefix = vec![];
        highest_level_prefix.extend_from_slice(&field_id.to_be_bytes());
        highest_level_prefix.push(highest_level);

        let size_highest_level =
            self.db.remap_types::<Bytes, Bytes>().prefix_iter(txn, &highest_level_prefix)?.count();

        if size_highest_level >= self.group_size as usize * self.min_level_size as usize {
            self.add_level(txn, field_id, highest_level, &highest_level_prefix, size_highest_level)
        } else if size_highest_level < self.min_level_size as usize && highest_level != 0 {
            self.delete_level(txn, &highest_level_prefix)
        } else {
            Ok(())
        }
    }

    /// Delete a level.
    fn delete_level(&self, txn: &mut RwTxn<'_>, highest_level_prefix: &[u8]) -> Result<()> {
        let mut to_delete = vec![];
        let mut iter =
            self.db.remap_types::<Bytes, Bytes>().prefix_iter(txn, highest_level_prefix)?;
        for el in iter.by_ref() {
            let (k, _) = el?;
            to_delete.push(
                FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(k)
                    .map_err(Error::Encoding)?
                    .into_owned(),
            );
        }
        drop(iter);
        for k in to_delete {
            self.db.delete(txn, &k.as_ref())?;
        }
        Ok(())
    }

    /// Build an additional level for the field id.
    fn add_level(
        &self,
        txn: &mut RwTxn<'_>,
        field_id: u16,
        highest_level: u8,
        highest_level_prefix: &[u8],
        size_highest_level: usize,
    ) -> Result<()> {
        let mut groups_iter = self
            .db
            .remap_types::<Bytes, FacetGroupValueCodec>()
            .prefix_iter(txn, highest_level_prefix)?;

        let nbr_new_groups = size_highest_level / self.group_size as usize;
        let nbr_leftover_elements = size_highest_level % self.group_size as usize;

        let mut to_add = vec![];
        for _ in 0..nbr_new_groups {
            let mut first_key = None;
            let mut values = RoaringBitmap::new();
            for _ in 0..self.group_size {
                let (key_bytes, value_i) = groups_iter.next().unwrap()?;
                let key_i = FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(key_bytes)
                    .map_err(Error::Encoding)?;

                if first_key.is_none() {
                    first_key = Some(key_i);
                }
                values |= value_i.bitmap;
            }
            let key = FacetGroupKey {
                field_id,
                level: highest_level + 1,
                left_bound: first_key.unwrap().left_bound,
            };
            let value = FacetGroupValue { size: self.group_size, bitmap: values };
            to_add.push((key.into_owned(), value));
        }
        // now we add the rest of the level, in case its size is > group_size * min_level_size
        // this can indeed happen if the min_level_size parameter changes between two calls to `insert`
        if nbr_leftover_elements > 0 {
            let mut first_key = None;
            let mut values = RoaringBitmap::new();
            for _ in 0..nbr_leftover_elements {
                let (key_bytes, value_i) = groups_iter.next().unwrap()?;
                let key_i = FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(key_bytes)
                    .map_err(Error::Encoding)?;

                if first_key.is_none() {
                    first_key = Some(key_i);
                }
                values |= value_i.bitmap;
            }
            let key = FacetGroupKey {
                field_id,
                level: highest_level + 1,
                left_bound: first_key.unwrap().left_bound,
            };
            // Note: nbr_leftover_elements can be casted to a u8 since it is bounded by `max_group_size`
            // when it is created above.
            let value = FacetGroupValue { size: nbr_leftover_elements as u8, bitmap: values };
            to_add.push((key.into_owned(), value));
        }

        drop(groups_iter);
        for (key, value) in to_add {
            self.db.put(txn, &key.as_ref(), &value)?;
        }
        Ok(())
    }
}

impl<'a> FacetGroupKey<&'a [u8]> {
    pub fn into_owned(self) -> FacetGroupKey<Vec<u8>> {
        FacetGroupKey {
            field_id: self.field_id,
            level: self.level,
            left_bound: self.left_bound.to_vec(),
        }
    }
}

impl FacetGroupKey<Vec<u8>> {
    pub fn as_ref(&self) -> FacetGroupKey<&[u8]> {
        FacetGroupKey {
            field_id: self.field_id,
            level: self.level,
            left_bound: self.left_bound.as_slice(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use roaring::RoaringBitmap;

    use crate::heed_codec::facet::OrderedF64Codec;
    use crate::heed_codec::StrRefCodec;
    use crate::milli_snap;
    use crate::update::facet::test_helpers::FacetIndex;

    #[test]
    fn append() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        for i in 0..256u16 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        let txn = index.env.read_txn().unwrap();
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"));
    }
    #[test]
    fn many_field_ids_append() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        for i in 0..256u16 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        for i in 0..256u16 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 2, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        for i in 0..256u16 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 1, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        let txn = index.env.read_txn().unwrap();
        index.verify_structure_validity(&txn, 0);
        index.verify_structure_validity(&txn, 1);
        index.verify_structure_validity(&txn, 2);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"));
    }
    #[test]
    fn many_field_ids_prepend() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        for i in (0..256).rev() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        for i in (0..256).rev() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 2, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        for i in (0..256).rev() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 1, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        let txn = index.env.read_txn().unwrap();
        index.verify_structure_validity(&txn, 0);
        index.verify_structure_validity(&txn, 1);
        index.verify_structure_validity(&txn, 2);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn prepend() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();

        for i in (0..256).rev() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
        }

        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn shuffled() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();

        let mut keys = (0..256).collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for key in keys {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            index.insert(&mut txn, 0, &(key as f64), &bitmap);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn merge_values() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();

        let mut keys = (0..256).collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for key in keys {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(rng.gen_range(256..512));
            index.verify_structure_validity(&txn, 0);
            index.insert(&mut txn, 0, &(key as f64), &bitmap);
        }

        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn delete_from_end() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();
        for i in 0..256 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            index.verify_structure_validity(&txn, 0);
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
        }

        for i in (200..256).rev() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 200);
        let mut txn = index.env.write_txn().unwrap();

        for i in (150..200).rev() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 150);
        let mut txn = index.env.write_txn().unwrap();
        for i in (100..150).rev() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 100);
        let mut txn = index.env.write_txn().unwrap();
        for i in (17..100).rev() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 17);
        let mut txn = index.env.write_txn().unwrap();
        for i in (15..17).rev() {
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 15);
        let mut txn = index.env.write_txn().unwrap();
        for i in (0..15).rev() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 0);
    }

    #[test]
    fn delete_from_start() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();

        for i in 0..256 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            index.verify_structure_validity(&txn, 0);
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
        }

        for i in 0..128 {
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 127);
        let mut txn = index.env.write_txn().unwrap();
        for i in 128..216 {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 215);
        let mut txn = index.env.write_txn().unwrap();
        for i in 216..256 {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 255);
    }

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn delete_shuffled() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();
        for i in 0..256 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            index.verify_structure_validity(&txn, 0);
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
        }

        let mut keys = (0..256).collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for i in 0..128 {
            let key = keys[i];
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(key as f64), key as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 127);
        let mut txn = index.env.write_txn().unwrap();
        for i in 128..216 {
            let key = keys[i];
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(key as f64), key as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        let mut txn = index.env.write_txn().unwrap();
        milli_snap!(format!("{index}"), 215);
        for i in 216..256 {
            let key = keys[i];
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(key as f64), key as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 255);
    }

    #[test]
    fn in_place_level0_insert() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();

        let mut keys = (0..16).collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);
        for i in 0..4 {
            for &key in keys.iter() {
                let mut bitmap = RoaringBitmap::new();
                bitmap.insert(rng.gen_range(i * 256..(i + 1) * 256));
                index.verify_structure_validity(&txn, 0);
                index.insert(&mut txn, 0, &(key as f64), &bitmap);
            }
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn in_place_level0_delete() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();

        let mut keys = (0..64).collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for &key in keys.iter() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(key + 100);
            index.verify_structure_validity(&txn, 0);

            index.insert(&mut txn, 0, &(key as f64), &bitmap);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), "before_delete");

        let mut txn = index.env.write_txn().unwrap();

        for &key in keys.iter() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(key as f64), key + 100);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), "after_delete");
    }

    #[test]
    fn shuffle_merge_string_and_delete() {
        let index = FacetIndex::<StrRefCodec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();

        let mut keys = (1000..1064).collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for &key in keys.iter() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(key + 100);
            index.verify_structure_validity(&txn, 0);
            index.insert(&mut txn, 0, &format!("{key:x}").as_str(), &bitmap);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), "before_delete");

        let mut txn = index.env.write_txn().unwrap();

        for &key in keys.iter() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &format!("{key:x}").as_str(), key + 100);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), "after_delete");
    }
}

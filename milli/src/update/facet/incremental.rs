use std::collections::HashMap;
use std::fs::File;

use heed::types::{ByteSlice, DecodeIgnore};
use heed::{BytesDecode, Error, RoTxn, RwTxn};
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValue, FacetGroupValueCodec,
};
use crate::heed_codec::ByteSliceRefCodec;
use crate::search::facet::get_highest_level;
use crate::{CboRoaringBitmapCodec, FieldId, Index, Result};

enum InsertionResult {
    InPlace,
    Expand,
    Insert,
}
enum DeletionResult {
    InPlace,
    Reduce { next: Option<Vec<u8>> },
    Remove { next: Option<Vec<u8>> },
}

/// Algorithm to incrementally insert and delete elememts into the
/// `facet_id_(string/f64)_docids` databases.
///
/// Rhe `faceted_documents_ids` value in the main database of `Index`
/// is also updated to contain the new set of faceted documents.
pub struct FacetsUpdateIncremental<'i> {
    index: &'i Index,
    inner: FacetsUpdateIncrementalInner,
    facet_type: FacetType,
    new_data: grenad::Reader<File>,
}

impl<'i> FacetsUpdateIncremental<'i> {
    pub fn new(
        index: &'i Index,
        facet_type: FacetType,
        new_data: grenad::Reader<File>,
        group_size: u8,
        min_level_size: u8,
        max_group_size: u8,
    ) -> Self {
        FacetsUpdateIncremental {
            index,
            inner: FacetsUpdateIncrementalInner {
                db: match facet_type {
                    FacetType::String => index
                        .facet_id_string_docids
                        .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
                    FacetType::Number => index
                        .facet_id_f64_docids
                        .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
                },
                group_size,
                max_group_size,
                min_level_size,
            },
            facet_type,
            new_data,
        }
    }

    pub fn execute(self, wtxn: &'i mut RwTxn) -> crate::Result<()> {
        let mut new_faceted_docids = HashMap::<FieldId, RoaringBitmap>::default();

        let mut cursor = self.new_data.into_cursor()?;
        while let Some((key, value)) = cursor.move_on_next()? {
            let key = FacetGroupKeyCodec::<ByteSliceRefCodec>::bytes_decode(key)
                .ok_or(heed::Error::Encoding)?;
            let docids = CboRoaringBitmapCodec::bytes_decode(value).ok_or(heed::Error::Encoding)?;
            self.inner.insert(wtxn, key.field_id, key.left_bound, &docids)?;
            *new_faceted_docids.entry(key.field_id).or_default() |= docids;
        }

        for (field_id, new_docids) in new_faceted_docids {
            let mut docids = self.index.faceted_documents_ids(wtxn, field_id, self.facet_type)?;
            docids |= new_docids;
            self.index.put_faceted_documents_ids(wtxn, field_id, self.facet_type, &docids)?;
        }
        Ok(())
    }
}

/// Implementation of `FacetsUpdateIncremental` that is independent of milli's `Index` type
pub struct FacetsUpdateIncrementalInner {
    pub db: heed::Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
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
        txn: &RoTxn,
    ) -> Result<(FacetGroupKey<Vec<u8>>, FacetGroupValue)> {
        assert!(level > 0);

        let mut prefix = vec![];
        prefix.extend_from_slice(&field_id.to_be_bytes());
        prefix.push(level);
        prefix.extend_from_slice(facet_value);

        let mut prefix_iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(txn, prefix.as_slice())?;
        if let Some(e) = prefix_iter.next() {
            let (key_bytes, value) = e?;
            Ok((
                FacetGroupKeyCodec::<ByteSliceRefCodec>::bytes_decode(key_bytes)
                    .ok_or(Error::Encoding)?
                    .into_owned(),
                value,
            ))
        } else {
            let key = FacetGroupKey { field_id, level, left_bound: facet_value };
            match self.db.get_lower_than(txn, &key)? {
                Some((key, value)) => {
                    if key.level != level {
                        let mut prefix = vec![];
                        prefix.extend_from_slice(&field_id.to_be_bytes());
                        prefix.push(level);

                        let mut iter = self
                            .db
                            .as_polymorph()
                            .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(
                                txn,
                                prefix.as_slice(),
                            )?;
                        let (key_bytes, value) = iter.next().unwrap()?;
                        Ok((
                            FacetGroupKeyCodec::<ByteSliceRefCodec>::bytes_decode(key_bytes)
                                .ok_or(Error::Encoding)?
                                .into_owned(),
                            value,
                        ))
                    } else {
                        Ok((key.into_owned(), value))
                    }
                }
                None => panic!(),
            }
        }
    }

    /// Insert the given facet value and corresponding document ids in the level 0 of the database
    ///
    /// ## Return
    /// See documentation of `insert_in_level`
    fn insert_in_level_0<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        facet_value: &[u8],
        docids: &RoaringBitmap,
    ) -> Result<InsertionResult> {
        let key = FacetGroupKey { field_id, level: 0, left_bound: facet_value };
        let value = FacetGroupValue { bitmap: docids.clone(), size: 1 };

        let mut level0_prefix = vec![];
        level0_prefix.extend_from_slice(&field_id.to_be_bytes());
        level0_prefix.push(0);

        let mut iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, DecodeIgnore>(txn, &level0_prefix)?;

        if iter.next().is_none() {
            drop(iter);
            self.db.put(txn, &key, &value)?;
            Ok(InsertionResult::Insert)
        } else {
            drop(iter);
            let old_value = self.db.get(txn, &key)?;
            match old_value {
                Some(mut updated_value) => {
                    // now merge the two
                    updated_value.bitmap |= value.bitmap;
                    self.db.put(txn, &key, &updated_value)?;
                    Ok(InsertionResult::InPlace)
                }
                None => {
                    self.db.put(txn, &key, &value)?;
                    Ok(InsertionResult::Insert)
                }
            }
        }
    }

    /// Insert the given facet value  and corresponding document ids in all the levels of the database up to the given `level`.
    /// This function works recursively.
    ///
    /// ## Return
    /// Returns the effect of adding the facet value to the database on the given `level`.
    ///
    /// - `InsertionResult::InPlace` means that inserting the `facet_value` into the `level` did not have
    /// an effect on the number of keys in that level. Therefore, it did not increase the number of children
    /// of the parent node.
    ///
    /// - `InsertionResult::Insert` means that inserting the `facet_value` into the `level` resulted
    /// in the addition of a new key in that level, and that therefore the number of children
    /// of the parent node should be incremented.
    fn insert_in_level<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        level: u8,
        facet_value: &[u8],
        docids: &RoaringBitmap,
    ) -> Result<InsertionResult> {
        if level == 0 {
            return self.insert_in_level_0(txn, field_id, facet_value, docids);
        }

        let max_group_size = self.max_group_size;

        let result = self.insert_in_level(txn, field_id, level - 1, facet_value, docids)?;
        // level below inserted an element

        let (insertion_key, insertion_value) =
            self.find_insertion_key_value(field_id, level, facet_value, txn)?;

        match result {
            // because we know that we inserted in place, the facet_value is not a new one
            // thus it doesn't extend a group, and thus the insertion key computed above is
            // still correct
            InsertionResult::InPlace => {
                let mut updated_value = insertion_value;
                updated_value.bitmap |= docids;
                self.db.put(txn, &insertion_key.as_ref(), &updated_value)?;

                return Ok(InsertionResult::InPlace);
            }
            InsertionResult::Expand => {}
            InsertionResult::Insert => {}
        }

        // Here we know that inserting the facet value in the level below resulted in the creation
        // of a new key. Therefore, it may be the case that we need to modify the left bound of the
        // insertion key (see documentation of `find_insertion_key_value` for an example of when that
        // could happen).
        let (insertion_key, insertion_key_was_modified) = {
            let mut new_insertion_key = insertion_key.clone();
            let mut key_should_be_modified = false;

            if facet_value < insertion_key.left_bound.as_slice() {
                new_insertion_key.left_bound = facet_value.to_vec();
                key_should_be_modified = true;
            }
            if key_should_be_modified {
                let is_deleted = self.db.delete(txn, &insertion_key.as_ref())?;
                assert!(is_deleted);
                self.db.put(txn, &new_insertion_key.as_ref(), &insertion_value)?;
            }
            (new_insertion_key, key_should_be_modified)
        };
        // Now we know that the insertion key contains the `facet_value`.

        // We still need to update the insertion value by:
        // 1. Incrementing the number of children (since the recursive call returned `InsertionResult::Insert`)
        // 2. Merge the previous docids with the new one
        let mut updated_value = insertion_value;

        if matches!(result, InsertionResult::Insert) {
            updated_value.size += 1;
        }

        if updated_value.size < max_group_size {
            updated_value.bitmap |= docids;
            self.db.put(txn, &insertion_key.as_ref(), &updated_value)?;
            if insertion_key_was_modified {
                return Ok(InsertionResult::Expand);
            } else {
                return Ok(InsertionResult::InPlace);
            }
        }

        // We've increased the group size of the value and realised it has become greater than or equal to `max_group_size`
        // Therefore it must be split into two nodes.

        let size_left = updated_value.size / 2;
        let size_right = updated_value.size - size_left;

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

        Ok(InsertionResult::Insert)
    }

    /// Insert the given facet value and corresponding document ids in the database.
    pub fn insert<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        facet_value: &[u8],
        docids: &RoaringBitmap,
    ) -> Result<()> {
        if docids.is_empty() {
            return Ok(());
        }
        let group_size = self.group_size;

        let highest_level = get_highest_level(txn, self.db, field_id)?;

        let result = self.insert_in_level(txn, field_id, highest_level, facet_value, docids)?;
        match result {
            InsertionResult::InPlace => return Ok(()),
            InsertionResult::Expand => return Ok(()),
            InsertionResult::Insert => {}
        }

        // Here we check whether the highest level has exceeded `min_level_size` * `self.group_size`.
        // If it has, we must build an addition level above it.

        let mut highest_level_prefix = vec![];
        highest_level_prefix.extend_from_slice(&field_id.to_be_bytes());
        highest_level_prefix.push(highest_level);

        let size_highest_level = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, ByteSlice>(txn, &highest_level_prefix)?
            .count();

        if size_highest_level < self.group_size as usize * self.min_level_size as usize {
            return Ok(());
        }

        let mut groups_iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(txn, &highest_level_prefix)?;

        let nbr_new_groups = size_highest_level / self.group_size as usize;
        let nbr_leftover_elements = size_highest_level % self.group_size as usize;

        let mut to_add = vec![];
        for _ in 0..nbr_new_groups {
            let mut first_key = None;
            let mut values = RoaringBitmap::new();
            for _ in 0..group_size {
                let (key_bytes, value_i) = groups_iter.next().unwrap()?;
                let key_i = FacetGroupKeyCodec::<ByteSliceRefCodec>::bytes_decode(key_bytes)
                    .ok_or(Error::Encoding)?;

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
            let value = FacetGroupValue { size: group_size, bitmap: values };
            to_add.push((key.into_owned(), value));
        }
        // now we add the rest of the level, in case its size is > group_size * min_level_size
        // this can indeed happen if the min_level_size parameter changes between two calls to `insert`
        if nbr_leftover_elements > 0 {
            let mut first_key = None;
            let mut values = RoaringBitmap::new();
            for _ in 0..nbr_leftover_elements {
                let (key_bytes, value_i) = groups_iter.next().unwrap()?;
                let key_i = FacetGroupKeyCodec::<ByteSliceRefCodec>::bytes_decode(key_bytes)
                    .ok_or(Error::Encoding)?;

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
            let value = FacetGroupValue { size: nbr_leftover_elements as u8, bitmap: values };
            to_add.push((key.into_owned(), value));
        }

        drop(groups_iter);
        for (key, value) in to_add {
            self.db.put(txn, &key.as_ref(), &value)?;
        }
        Ok(())
    }

    /// Delete the given document id from the given facet value in the database, from level 0 to the
    /// the given level.
    ///
    /// ## Return
    /// Returns the effect of removing the document id from the database on the given `level`.
    ///
    /// - `DeletionResult::InPlace` means that deleting the document id did not have
    /// an effect on the keys in that level.
    ///
    /// - `DeletionResult::Reduce` means that deleting the document id resulted in a change in the
    /// number of keys in the level. For example, removing a document id from the facet value `3` could
    /// cause it to have no corresponding document in level 0 anymore, and therefore the key was deleted
    /// entirely. In that case, `DeletionResult::Remove` is returned. The parent of the deleted key must
    /// then adjust its group size. If its group size falls to 0, then it will need to be deleted as well.
    ///
    /// - `DeletionResult::Reduce` means that deleting the document id resulted in a change in the
    /// bounds of the keys of the level. For example, removing a document id from the facet value
    /// `3` might have caused the facet value `3` to have no corresponding document in level 0. Therefore,
    /// in level 1, the key with the left bound `3` had to be changed to the next facet value (e.g. 4).
    /// In that case `DeletionResult::Reduce` is returned. The parent of the reduced key may need to adjust
    /// its left bound as well.
    fn delete_in_level<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        level: u8,
        facet_value: &[u8],
        docids: &RoaringBitmap,
    ) -> Result<DeletionResult> {
        if level == 0 {
            return self.delete_in_level_0(txn, field_id, facet_value, docids);
        }
        let (deletion_key, mut bitmap) =
            self.find_insertion_key_value(field_id, level, facet_value, txn)?;

        let result = self.delete_in_level(txn, field_id, level - 1, facet_value, docids)?;

        let mut decrease_size = false;
        let next_key = match result {
            DeletionResult::InPlace => {
                bitmap.bitmap -= docids;
                self.db.put(txn, &deletion_key.as_ref(), &bitmap)?;
                return Ok(DeletionResult::InPlace);
            }
            DeletionResult::Reduce { next } => next,
            DeletionResult::Remove { next } => {
                decrease_size = true;
                next
            }
        };
        // If either DeletionResult::Reduce or DeletionResult::Remove was returned,
        // then we may need to adjust the left_bound of the deletion key.

        // If DeletionResult::Remove was returned, then we need to decrease the group
        // size of the deletion key.
        let mut updated_value = bitmap;
        if decrease_size {
            updated_value.size -= 1;
        }

        if updated_value.size == 0 {
            self.db.delete(txn, &deletion_key.as_ref())?;
            Ok(DeletionResult::Remove { next: next_key })
        } else {
            let mut updated_deletion_key = deletion_key.clone();
            let reduced_range = facet_value == deletion_key.left_bound;
            if reduced_range {
                updated_deletion_key.left_bound = next_key.clone().unwrap();
            }
            updated_value.bitmap -= docids;
            let _ = self.db.delete(txn, &deletion_key.as_ref())?;
            self.db.put(txn, &updated_deletion_key.as_ref(), &updated_value)?;
            if reduced_range {
                Ok(DeletionResult::Reduce { next: next_key })
            } else {
                Ok(DeletionResult::InPlace)
            }
        }
    }

    fn delete_in_level_0<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        facet_value: &[u8],
        docids: &RoaringBitmap,
    ) -> Result<DeletionResult> {
        let key = FacetGroupKey { field_id, level: 0, left_bound: facet_value };
        let mut bitmap = self.db.get(txn, &key)?.unwrap().bitmap;
        bitmap -= docids;

        if bitmap.is_empty() {
            let mut next_key = None;
            if let Some((next, _)) =
                self.db.remap_data_type::<DecodeIgnore>().get_greater_than(txn, &key)?
            {
                if next.field_id == field_id && next.level == 0 {
                    next_key = Some(next.left_bound.to_vec());
                }
            }
            self.db.delete(txn, &key)?;
            Ok(DeletionResult::Remove { next: next_key })
        } else {
            self.db.put(txn, &key, &FacetGroupValue { size: 1, bitmap })?;
            Ok(DeletionResult::InPlace)
        }
    }

    pub fn delete<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        facet_value: &[u8],
        docids: &RoaringBitmap,
    ) -> Result<()> {
        if self
            .db
            .remap_data_type::<DecodeIgnore>()
            .get(txn, &FacetGroupKey { field_id, level: 0, left_bound: facet_value })?
            .is_none()
        {
            return Ok(());
        }
        let highest_level = get_highest_level(txn, self.db, field_id)?;

        let result = self.delete_in_level(txn, field_id, highest_level, facet_value, docids)?;
        match result {
            DeletionResult::InPlace => return Ok(()),
            DeletionResult::Reduce { .. } => return Ok(()),
            DeletionResult::Remove { .. } => {}
        }

        // if we either removed a key from the highest level, its size may have fallen
        // below `min_level_size`, in which case we need to remove the entire level

        let mut highest_level_prefix = vec![];
        highest_level_prefix.extend_from_slice(&field_id.to_be_bytes());
        highest_level_prefix.push(highest_level);

        if highest_level == 0
            || self
                .db
                .as_polymorph()
                .prefix_iter::<_, ByteSlice, ByteSlice>(txn, &highest_level_prefix)?
                .count()
                >= self.min_level_size as usize
        {
            return Ok(());
        }
        let mut to_delete = vec![];
        let mut iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, ByteSlice>(txn, &highest_level_prefix)?;
        for el in iter.by_ref() {
            let (k, _) = el?;
            to_delete.push(
                FacetGroupKeyCodec::<ByteSliceRefCodec>::bytes_decode(k)
                    .ok_or(Error::Encoding)?
                    .into_owned(),
            );
        }
        drop(iter);
        for k in to_delete {
            self.db.delete(txn, &k.as_ref())?;
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
    use crate::update::facet::tests::FacetIndex;

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
        for i in (0..256).into_iter().rev() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        for i in (0..256).into_iter().rev() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 2, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        for i in (0..256).into_iter().rev() {
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

        for i in (0..256).into_iter().rev() {
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

        let mut keys = (0..256).into_iter().collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for (_i, key) in keys.into_iter().enumerate() {
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

        let mut keys = (0..256).into_iter().collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for (_i, key) in keys.into_iter().enumerate() {
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

        for i in (200..256).into_iter().rev() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 200);
        let mut txn = index.env.write_txn().unwrap();

        for i in (150..200).into_iter().rev() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 150);
        let mut txn = index.env.write_txn().unwrap();
        for i in (100..150).into_iter().rev() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 100);
        let mut txn = index.env.write_txn().unwrap();
        for i in (17..100).into_iter().rev() {
            index.verify_structure_validity(&txn, 0);
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 17);
        let mut txn = index.env.write_txn().unwrap();
        for i in (15..17).into_iter().rev() {
            index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
        }
        index.verify_structure_validity(&txn, 0);
        txn.commit().unwrap();
        milli_snap!(format!("{index}"), 15);
        let mut txn = index.env.write_txn().unwrap();
        for i in (0..15).into_iter().rev() {
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
    fn delete_shuffled() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
        let mut txn = index.env.write_txn().unwrap();
        for i in 0..256 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            index.verify_structure_validity(&txn, 0);
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
        }

        let mut keys = (0..256).into_iter().collect::<Vec<_>>();
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

        let mut keys = (0..16).into_iter().collect::<Vec<_>>();
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

        let mut keys = (0..64).into_iter().collect::<Vec<_>>();
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

        let mut keys = (1000..1064).into_iter().collect::<Vec<_>>();
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

// fuzz tests
#[cfg(all(test, fuzzing))]
/**
Fuzz test for the incremental indxer.

The fuzz test uses fuzzcheck, a coverage-guided fuzzer.
See https://github.com/loiclec/fuzzcheck-rs and https://fuzzcheck.neocities.org
for more information.

It is only run when using the `cargo fuzzcheck` command line tool, which can be installed with:
```sh
cargo install cargo-fuzzcheck
```
To start the fuzz test, run (from the base folder or from milli/):
```sh
cargo fuzzcheck update::facet::incremental::fuzz::fuzz
```
and wait a couple minutes to make sure the code was thoroughly tested, then
hit `Ctrl-C` to stop the fuzzer. The corpus generated by the fuzzer is located in milli/fuzz.

To work on this module with rust-analyzer working properly, add the following to your .cargo/config.toml file:
```toml
[build]
rustflags = ["--cfg",  "fuzzing"]
```

The fuzz test generates sequences of additions and deletions to the facet database and
ensures that:
1. its structure is still internally valid
2. its content is the same as a trivially correct implementation of the same database
*/
mod fuzz {
    use std::borrow::Cow;
    use std::collections::{BTreeMap, HashMap};
    use std::convert::TryFrom;
    use std::iter::FromIterator;
    use std::rc::Rc;

    use fuzzcheck::mutators::integer::U8Mutator;
    use fuzzcheck::mutators::integer_within_range::{U16WithinRangeMutator, U8WithinRangeMutator};
    use fuzzcheck::mutators::vector::VecMutator;
    use fuzzcheck::DefaultMutator;
    use heed::BytesEncode;
    use roaring::RoaringBitmap;
    use tempfile::TempDir;

    use super::*;
    use crate::update::facet::tests::FacetIndex;

    struct NEU16Codec;
    impl<'a> BytesEncode<'a> for NEU16Codec {
        type EItem = u16;
        #[no_coverage]
        fn bytes_encode(item: &'a Self::EItem) -> Option<std::borrow::Cow<'a, [u8]>> {
            Some(Cow::Owned(item.to_be_bytes().to_vec()))
        }
    }
    impl<'a> BytesDecode<'a> for NEU16Codec {
        type DItem = u16;
        #[no_coverage]
        fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
            let bytes = <[u8; 2]>::try_from(&bytes[0..=1]).unwrap();
            Some(u16::from_be_bytes(bytes))
        }
    }

    #[derive(Default)]
    pub struct TrivialDatabase<T> {
        pub elements: BTreeMap<u16, BTreeMap<T, RoaringBitmap>>,
    }
    impl<T> TrivialDatabase<T>
    where
        T: Ord + Clone + Copy + Eq + std::fmt::Debug,
    {
        #[no_coverage]
        pub fn insert(&mut self, field_id: u16, new_key: T, new_values: &RoaringBitmap) {
            if new_values.is_empty() {
                return;
            }
            let values_field_id = self.elements.entry(field_id).or_default();
            let values = values_field_id.entry(new_key).or_default();
            *values |= new_values;
        }
        #[no_coverage]
        pub fn delete(&mut self, field_id: u16, key: T, values_to_remove: &RoaringBitmap) {
            if let Some(values_field_id) = self.elements.get_mut(&field_id) {
                if let Some(values) = values_field_id.get_mut(&key) {
                    *values -= values_to_remove;
                    if values.is_empty() {
                        values_field_id.remove(&key);
                    }
                }
                if values_field_id.is_empty() {
                    self.elements.remove(&field_id);
                }
            }
        }
    }
    #[derive(Clone, DefaultMutator, serde::Serialize, serde::Deserialize)]
    struct Operation<Key> {
        key: Key,
        #[field_mutator(U8WithinRangeMutator = { U8WithinRangeMutator::new(..32) })]
        group_size: u8,
        #[field_mutator(U8WithinRangeMutator = { U8WithinRangeMutator::new(..32) })]
        max_group_size: u8,
        #[field_mutator(U8WithinRangeMutator = { U8WithinRangeMutator::new(..32) })]
        min_level_size: u8,
        #[field_mutator(U16WithinRangeMutator = { U16WithinRangeMutator::new(..=3) })]
        field_id: u16,
        kind: OperationKind,
    }
    #[derive(Clone, DefaultMutator, serde::Serialize, serde::Deserialize)]
    enum OperationKind {
        Insert(
            #[field_mutator(VecMutator<u8, U8Mutator> = { VecMutator::new(U8Mutator::default(), 0 ..= 10) })]
             Vec<u8>,
        ),
        Delete(
            #[field_mutator(VecMutator<u8, U8Mutator> = { VecMutator::new(U8Mutator::default(), 0 ..= 10) })]
             Vec<u8>,
        ),
    }

    #[no_coverage]
    fn compare_with_trivial_database(tempdir: Rc<TempDir>, operations: &[Operation<u16>]) {
        let index = FacetIndex::<NEU16Codec>::open_from_tempdir(tempdir, 4, 8, 5); // dummy params, they'll be overwritten
                                                                                   // let mut txn = index.env.write_txn().unwrap();
        let mut txn = index.env.write_txn().unwrap();

        let mut trivial_db = TrivialDatabase::<u16>::default();
        let mut value_to_keys = HashMap::<u8, Vec<u16>>::new();
        for Operation { key, group_size, max_group_size, min_level_size, field_id, kind } in
            operations
        {
            index.set_group_size(*group_size);
            index.set_max_group_size(*max_group_size);
            index.set_min_level_size(*min_level_size);
            match kind {
                OperationKind::Insert(values) => {
                    let mut bitmap = RoaringBitmap::new();
                    for value in values {
                        bitmap.insert(*value as u32);
                        value_to_keys.entry(*value).or_default().push(*key);
                    }
                    index.insert(&mut txn, *field_id, key, &bitmap);
                    trivial_db.insert(*field_id, *key, &bitmap);
                }
                OperationKind::Delete(values) => {
                    let values = RoaringBitmap::from_iter(values.iter().copied().map(|x| x as u32));
                    let mut values_per_key = HashMap::new();

                    for value in values {
                        if let Some(keys) = value_to_keys.get(&(value as u8)) {
                            for key in keys {
                                let values: &mut RoaringBitmap =
                                    values_per_key.entry(key).or_default();
                                values.insert(value);
                            }
                        }
                    }
                    for (key, values) in values_per_key {
                        index.delete(&mut txn, *field_id, &key, &values);
                        trivial_db.delete(*field_id, *key, &values);
                    }
                }
            }
        }

        for (field_id, values_field_id) in trivial_db.elements.iter() {
            let level0iter = index
                .content
                .as_polymorph()
                .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(
                    &mut txn,
                    &field_id.to_be_bytes(),
                )
                .unwrap();

            for ((key, values), group) in values_field_id.iter().zip(level0iter) {
                let (group_key, group_values) = group.unwrap();
                let group_key = FacetGroupKeyCodec::<NEU16Codec>::bytes_decode(group_key).unwrap();
                assert_eq!(key, &group_key.left_bound);
                assert_eq!(values, &group_values.bitmap);
            }
        }

        for (field_id, values_field_id) in trivial_db.elements.iter() {
            let level0iter = index
                .content
                .as_polymorph()
                .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(&txn, &field_id.to_be_bytes())
                .unwrap();

            for ((key, values), group) in values_field_id.iter().zip(level0iter) {
                let (group_key, group_values) = group.unwrap();
                let group_key = FacetGroupKeyCodec::<NEU16Codec>::bytes_decode(group_key).unwrap();
                assert_eq!(key, &group_key.left_bound);
                assert_eq!(values, &group_values.bitmap);
            }
            index.verify_structure_validity(&txn, *field_id);
        }
        txn.abort().unwrap();
    }

    #[test]
    #[no_coverage]
    fn fuzz() {
        let tempdir = Rc::new(TempDir::new().unwrap());
        let tempdir_cloned = tempdir.clone();
        let result = fuzzcheck::fuzz_test(move |operations: &[Operation<u16>]| {
            compare_with_trivial_database(tempdir_cloned.clone(), operations)
        })
        .default_mutator()
        .serde_serializer()
        .default_sensor_and_pool_with_custom_filter(|file, function| {
            file == std::path::Path::new("milli/src/update/facet/incremental.rs")
                && !function.contains("serde")
                && !function.contains("tests::")
                && !function.contains("fuzz::")
                && !function.contains("display_bitmap")
        })
        .arguments_from_cargo_fuzzcheck()
        .launch();
        assert!(!result.found_test_failure);
    }

    #[test]
    #[no_coverage]
    fn reproduce_bug1() {
        let operations = r#"
        [
        {"key":0, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[109]}},
        {"key":143, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[243]}},
        {"key":90, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[217]}},
        {"key":172, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[94]}},
        {"key":27, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[4]}},
        {"key":124, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[0]}},
        {"key":123, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[0]}},
        {"key":67, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[109]}},
        {"key":13, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[0]}},
        {"key":162, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[213]}},
        {"key":235, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[67]}},
        {"key":251, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[50]}},
        {"key":218, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[164]}},
        {"key":166, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[67]}},
        {"key":64, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[61]}},
        {"key":183, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[210]}},
        {"key":250, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Delete":[50]}}
        ]
        "#;
        let operations: Vec<Operation<u16>> = serde_json::from_str(operations).unwrap();
        let tempdir = TempDir::new().unwrap();
        compare_with_trivial_database(Rc::new(tempdir), &operations);
    }

    #[test]
    #[no_coverage]
    fn reproduce_bug2() {
        let operations = r#"
        [
        {"key":102, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[122]}},
        {"key":73, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[132]}},
        {"key":20, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[215]}},
        {"key":39, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[152]}},
        {"key":151, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[226]}},
        {"key":17, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[101]}},
        {"key":74, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[210]}},
        {"key":2, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[130]}},
        {"key":64, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[180]}},
        {"key":83, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[250]}},
        {"key":80, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[210]}},
        {"key":113, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[63]}},
        {"key":201, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[210]}},
        {"key":200, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[5]}},
        {"key":93, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[98]}},
        {"key":162, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Insert":[5]}},
        {"key":80, "field_id": 0, "group_size":4, "max_group_size":8, "min_level_size":5, "kind":{"Delete":[210]}}
        ]
        "#;
        let operations: Vec<Operation<u16>> = serde_json::from_str(operations).unwrap();
        let tempdir = TempDir::new().unwrap();
        compare_with_trivial_database(Rc::new(tempdir), &operations);
    }
    #[test]
    #[no_coverage]
    fn reproduce_bug3() {
        let operations = r#"
        [
        {"key":27488, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[206]}},
        {"key":64716, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[216]}},
        {"key":60886, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[206]}},
        {"key":59509, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[187,231]}},
        {"key":55057, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[37]}},
        {"key":45200, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[206]}},
        {"key":55056, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[37]}},
        {"key":63679, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[206]}},
        {"key":52155, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[74]}},
        {"key":20648, "field_id": 0, "group_size":0, "max_group_size":7, "min_level_size":0, "kind":{"Insert":[47,138,157]}}
        ]
        "#;
        let operations: Vec<Operation<u16>> = serde_json::from_str(operations).unwrap();
        let tempdir = TempDir::new().unwrap();
        compare_with_trivial_database(Rc::new(tempdir), &operations);
    }

    #[test]
    #[no_coverage]
    fn reproduce_bug4() {
        let operations = r#"[
        {"key":63499, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[87]}},
        {"key":25374, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[14]}},
        {"key":64481, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Delete":[87]}},
        {"key":23038, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[173]}},
        {"key":14862, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[8]}},
        {"key":13145, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[5,64]}},
        {"key":23446, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[86,59]}},
        {"key":17972, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[58,137]}},
        {"key":21273, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[121,132,81,147]}},
        {"key":28264, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[36]}},
        {"key":46659, "field_id": 0, "group_size":2, "max_group_size":1, "min_level_size":0, "kind":{"Insert":[]}}
        ]
        "#;
        let operations: Vec<Operation<u16>> = serde_json::from_str(operations).unwrap();
        let tempdir = TempDir::new().unwrap();
        compare_with_trivial_database(Rc::new(tempdir), &operations);
    }

    #[test]
    #[no_coverage]
    fn reproduce_bug5() {
        let input = r#"
        [
            {
                "key":3438,
                "group_size":11,
                "max_group_size":0,
                "min_level_size":17,
                "field_id":3,
                "kind":{"Insert":[198]}
            },

            {
                "key":47098,
                "group_size":0,
                "max_group_size":8,
                "min_level_size":0,
                "field_id":3,
                "kind":{"Insert":[11]}
            },
            {
                "key":22453,
                "group_size":0,
                "max_group_size":0,
                "min_level_size":0,
                "field_id":3,
                "kind":{"Insert":[145]}
            },
            {
                "key":14105,
                "group_size":14,
                "max_group_size":4,
                "min_level_size":25,
                "field_id":3,
                "kind":{"Delete":[11]}
            }
        ]
        "#;
        let operations: Vec<Operation<u16>> = serde_json::from_str(input).unwrap();
        let tmpdir = TempDir::new().unwrap();
        compare_with_trivial_database(Rc::new(tmpdir), &operations);
    }

    #[test]
    #[no_coverage]
    fn reproduce_bug6() {
        let input = r#"
        [
        {"key":45720,"group_size":1,"max_group_size":4,"min_level_size":0,"field_id":0,"kind":{"Insert":[120]}},
        {"key":37463,"group_size":1,"max_group_size":4,"min_level_size":0,"field_id":0,"kind":{"Insert":[187]}},
        {"key":21512,"group_size":23,"max_group_size":20,"min_level_size":23,"field_id":0,"kind":{"Insert":[181]}},
        {"key":21511,"group_size":23,"max_group_size":20,"min_level_size":23,"field_id":0,"kind":{"Insert":[181]}},
        {"key":37737,"group_size":12,"max_group_size":0,"min_level_size":6,"field_id":0,"kind":{"Insert":[181]}},
        {"key":53042,"group_size":23,"max_group_size":20,"min_level_size":23,"field_id":0,"kind":{"Insert":[181]}}
        ]
        "#;
        let operations: Vec<Operation<u16>> = serde_json::from_str(input).unwrap();
        let tmpdir = TempDir::new().unwrap();
        compare_with_trivial_database(Rc::new(tmpdir), &operations);
    }
}

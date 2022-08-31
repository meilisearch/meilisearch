use crate::heed_codec::facet::new::{
    FacetGroupValue, FacetGroupValueCodec, FacetKey, FacetKeyCodec, MyByteSlice,
};
use crate::Result;
use heed::Error;
use heed::{types::ByteSlice, BytesDecode, RoTxn, RwTxn};
use roaring::RoaringBitmap;

use super::get_highest_level;

enum InsertionResult {
    InPlace,
    Insert,
}
enum DeletionResult {
    InPlace,
    Reduce { prev: Option<Vec<u8>>, next: Option<Vec<u8>> },
    Remove { prev: Option<Vec<u8>>, next: Option<Vec<u8>> },
}

struct IncrementalFacetUpdate<'i> {
    db: &'i heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    group_size: usize,
    min_level_size: usize,
    max_group_size: usize,
}
impl<'i> IncrementalFacetUpdate<'i> {
    fn find_insertion_key_value<'a>(
        &self,
        field_id: u16,
        level: u8,
        search_key: &[u8],
        txn: &RoTxn,
    ) -> Result<(FacetKey<Vec<u8>>, FacetGroupValue)> {
        let mut prefix = vec![];
        prefix.extend_from_slice(&field_id.to_be_bytes());
        prefix.push(level);
        prefix.extend_from_slice(search_key);

        let mut prefix_iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, MyByteSlice, FacetGroupValueCodec>(txn, &prefix.as_slice())?;
        if let Some(e) = prefix_iter.next() {
            let (key_bytes, value) = e?;
            Ok((
                FacetKeyCodec::<MyByteSlice>::bytes_decode(&key_bytes)
                    .ok_or(Error::Encoding)?
                    .into_owned(),
                value,
            ))
        } else {
            let key = FacetKey { field_id, level, left_bound: search_key };
            match self.db.get_lower_than(txn, &key)? {
                Some((key, value)) => {
                    if key.level != level || key.field_id != field_id {
                        let mut prefix = vec![];
                        prefix.extend_from_slice(&field_id.to_be_bytes());
                        prefix.push(level);

                        let mut iter = self
                            .db
                            .as_polymorph()
                            .prefix_iter::<_, MyByteSlice, FacetGroupValueCodec>(
                                txn,
                                &prefix.as_slice(),
                            )?;
                        let (key_bytes, value) = iter.next().unwrap()?;
                        Ok((
                            FacetKeyCodec::<MyByteSlice>::bytes_decode(&key_bytes)
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

    fn insert_in_level_0<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        new_key: &[u8],
        new_values: &RoaringBitmap,
    ) -> Result<InsertionResult> {
        let key = FacetKey { field_id, level: 0, left_bound: new_key };
        let value = FacetGroupValue { bitmap: new_values.clone(), size: 1 };

        let mut level0_prefix = vec![];
        level0_prefix.extend_from_slice(&field_id.to_be_bytes());
        level0_prefix.push(0);

        let mut iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(&txn, &level0_prefix)?;

        if iter.next().is_none() {
            drop(iter);
            self.db.put(txn, &key, &value)?;
            return Ok(InsertionResult::Insert);
        } else {
            drop(iter);
            let old_value = self.db.get(&txn, &key)?;
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
    fn insert_in_level<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        level: u8,
        new_key: &[u8],
        new_values: &RoaringBitmap,
    ) -> Result<InsertionResult> {
        if level == 0 {
            return self.insert_in_level_0(txn, field_id, new_key, new_values);
        }

        let max_group_size = self.max_group_size;

        let (insertion_key, insertion_value) =
            self.find_insertion_key_value(field_id, level, new_key, txn)?;

        let result = self.insert_in_level(txn, field_id, level - 1, new_key.clone(), new_values)?;
        // level below inserted an element

        let insertion_key = {
            let mut new_insertion_key = insertion_key.clone();
            let mut modified = false;

            if new_key < insertion_key.left_bound.as_slice() {
                new_insertion_key.left_bound = new_key.to_vec();
                modified = true;
            }
            if modified {
                let is_deleted = self.db.delete(txn, &insertion_key.as_ref())?;
                assert!(is_deleted);
                self.db.put(txn, &new_insertion_key.as_ref(), &insertion_value)?;
            }
            new_insertion_key
        };

        match result {
            // TODO: this could go above the block recomputing insertion key
            // because we know that if we inserted in place, the key is not a new one
            // thus it doesn't extend a group
            InsertionResult::InPlace => {
                let mut updated_value = self.db.get(&txn, &insertion_key.as_ref())?.unwrap();
                updated_value.bitmap |= new_values;
                self.db.put(txn, &insertion_key.as_ref(), &updated_value)?;

                return Ok(InsertionResult::InPlace);
            }
            InsertionResult::Insert => {}
        }
        let mut updated_value = self.db.get(&txn, &insertion_key.as_ref())?.unwrap();

        updated_value.size += 1;
        if updated_value.size as usize == max_group_size {
            // need to split it
            // recompute left element and right element
            // replace current group by left element
            // add one more group to the right

            let size_left = max_group_size / 2;
            let size_right = max_group_size - size_left;

            let level_below = level - 1;

            let (start_key, _) = self
                .db
                .get_greater_than_or_equal_to(
                    &txn,
                    &FacetKey {
                        field_id,
                        level: level_below,
                        left_bound: insertion_key.left_bound.as_slice(),
                    },
                )?
                .unwrap();

            let mut iter = self.db.range(&txn, &(start_key..))?.take(max_group_size);

            let group_left = {
                let mut values_left = RoaringBitmap::new();

                let mut i = 0;
                while let Some(next) = iter.next() {
                    let (_key, value) = next?;
                    i += 1;
                    values_left |= &value.bitmap;
                    if i == size_left {
                        break;
                    }
                }

                let key =
                    FacetKey { field_id, level, left_bound: insertion_key.left_bound.clone() };
                let value = FacetGroupValue { size: size_left as u8, bitmap: values_left };
                (key, value)
            };

            let group_right = {
                let mut values_right = RoaringBitmap::new();
                let mut right_start_key = None;

                while let Some(next) = iter.next() {
                    let (key, value) = next?;
                    if right_start_key.is_none() {
                        right_start_key = Some(key.left_bound);
                    }
                    values_right |= &value.bitmap;
                }

                let key =
                    FacetKey { field_id, level, left_bound: right_start_key.unwrap().to_vec() };
                let value = FacetGroupValue { size: size_right as u8, bitmap: values_right };
                (key, value)
            };
            drop(iter);

            let _ = self.db.delete(txn, &insertion_key.as_ref())?;

            self.db.put(txn, &group_left.0.as_ref(), &group_left.1)?;
            self.db.put(txn, &group_right.0.as_ref(), &group_right.1)?;

            Ok(InsertionResult::Insert)
        } else {
            let mut value = self.db.get(&txn, &insertion_key.as_ref())?.unwrap();
            value.bitmap |= new_values;
            value.size += 1;
            self.db.put(txn, &insertion_key.as_ref(), &value).unwrap();

            Ok(InsertionResult::InPlace)
        }
    }

    pub fn insert<'a, 't>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        new_key: &[u8],
        new_values: &RoaringBitmap,
    ) -> Result<()> {
        if new_values.is_empty() {
            return Ok(());
        }
        let group_size = self.group_size;

        let highest_level = get_highest_level(&txn, &self.db, field_id)?;

        let result =
            self.insert_in_level(txn, field_id, highest_level as u8, new_key, new_values)?;
        match result {
            InsertionResult::InPlace => return Ok(()),
            InsertionResult::Insert => {}
        }

        let mut highest_level_prefix = vec![];
        highest_level_prefix.extend_from_slice(&field_id.to_be_bytes());
        highest_level_prefix.push(highest_level);

        let size_highest_level = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, ByteSlice>(&txn, &highest_level_prefix)?
            .count();

        if size_highest_level < self.min_level_size {
            return Ok(());
        }

        let mut groups_iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(&txn, &highest_level_prefix)?;

        let mut to_add = vec![];
        for _ in 0..group_size {
            let mut first_key = None;
            let mut values = RoaringBitmap::new();
            for _ in 0..group_size {
                let (key_bytes, value_i) = groups_iter.next().unwrap()?;
                let key_i = FacetKeyCodec::<MyByteSlice>::bytes_decode(&key_bytes)
                    .ok_or(Error::Encoding)?;

                if first_key.is_none() {
                    first_key = Some(key_i);
                }
                values |= value_i.bitmap;
            }
            let key = FacetKey {
                field_id,
                level: highest_level + 1,
                left_bound: first_key.unwrap().left_bound,
            };
            let value = FacetGroupValue { size: group_size as u8, bitmap: values };
            to_add.push((key.into_owned(), value));
        }
        drop(groups_iter);
        for (key, value) in to_add {
            self.db.put(txn, &key.as_ref(), &value)?;
        }
        Ok(())
    }

    fn delete_in_level<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        level: u8,
        key: &[u8],
        value: u32,
    ) -> Result<DeletionResult> {
        if level == 0 {
            return self.delete_in_level_0(txn, field_id, key, value);
        }
        let (deletion_key, mut bitmap) =
            self.find_insertion_key_value(field_id, level, key, txn)?;

        let result = self.delete_in_level(txn, field_id, level - 1, key.clone(), value)?;

        let mut decrease_size = false;
        let (prev_key, next_key) = match result {
            DeletionResult::InPlace => {
                bitmap.bitmap.remove(value);
                self.db.put(txn, &deletion_key.as_ref(), &bitmap)?;
                return Ok(DeletionResult::InPlace);
            }
            DeletionResult::Reduce { prev, next } => (prev, next),
            DeletionResult::Remove { prev, next } => {
                decrease_size = true;
                (prev, next)
            }
        };

        let mut updated_value = bitmap;
        if decrease_size {
            updated_value.size -= 1;
        }

        if updated_value.size == 0 {
            self.db.delete(txn, &deletion_key.as_ref())?;
            Ok(DeletionResult::Remove { prev: prev_key, next: next_key })
        } else {
            let mut updated_deletion_key = deletion_key.clone();
            if key == deletion_key.left_bound {
                updated_deletion_key.left_bound = next_key.clone().unwrap();
            }
            updated_value.bitmap.remove(value);
            let _ = self.db.delete(txn, &deletion_key.as_ref())?;
            self.db.put(txn, &updated_deletion_key.as_ref(), &updated_value)?;

            Ok(DeletionResult::Reduce { prev: prev_key, next: next_key })
        }
    }

    fn delete_in_level_0<'t>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        key: &[u8],
        value: u32,
    ) -> Result<DeletionResult> {
        let key = FacetKey { field_id, level: 0, left_bound: key };
        let mut bitmap = self.db.get(&txn, &key)?.unwrap().bitmap;
        bitmap.remove(value);

        if bitmap.is_empty() {
            let mut prev_key = None;
            let mut next_key = None;

            if let Some(prev) = self.db.get_lower_than(&txn, &key)? {
                prev_key = Some(prev.0.left_bound.to_vec());
            }
            if let Some(next) = self.db.get_greater_than(&txn, &key)? {
                if next.0.level == 0 {
                    next_key = Some(next.0.left_bound.to_vec());
                }
            }
            self.db.delete(txn, &key)?;
            Ok(DeletionResult::Remove { prev: prev_key, next: next_key })
        } else {
            self.db.put(txn, &key, &FacetGroupValue { size: 1, bitmap })?;
            Ok(DeletionResult::InPlace)
        }
    }

    pub fn delete<'a, 't>(
        &self,
        txn: &'t mut RwTxn,
        field_id: u16,
        key: &[u8],
        value: u32,
    ) -> Result<()> {
        if self.db.get(txn, &FacetKey { field_id, level: 0, left_bound: key })?.is_none() {
            return Ok(());
        }
        let highest_level = get_highest_level(&txn, &self.db, field_id)?;

        // let key_bytes = BoundCodec::bytes_encode(&key).unwrap();

        let result = self.delete_in_level(txn, field_id, highest_level as u8, key, value)?;
        match result {
            DeletionResult::InPlace => return Ok(()),
            DeletionResult::Reduce { .. } => {}
            DeletionResult::Remove { .. } => {}
        }
        let mut highest_level_prefix = vec![];
        highest_level_prefix.extend_from_slice(&field_id.to_be_bytes());
        highest_level_prefix.push(highest_level);

        if highest_level == 0
            || self
                .db
                .as_polymorph()
                .prefix_iter::<_, ByteSlice, ByteSlice>(&txn, &highest_level_prefix)?
                .count()
                >= self.group_size
        {
            return Ok(());
        }
        let mut to_delete = vec![];
        let mut iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, ByteSlice>(txn, &highest_level_prefix)?;
        while let Some(el) = iter.next() {
            let (k, _) = el?;
            to_delete.push(
                FacetKeyCodec::<MyByteSlice>::bytes_decode(k).ok_or(Error::Encoding)?.into_owned(),
            );
        }
        drop(iter);
        for k in to_delete {
            self.db.delete(txn, &k.as_ref())?;
        }
        Ok(())
    }
}

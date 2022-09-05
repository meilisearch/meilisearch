use heed::types::ByteSlice;
use heed::{BytesDecode, Error, RoTxn, RwTxn};
use roaring::RoaringBitmap;

use crate::heed_codec::facet::{
    ByteSliceRef, FacetGroupKey, FacetGroupKeyCodec, FacetGroupValue, FacetGroupValueCodec,
};
use crate::search::facet::get_highest_level;
use crate::Result;

enum InsertionResult {
    InPlace,
    Insert,
}
enum DeletionResult {
    InPlace,
    Reduce { prev: Option<Vec<u8>>, next: Option<Vec<u8>> },
    Remove { prev: Option<Vec<u8>>, next: Option<Vec<u8>> },
}

pub struct FacetsUpdateIncremental {
    db: heed::Database<FacetGroupKeyCodec<ByteSliceRef>, FacetGroupValueCodec>,
    group_size: u8,
    min_level_size: u8,
    max_group_size: u8,
}
impl FacetsUpdateIncremental {
    pub fn new(db: heed::Database<FacetGroupKeyCodec<ByteSliceRef>, FacetGroupValueCodec>) -> Self {
        Self { db, group_size: 4, min_level_size: 5, max_group_size: 8 }
    }
    pub fn group_size(mut self, size: u8) -> Self {
        self.group_size = size;
        self
    }
    pub fn min_level_size(mut self, size: u8) -> Self {
        self.min_level_size = size;
        self
    }
    pub fn max_group_size(mut self, size: u8) -> Self {
        self.max_group_size = size;
        self
    }
}
impl FacetsUpdateIncremental {
    fn find_insertion_key_value(
        &self,
        field_id: u16,
        level: u8,
        search_key: &[u8],
        txn: &RoTxn,
    ) -> Result<(FacetGroupKey<Vec<u8>>, FacetGroupValue)> {
        let mut prefix = vec![];
        prefix.extend_from_slice(&field_id.to_be_bytes());
        prefix.push(level);
        prefix.extend_from_slice(search_key);

        let mut prefix_iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSliceRef, FacetGroupValueCodec>(txn, &prefix.as_slice())?;
        if let Some(e) = prefix_iter.next() {
            let (key_bytes, value) = e?;
            Ok((
                FacetGroupKeyCodec::<ByteSliceRef>::bytes_decode(&key_bytes)
                    .ok_or(Error::Encoding)?
                    .into_owned(),
                value,
            ))
        } else {
            let key = FacetGroupKey { field_id, level, left_bound: search_key };
            match self.db.get_lower_than(txn, &key)? {
                Some((key, value)) => {
                    if key.level != level || key.field_id != field_id {
                        let mut prefix = vec![];
                        prefix.extend_from_slice(&field_id.to_be_bytes());
                        prefix.push(level);

                        let mut iter = self
                            .db
                            .as_polymorph()
                            .prefix_iter::<_, ByteSliceRef, FacetGroupValueCodec>(
                                txn,
                                &prefix.as_slice(),
                            )?;
                        let (key_bytes, value) = iter.next().unwrap()?;
                        Ok((
                            FacetGroupKeyCodec::<ByteSliceRef>::bytes_decode(&key_bytes)
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
        let key = FacetGroupKey { field_id, level: 0, left_bound: new_key };
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
        if updated_value.size == max_group_size {
            let size_left = max_group_size / 2;
            let size_right = max_group_size - size_left;

            let level_below = level - 1;

            let (start_key, _) = self
                .db
                .get_greater_than_or_equal_to(
                    &txn,
                    &FacetGroupKey {
                        field_id,
                        level: level_below,
                        left_bound: insertion_key.left_bound.as_slice(),
                    },
                )?
                .unwrap();

            let mut iter = self.db.range(&txn, &(start_key..))?.take(max_group_size as usize);

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
                    FacetGroupKey { field_id, level, left_bound: insertion_key.left_bound.clone() };
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

                let key = FacetGroupKey {
                    field_id,
                    level,
                    left_bound: right_start_key.unwrap().to_vec(),
                };
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

        let highest_level = get_highest_level(&txn, self.db, field_id)?;

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

        if size_highest_level < self.group_size as usize * self.min_level_size as usize {
            return Ok(());
        }

        let mut groups_iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(&txn, &highest_level_prefix)?;

        let mut to_add = vec![];
        for _ in 0..self.min_level_size {
            let mut first_key = None;
            let mut values = RoaringBitmap::new();
            for _ in 0..group_size {
                let (key_bytes, value_i) = groups_iter.next().unwrap()?;
                let key_i = FacetGroupKeyCodec::<ByteSliceRef>::bytes_decode(&key_bytes)
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
        let key = FacetGroupKey { field_id, level: 0, left_bound: key };
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
        if self.db.get(txn, &FacetGroupKey { field_id, level: 0, left_bound: key })?.is_none() {
            return Ok(());
        }
        let highest_level = get_highest_level(&txn, self.db, field_id)?;

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
                >= self.min_level_size as usize
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
                FacetGroupKeyCodec::<ByteSliceRef>::bytes_decode(k)
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

#[cfg(test)]
mod tests {
    use heed::types::ByteSlice;
    use heed::{BytesDecode, BytesEncode};
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use roaring::RoaringBitmap;

    use crate::heed_codec::facet::OrderedF64Codec;
    use crate::heed_codec::facet::StrRefCodec;
    use crate::heed_codec::facet::{ByteSliceRef, FacetGroupKeyCodec, FacetGroupValueCodec};
    use crate::milli_snap;
    use crate::search::facet::get_highest_level;
    use crate::search::facet::test::FacetIndex;

    pub fn verify_structure_validity<C>(index: &FacetIndex<C>, field_id: u16)
    where
        for<'a> C: BytesDecode<'a> + BytesEncode<'a, EItem = <C as BytesDecode<'a>>::DItem>,
    {
        let FacetIndex { env, db, .. } = index;

        let txn = env.write_txn().unwrap();
        let mut field_id_prefix = vec![];
        field_id_prefix.extend_from_slice(&field_id.to_be_bytes());

        let highest_level = get_highest_level(&txn, index.db.content, field_id).unwrap();
        txn.commit().unwrap();

        let txn = env.read_txn().unwrap();
        for level_no in (1..=highest_level).rev() {
            let mut level_no_prefix = vec![];
            level_no_prefix.extend_from_slice(&field_id.to_be_bytes());
            level_no_prefix.push(level_no);

            let mut iter = db
                .content
                .as_polymorph()
                .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(&txn, &level_no_prefix)
                .unwrap();
            while let Some(el) = iter.next() {
                let (key, value) = el.unwrap();
                let key = FacetGroupKeyCodec::<ByteSliceRef>::bytes_decode(&key).unwrap();

                let mut prefix_start_below = vec![];
                prefix_start_below.extend_from_slice(&field_id.to_be_bytes());
                prefix_start_below.push(level_no - 1);
                prefix_start_below.extend_from_slice(&key.left_bound);

                let start_below = {
                    let mut start_below_iter = db
                        .content
                        .as_polymorph()
                        .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(
                            &txn,
                            &prefix_start_below,
                        )
                        .unwrap();
                    let (key_bytes, _) = start_below_iter.next().unwrap().unwrap();
                    FacetGroupKeyCodec::<ByteSliceRef>::bytes_decode(&key_bytes).unwrap()
                };

                assert!(value.size > 0 && (value.size as usize) < db.max_group_size);

                let mut actual_size = 0;
                let mut values_below = RoaringBitmap::new();
                let mut iter_below =
                    db.content.range(&txn, &(start_below..)).unwrap().take(value.size as usize);
                while let Some(el) = iter_below.next() {
                    let (_, value) = el.unwrap();
                    actual_size += 1;
                    values_below |= value.bitmap;
                }
                assert_eq!(actual_size, value.size, "{key:?} start_below: {start_below:?}");

                assert_eq!(value.bitmap, values_below);
            }
        }
    }
    #[test]
    fn append() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
        for i in 0..256u16 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i as u32);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"));
    }
    #[test]
    fn many_field_ids_append() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
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
        verify_structure_validity(&index, 0);
        verify_structure_validity(&index, 1);
        verify_structure_validity(&index, 2);
        milli_snap!(format!("{index}"));
    }
    #[test]
    fn many_field_ids_prepend() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
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
        verify_structure_validity(&index, 0);
        verify_structure_validity(&index, 1);
        verify_structure_validity(&index, 2);
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn prepend() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
        let mut txn = index.env.write_txn().unwrap();

        for i in (0..256).into_iter().rev() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
        }
        txn.commit().unwrap();
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn shuffled() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
        let mut txn = index.env.write_txn().unwrap();

        let mut keys = (0..256).into_iter().collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for (_i, key) in keys.into_iter().enumerate() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            index.insert(&mut txn, 0, &(key as f64), &bitmap);
        }
        txn.commit().unwrap();
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn merge_values() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);

        let mut keys = (0..256).into_iter().collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);
        for (_i, key) in keys.into_iter().enumerate() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(rng.gen_range(256..512));
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(key as f64), &bitmap);
            txn.commit().unwrap();
        }

        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn delete_from_end() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
        for i in 0..256 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(&(i as f64)), &bitmap);
            txn.commit().unwrap();
        }

        for i in (200..256).into_iter().rev() {
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(i as f64), i as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 200);

        for i in (150..200).into_iter().rev() {
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(i as f64), i as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 150);

        for i in (100..150).into_iter().rev() {
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(i as f64), i as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 100);

        for i in (17..100).into_iter().rev() {
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(i as f64), i as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 17);

        let mut txn = index.env.write_txn().unwrap();
        for i in (15..17).into_iter().rev() {
            index.delete(&mut txn, 0, &(i as f64), i as u32);
        }
        txn.commit().unwrap();
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 15);
        for i in (0..15).into_iter().rev() {
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(i as f64), i as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 0);
    }

    #[test]
    fn delete_from_start() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);

        for i in 0..256 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }

        for i in 0..128 {
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(i as f64), i as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 127);
        for i in 128..216 {
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(i as f64), i as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 215);
        for i in 216..256 {
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(i as f64), i as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 255);
    }

    #[test]
    fn delete_shuffled() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);

        for i in 0..256 {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(i as f64), &bitmap);
            txn.commit().unwrap();
        }

        let mut keys = (0..256).into_iter().collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for i in 0..128 {
            let key = keys[i];
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(key as f64), key as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 127);
        for i in 128..216 {
            let key = keys[i];
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(key as f64), key as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 215);
        for i in 216..256 {
            let key = keys[i];
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(key as f64), key as u32);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), 255);
    }

    #[test]
    fn in_place_level0_insert() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);
        let mut keys = (0..16).into_iter().collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);
        for i in 0..4 {
            for &key in keys.iter() {
                let mut bitmap = RoaringBitmap::new();
                bitmap.insert(rng.gen_range(i * 256..(i + 1) * 256));
                verify_structure_validity(&index, 0);
                let mut txn = index.env.write_txn().unwrap();
                index.insert(&mut txn, 0, &(key as f64), &bitmap);
                txn.commit().unwrap();
            }
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"));
    }

    #[test]
    fn in_place_level0_delete() {
        let index = FacetIndex::<OrderedF64Codec>::new(4, 8);

        let mut keys = (0..64).into_iter().collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for &key in keys.iter() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(key + 100);
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &(key as f64), &bitmap);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), "before_delete");

        for &key in keys.iter() {
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &(key as f64), key + 100);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), "after_delete");
    }

    #[test]
    fn shuffle_merge_string_and_delete() {
        let index = FacetIndex::<StrRefCodec>::new(4, 8);

        let mut keys = (1000..1064).into_iter().collect::<Vec<_>>();
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        keys.shuffle(&mut rng);

        for &key in keys.iter() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(key);
            bitmap.insert(key + 100);
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.insert(&mut txn, 0, &format!("{key:x}").as_str(), &bitmap);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), "before_delete");

        for &key in keys.iter() {
            verify_structure_validity(&index, 0);
            let mut txn = index.env.write_txn().unwrap();
            index.delete(&mut txn, 0, &format!("{key:x}").as_str(), key + 100);
            txn.commit().unwrap();
        }
        verify_structure_validity(&index, 0);
        milli_snap!(format!("{index}"), "after_delete");
    }

    // fuzz tests
}
// #[cfg(all(test, fuzzing))]
// mod fuzz {
//     use crate::codec::U16Codec;

//     use super::tests::verify_structure_validity;
//     use super::*;
//     use fuzzcheck::mutators::integer_within_range::U16WithinRangeMutator;
//     use fuzzcheck::DefaultMutator;
//     use roaring::RoaringBitmap;
//     use std::collections::BTreeMap;
//     use std::collections::HashMap;

//     #[derive(Default)]
//     pub struct TrivialDatabase<T> {
//         pub elements: BTreeMap<u16, BTreeMap<T, RoaringBitmap>>,
//     }
//     impl<T> TrivialDatabase<T>
//     where
//         T: Ord + Clone + Copy + Eq + std::fmt::Debug,
//     {
//         pub fn insert(&mut self, field_id: u16, new_key: T, new_values: &RoaringBitmap) {
//             if new_values.is_empty() {
//                 return;
//             }
//             let values_field_id = self.elements.entry(field_id).or_default();
//             let values = values_field_id.entry(new_key).or_default();
//             *values |= new_values;
//         }
//         pub fn delete(&mut self, field_id: u16, key: T, value: u32) {
//             if let Some(values_field_id) = self.elements.get_mut(&field_id) {
//                 if let Some(values) = values_field_id.get_mut(&key) {
//                     values.remove(value);
//                     if values.is_empty() {
//                         values_field_id.remove(&key);
//                     }
//                 }
//                 if values_field_id.is_empty() {
//                     self.elements.remove(&field_id);
//                 }
//             }
//         }
//     }
//     #[derive(Clone, DefaultMutator, serde::Serialize, serde::Deserialize)]
//     struct Operation<Key> {
//         key: Key,
//         #[field_mutator(U16WithinRangeMutator = { U16WithinRangeMutator::new(..=3) })]
//         field_id: u16,
//         kind: OperationKind,
//     }
//     #[derive(Clone, DefaultMutator, serde::Serialize, serde::Deserialize)]
//     enum OperationKind {
//         Insert(Vec<u8>),
//         Delete(u8),
//     }

//     fn compare_with_trivial_database(
//         tempdir: Rc<TempDir>,
//         group_size: u8,
//         max_group_size: u8,
//         operations: &[Operation<u16>],
//     ) {
//         let index = FacetIndex::<OrderedF64Codec>::open_from_tempdir(tempdir, group_size, max_group_size);
//         let mut trivial_db = TrivialDatabase::<u16>::default();
//         let mut value_to_keys = HashMap::<u8, Vec<u16>>::new();
//         let mut txn = index.env.write_txn().unwrap();
//         for Operation { key, field_id, kind } in operations {
//             match kind {
//                 OperationKind::Insert(values) => {
//                     let mut bitmap = RoaringBitmap::new();
//                     for value in values {
//                         bitmap.insert(*value as u32);
//                         value_to_keys.entry(*value).or_default().push(*key);
//                     }
//                     index.insert(&mut txn, *field_id, key, &bitmap);
//                     trivial_db.insert(*field_id, *key, &bitmap);
//                 }
//                 OperationKind::Delete(value) => {
//                     if let Some(keys) = value_to_keys.get(value) {
//                         for key in keys {
//                             index.delete(&mut txn, *field_id, key, *value as u32);
//                             trivial_db.delete(*field_id, *key, *value as u32);
//                         }
//                     }
//                 }
//             }
//         }
//         for (field_id, values_field_id) in trivial_db.elements.iter() {
//             let level0iter = index
//                 .db
//                 .content
//                 .as_polymorph()
//                 .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(
//                     &mut txn,
//                     &field_id.to_be_bytes(),
//                 )
//                 .unwrap();

//             for ((key, values), group) in values_field_id.iter().zip(level0iter) {
//                 let (group_key, group_values) = group.unwrap();
//                 let group_key = FacetGroupKeyCodec::<U16Codec>::bytes_decode(group_key).unwrap();
//                 assert_eq!(key, &group_key.left_bound);
//                 assert_eq!(values, &group_values.bitmap);
//             }
//         }

//         txn.commit().unwrap();
//         let mut txn = index.env.write_txn().unwrap();
//         for (field_id, values_field_id) in trivial_db.elements.iter() {
//             let level0iter = index
//                 .db
//                 .content
//                 .as_polymorph()
//                 .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(&txn, &field_id.to_be_bytes())
//                 .unwrap();

//             for ((key, values), group) in values_field_id.iter().zip(level0iter) {
//                 let (group_key, group_values) = group.unwrap();
//                 let group_key = FacetGroupKeyCodec::<U16Codec>::bytes_decode(group_key).unwrap();
//                 assert_eq!(key, &group_key.left_bound);
//                 assert_eq!(values, &group_values.bitmap);
//             }
//             verify_structure_validity(&index, *field_id);
//         }

//         index.db.content.clear(&mut txn).unwrap();
//         txn.commit().unwrap();
//     }

//     #[test]
//     fn fuzz() {
//         let tempdir = Rc::new(TempDir::new().unwrap());
//         let tempdir_cloned = tempdir.clone();
//         let result = fuzzcheck::fuzz_test(move |x: &(u8, u8, Vec<Operation<u16>>)| {
//             compare_with_trivial_database(tempdir_cloned.clone(), x.0, x.1, &x.2)
//         })
//         .default_mutator()
//         .serde_serializer()
//         .default_sensor_and_pool_with_custom_filter(|file, function| {
//             if file.is_relative()
//                 && !function.contains("serde")
//                 && !function.contains("tests::")
//                 && !function.contains("fuzz::")
//                 && !function.contains("display_bitmap")
//             {
//                 true
//             } else {
//                 false
//             }
//         })
//         .arguments_from_cargo_fuzzcheck()
//         .launch();
//         assert!(!result.found_test_failure);
//     }

//     #[test]
//     fn reproduce_bug() {
//         let operations = r#"
//         [
//         {"key":0, "field_id": 0, "kind":{"Insert":[109]}},
//         {"key":143, "field_id": 0, "kind":{"Insert":[243]}},
//         {"key":90, "field_id": 0, "kind":{"Insert":[217]}},
//         {"key":172, "field_id": 0, "kind":{"Insert":[94]}},
//         {"key":27, "field_id": 0, "kind":{"Insert":[4]}},
//         {"key":124, "field_id": 0, "kind":{"Insert":[0]}},
//         {"key":123, "field_id": 0, "kind":{"Insert":[0]}},
//         {"key":67, "field_id": 0, "kind":{"Insert":[109]}},
//         {"key":13, "field_id": 0, "kind":{"Insert":[0]}},
//         {"key":162, "field_id": 0, "kind":{"Insert":[213]}},
//         {"key":235, "field_id": 0, "kind":{"Insert":[67]}},
//         {"key":251, "field_id": 0, "kind":{"Insert":[50]}},
//         {"key":218, "field_id": 0, "kind":{"Insert":[164]}},
//         {"key":166, "field_id": 0, "kind":{"Insert":[67]}},
//         {"key":64, "field_id": 0, "kind":{"Insert":[61]}},
//         {"key":183, "field_id": 0, "kind":{"Insert":[210]}},
//         {"key":250, "field_id": 0, "kind":{"Delete":50}}
//         ]
//         "#;
//         let operations: Vec<Operation<u16>> = serde_json::from_str(operations).unwrap();
//         let tempdir = TempDir::new().unwrap();
//         compare_with_trivial_database(Rc::new(tempdir), 4, 8, &operations);
//     }

//     #[test]
//     fn reproduce_bug2() {
//         let operations = r#"
//         [
//         {"key":102, "field_id": 0, "kind":{"Insert":[122]}},
//         {"key":73, "field_id": 0, "kind":{"Insert":[132]}},
//         {"key":20, "field_id": 0, "kind":{"Insert":[215]}},
//         {"key":39, "field_id": 0, "kind":{"Insert":[152]}},
//         {"key":151, "field_id": 0, "kind":{"Insert":[226]}},
//         {"key":17, "field_id": 0, "kind":{"Insert":[101]}},
//         {"key":74, "field_id": 0, "kind":{"Insert":[210]}},
//         {"key":2, "field_id": 0, "kind":{"Insert":[130]}},
//         {"key":64, "field_id": 0, "kind":{"Insert":[180]}},
//         {"key":83, "field_id": 0, "kind":{"Insert":[250]}},
//         {"key":80, "field_id": 0, "kind":{"Insert":[210]}},
//         {"key":113, "field_id": 0, "kind":{"Insert":[63]}},
//         {"key":201, "field_id": 0, "kind":{"Insert":[210]}},
//         {"key":200, "field_id": 0, "kind":{"Insert":[5]}},
//         {"key":93, "field_id": 0, "kind":{"Insert":[98]}},
//         {"key":162, "field_id": 0, "kind":{"Insert":[5]}},
//         {"key":80, "field_id": 0, "kind":{"Delete":210}}
//         ]
//         "#;
//         let operations: Vec<Operation<u16>> = serde_json::from_str(operations).unwrap();
//         let tempdir = TempDir::new().unwrap();
//         compare_with_trivial_database(Rc::new(tempdir), 4, 8, &operations);
//     }
//     #[test]
//     fn reproduce_bug3() {
//         let operations = r#"
//         [
//         {"key":27488, "field_id": 0, "kind":{"Insert":[206]}},
//         {"key":64716, "field_id": 0, "kind":{"Insert":[216]}},
//         {"key":60886, "field_id": 0, "kind":{"Insert":[206]}},
//         {"key":59509, "field_id": 0, "kind":{"Insert":[187,231]}},
//         {"key":55057, "field_id": 0, "kind":{"Insert":[37]}},
//         {"key":45200, "field_id": 0, "kind":{"Insert":[206]}},
//         {"key":55056, "field_id": 0, "kind":{"Insert":[37]}},
//         {"key":63679, "field_id": 0, "kind":{"Insert":[206]}},
//         {"key":52155, "field_id": 0, "kind":{"Insert":[74]}},
//         {"key":20648, "field_id": 0, "kind":{"Insert":[47,138,157]}}
//         ]
//         "#;
//         let operations: Vec<Operation<u16>> = serde_json::from_str(operations).unwrap();
//         let tempdir = TempDir::new().unwrap();
//         compare_with_trivial_database(Rc::new(tempdir), 0, 7, &operations);
//     }

//     #[test]
//     fn reproduce_bug4() {
//         let operations = r#"
//         [{"key":63499, "field_id": 0, "kind":{"Insert":[87]}},{"key":25374, "field_id": 0, "kind":{"Insert":[14]}},{"key":64481, "field_id": 0, "kind":{"Delete":87}},{"key":23038, "field_id": 0, "kind":{"Insert":[173]}},{"key":14862, "field_id": 0, "kind":{"Insert":[8]}},{"key":13145, "field_id": 0, "kind":{"Insert":[5,64]}},{"key":23446, "field_id": 0, "kind":{"Insert":[86,59]}},{"key":17972, "field_id": 0, "kind":{"Insert":[58,137]}},{"key":21273, "field_id": 0, "kind":{"Insert":[121,132,81,147]}},{"key":28264, "field_id": 0, "kind":{"Insert":[36]}},{"key":46659, "field_id": 0, "kind":{"Insert":[]}}]
//         "#;
//         let operations: Vec<Operation<u16>> = serde_json::from_str(operations).unwrap();
//         let tempdir = TempDir::new().unwrap();
//         compare_with_trivial_database(Rc::new(tempdir), 2, 1, &operations);
//     }
// }

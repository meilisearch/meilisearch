use std::borrow::Cow;
use std::fs::File;

use grenad::CompressionType;
use heed::types::ByteSlice;
use heed::{BytesEncode, Error, RoTxn, RwTxn};
use roaring::RoaringBitmap;

use super::{FACET_GROUP_SIZE, FACET_MIN_LEVEL_SIZE};
use crate::facet::FacetType;
use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValue, FacetGroupValueCodec,
};
use crate::heed_codec::ByteSliceRefCodec;
use crate::update::index_documents::{create_writer, valid_lmdb_key, writer_into_reader};
use crate::{CboRoaringBitmapCodec, FieldId, Index, Result};

/// Algorithm to insert elememts into the `facet_id_(string/f64)_docids` databases
/// by rebuilding the database "from scratch".
///
/// First, the new elements are inserted into the level 0 of the database. Then, the
/// higher levels are cleared and recomputed from the content of level 0.
///
/// Finally, the `faceted_documents_ids` value in the main database of `Index`
/// is updated to contain the new set of faceted documents.
pub struct FacetsUpdateBulk<'i> {
    index: &'i Index,
    group_size: u8,
    min_level_size: u8,
    facet_type: FacetType,
    field_ids: Vec<FieldId>,
    // None if level 0 does not need to be updated
    new_data: Option<grenad::Reader<File>>,
}

impl<'i> FacetsUpdateBulk<'i> {
    pub fn new(
        index: &'i Index,
        field_ids: Vec<FieldId>,
        facet_type: FacetType,
        new_data: grenad::Reader<File>,
        group_size: u8,
        min_level_size: u8,
    ) -> FacetsUpdateBulk<'i> {
        FacetsUpdateBulk {
            index,
            field_ids,
            group_size,
            min_level_size,
            facet_type,
            new_data: Some(new_data),
        }
    }

    pub fn new_not_updating_level_0(
        index: &'i Index,
        field_ids: Vec<FieldId>,
        facet_type: FacetType,
    ) -> FacetsUpdateBulk<'i> {
        FacetsUpdateBulk {
            index,
            field_ids,
            group_size: FACET_GROUP_SIZE,
            min_level_size: FACET_MIN_LEVEL_SIZE,
            facet_type,
            new_data: None,
        }
    }

    #[logging_timer::time("FacetsUpdateBulk::{}")]
    pub fn execute(self, wtxn: &mut heed::RwTxn) -> Result<()> {
        let Self { index, field_ids, group_size, min_level_size, facet_type, new_data } = self;

        let db = match facet_type {
            FacetType::String => index
                .facet_id_string_docids
                .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
            FacetType::Number => {
                index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>()
            }
        };

        let inner = FacetsUpdateBulkInner { db, new_data, group_size, min_level_size };

        inner.update(wtxn, &field_ids, |wtxn, field_id, all_docids| {
            index.put_faceted_documents_ids(wtxn, field_id, facet_type, &all_docids)?;
            Ok(())
        })?;

        Ok(())
    }
}

/// Implementation of `FacetsUpdateBulk` that is independent of milli's `Index` type
pub(crate) struct FacetsUpdateBulkInner<R: std::io::Read + std::io::Seek> {
    pub db: heed::Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
    pub new_data: Option<grenad::Reader<R>>,
    pub group_size: u8,
    pub min_level_size: u8,
}
impl<R: std::io::Read + std::io::Seek> FacetsUpdateBulkInner<R> {
    pub fn update(
        mut self,
        wtxn: &mut RwTxn,
        field_ids: &[u16],
        mut handle_all_docids: impl FnMut(&mut RwTxn, FieldId, RoaringBitmap) -> Result<()>,
    ) -> Result<()> {
        self.update_level0(wtxn)?;
        for &field_id in field_ids.iter() {
            self.clear_levels(wtxn, field_id)?;
        }

        for &field_id in field_ids.iter() {
            let (level_readers, all_docids) = self.compute_levels_for_field_id(field_id, wtxn)?;

            handle_all_docids(wtxn, field_id, all_docids)?;

            for level_reader in level_readers {
                let mut cursor = level_reader.into_cursor()?;
                while let Some((k, v)) = cursor.move_on_next()? {
                    self.db.remap_types::<ByteSlice, ByteSlice>().put(wtxn, k, v)?;
                }
            }
        }
        Ok(())
    }

    fn clear_levels(&self, wtxn: &mut heed::RwTxn, field_id: FieldId) -> Result<()> {
        let left = FacetGroupKey::<&[u8]> { field_id, level: 1, left_bound: &[] };
        let right = FacetGroupKey::<&[u8]> { field_id, level: u8::MAX, left_bound: &[] };
        let range = left..=right;
        self.db.delete_range(wtxn, &range).map(drop)?;
        Ok(())
    }
    fn update_level0(&mut self, wtxn: &mut RwTxn) -> Result<()> {
        let new_data = match self.new_data.take() {
            Some(x) => x,
            None => return Ok(()),
        };
        if self.db.is_empty(wtxn)? {
            let mut buffer = Vec::new();
            let mut database = self.db.iter_mut(wtxn)?.remap_types::<ByteSlice, ByteSlice>();
            let mut cursor = new_data.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                if !valid_lmdb_key(key) {
                    continue;
                }
                buffer.clear();
                // the group size for level 0
                buffer.push(1);
                // then we extend the buffer with the docids bitmap
                buffer.extend_from_slice(value);
                unsafe { database.append(key, &buffer)? };
            }
        } else {
            let mut buffer = Vec::new();
            let database = self.db.remap_types::<ByteSlice, ByteSlice>();

            let mut cursor = new_data.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                if !valid_lmdb_key(key) {
                    continue;
                }
                // the value is a CboRoaringBitmap, but I still need to prepend the
                // group size for level 0 (= 1) to it
                buffer.clear();
                buffer.push(1);
                // then we extend the buffer with the docids bitmap
                match database.get(wtxn, key)? {
                    Some(prev_value) => {
                        let old_bitmap = &prev_value[1..];
                        CboRoaringBitmapCodec::merge_into(
                            &[Cow::Borrowed(value), Cow::Borrowed(old_bitmap)],
                            &mut buffer,
                        )?;
                    }
                    None => {
                        buffer.extend_from_slice(value);
                    }
                };
                database.put(wtxn, key, &buffer)?;
            }
        }
        Ok(())
    }
    fn compute_levels_for_field_id(
        &self,
        field_id: FieldId,
        txn: &RoTxn,
    ) -> Result<(Vec<grenad::Reader<File>>, RoaringBitmap)> {
        let mut all_docids = RoaringBitmap::new();
        let subwriters = self.compute_higher_levels(txn, field_id, 32, &mut |bitmaps, _| {
            for bitmap in bitmaps {
                all_docids |= bitmap;
            }
            Ok(())
        })?;

        Ok((subwriters, all_docids))
    }
    #[allow(clippy::type_complexity)]
    fn read_level_0<'t>(
        &self,
        rtxn: &'t RoTxn,
        field_id: u16,
        handle_group: &mut dyn FnMut(&[RoaringBitmap], &'t [u8]) -> Result<()>,
    ) -> Result<()> {
        // we read the elements one by one and
        // 1. keep track of the left bound
        // 2. fill the `bitmaps` vector to give it to level 1 once `level_group_size` elements were read
        let mut bitmaps = vec![];

        let mut level_0_prefix = vec![];
        level_0_prefix.extend_from_slice(&field_id.to_be_bytes());
        level_0_prefix.push(0);

        let level_0_iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, ByteSlice>(rtxn, level_0_prefix.as_slice())?
            .remap_types::<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>();

        let mut left_bound: &[u8] = &[];
        let mut first_iteration_for_new_group = true;
        for el in level_0_iter {
            let (key, value) = el?;
            let bound = key.left_bound;
            let docids = value.bitmap;

            if first_iteration_for_new_group {
                left_bound = bound;
                first_iteration_for_new_group = false;
            }
            bitmaps.push(docids);

            if bitmaps.len() == self.group_size as usize {
                handle_group(&bitmaps, left_bound)?;
                first_iteration_for_new_group = true;
                bitmaps.clear();
            }
        }
        // don't forget to give the leftover bitmaps as well
        if !bitmaps.is_empty() {
            handle_group(&bitmaps, left_bound)?;
            bitmaps.clear();
        }
        Ok(())
    }

    /// Compute the content of the database levels from its level 0 for the given field id.
    ///
    /// ## Returns:
    /// A vector of grenad::Reader. The reader at index `i` corresponds to the elements of level `i + 1`
    /// that must be inserted into the database.
    #[allow(clippy::type_complexity)]
    fn compute_higher_levels<'t>(
        &self,
        rtxn: &'t RoTxn,
        field_id: u16,
        level: u8,
        handle_group: &mut dyn FnMut(&[RoaringBitmap], &'t [u8]) -> Result<()>,
    ) -> Result<Vec<grenad::Reader<File>>> {
        if level == 0 {
            self.read_level_0(rtxn, field_id, handle_group)?;
            // Level 0 is already in the database
            return Ok(vec![]);
        }
        // level >= 1
        // we compute each element of this level based on the elements of the level below it
        // once we have computed `level_group_size` elements, we give the left bound
        // of those elements, and their bitmaps, to the level above

        let mut cur_writer = create_writer(CompressionType::None, None, tempfile::tempfile()?);
        let mut cur_writer_len: usize = 0;

        let mut group_sizes = vec![];
        let mut left_bounds = vec![];
        let mut bitmaps = vec![];

        // compute the levels below
        // in the callback, we fill `cur_writer` with the correct elements for this level
        let mut sub_writers = self.compute_higher_levels(
            rtxn,
            field_id,
            level - 1,
            &mut |sub_bitmaps, left_bound| {
                let mut combined_bitmap = RoaringBitmap::default();
                for bitmap in sub_bitmaps {
                    combined_bitmap |= bitmap;
                }
                group_sizes.push(sub_bitmaps.len() as u8);
                left_bounds.push(left_bound);

                bitmaps.push(combined_bitmap);
                if bitmaps.len() != self.group_size as usize {
                    return Ok(());
                }
                let left_bound = left_bounds.first().unwrap();
                handle_group(&bitmaps, left_bound)?;

                for ((bitmap, left_bound), group_size) in
                    bitmaps.drain(..).zip(left_bounds.drain(..)).zip(group_sizes.drain(..))
                {
                    let key = FacetGroupKey { field_id, level, left_bound };
                    let key = FacetGroupKeyCodec::<ByteSliceRefCodec>::bytes_encode(&key)
                        .ok_or(Error::Encoding)?;
                    let value = FacetGroupValue { size: group_size, bitmap };
                    let value =
                        FacetGroupValueCodec::bytes_encode(&value).ok_or(Error::Encoding)?;
                    cur_writer.insert(key, value)?;
                    cur_writer_len += 1;
                }
                Ok(())
            },
        )?;
        // don't forget to insert the leftover elements into the writer as well

        // but only do so if the current number of elements to be inserted into this
        // levelcould grow to the minimum level size

        if !bitmaps.is_empty() && (cur_writer_len >= self.min_level_size as usize - 1) {
            // the length of bitmaps is between 0 and group_size
            assert!(bitmaps.len() < self.group_size as usize);
            assert!(cur_writer_len > 0);

            let left_bound = left_bounds.first().unwrap();
            handle_group(&bitmaps, left_bound)?;

            // Note: how many bitmaps are there here?
            for ((bitmap, left_bound), group_size) in
                bitmaps.drain(..).zip(left_bounds.drain(..)).zip(group_sizes.drain(..))
            {
                let key = FacetGroupKey { field_id, level, left_bound };
                let key = FacetGroupKeyCodec::<ByteSliceRefCodec>::bytes_encode(&key)
                    .ok_or(Error::Encoding)?;
                let value = FacetGroupValue { size: group_size, bitmap };
                let value = FacetGroupValueCodec::bytes_encode(&value).ok_or(Error::Encoding)?;
                cur_writer.insert(key, value)?;
                cur_writer_len += 1;
            }
        }
        // if we inserted enough elements to reach the minimum level size, then we push the writer
        if cur_writer_len as u8 >= self.min_level_size {
            sub_writers.push(writer_into_reader(cur_writer)?);
        } else {
            // otherwise, if there are still leftover elements, we give them to the level above
            // this is necessary in order to get the union of all docids
            if !bitmaps.is_empty() {
                handle_group(&bitmaps, left_bounds.first().unwrap())?;
            }
        }
        Ok(sub_writers)
    }
}

#[cfg(test)]
mod tests {
    use std::iter::once;

    use roaring::RoaringBitmap;

    use crate::heed_codec::facet::OrderedF64Codec;
    use crate::milli_snap;
    use crate::update::facet::tests::FacetIndex;

    #[test]
    fn insert() {
        let test = |name: &str, group_size: u8, min_level_size: u8| {
            let index =
                FacetIndex::<OrderedF64Codec>::new(group_size, 0 /*NA*/, min_level_size);

            let mut elements = Vec::<((u16, f64), RoaringBitmap)>::new();
            for i in 0..1_000u32 {
                // field id = 0, left_bound = i, docids = [i]
                elements.push(((0, i as f64), once(i).collect()));
            }
            for i in 0..100u32 {
                // field id = 1, left_bound = i, docids = [i]
                elements.push(((1, i as f64), once(i).collect()));
            }
            let mut wtxn = index.env.write_txn().unwrap();
            index.bulk_insert(&mut wtxn, &[0, 1], elements.iter());

            index.verify_structure_validity(&wtxn, 0);
            index.verify_structure_validity(&wtxn, 1);

            wtxn.commit().unwrap();

            milli_snap!(format!("{index}"), name);
        };

        test("default", 4, 5);
        test("small_group_small_min_level", 2, 2);
        test("small_group_large_min_level", 2, 128);
        test("large_group_small_min_level", 16, 2);
        test("odd_group_odd_min_level", 7, 3);
    }
    #[test]
    fn insert_delete_field_insert() {
        let test = |name: &str, group_size: u8, min_level_size: u8| {
            let index =
                FacetIndex::<OrderedF64Codec>::new(group_size, 0 /*NA*/, min_level_size);
            let mut wtxn = index.env.write_txn().unwrap();

            let mut elements = Vec::<((u16, f64), RoaringBitmap)>::new();
            for i in 0..100u32 {
                // field id = 0, left_bound = i, docids = [i]
                elements.push(((0, i as f64), once(i).collect()));
            }
            for i in 0..100u32 {
                // field id = 1, left_bound = i, docids = [i]
                elements.push(((1, i as f64), once(i).collect()));
            }
            index.bulk_insert(&mut wtxn, &[0, 1], elements.iter());

            index.verify_structure_validity(&wtxn, 0);
            index.verify_structure_validity(&wtxn, 1);
            // delete all the elements for the facet id 0
            for i in 0..100u32 {
                index.delete_single_docid(&mut wtxn, 0, &(i as f64), i);
            }
            index.verify_structure_validity(&wtxn, 0);
            index.verify_structure_validity(&wtxn, 1);

            let mut elements = Vec::<((u16, f64), RoaringBitmap)>::new();
            // then add some elements again for the facet id 1
            for i in 0..110u32 {
                // field id = 1, left_bound = i, docids = [i]
                elements.push(((1, i as f64), once(i).collect()));
            }
            index.verify_structure_validity(&wtxn, 0);
            index.verify_structure_validity(&wtxn, 1);
            index.bulk_insert(&mut wtxn, &[0, 1], elements.iter());

            wtxn.commit().unwrap();

            milli_snap!(format!("{index}"), name);
        };

        test("default", 4, 5);
        test("small_group_small_min_level", 2, 2);
        test("small_group_large_min_level", 2, 128);
        test("large_group_small_min_level", 16, 2);
        test("odd_group_odd_min_level", 7, 3);
    }
}

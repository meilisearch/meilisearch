use std::fs::File;
use std::io::BufReader;

use grenad::{CompressionType, Merger};
use heed::types::Bytes;
use heed::{BytesDecode, BytesEncode, Error, PutFlags, RoTxn, RwTxn};
use roaring::RoaringBitmap;

use super::{FACET_GROUP_SIZE, FACET_MIN_LEVEL_SIZE};
use crate::facet::FacetType;
use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValue, FacetGroupValueCodec,
};
use crate::heed_codec::BytesRefCodec;
use crate::update::del_add::{DelAdd, KvReaderDelAdd};
use crate::update::index_documents::{create_writer, valid_lmdb_key, writer_into_reader};
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{CboRoaringBitmapCodec, CboRoaringBitmapLenCodec, FieldId, Index, Result};

/// Algorithm to insert elememts into the `facet_id_(string/f64)_docids` databases
/// by rebuilding the database "from scratch".
///
/// First, the new elements are inserted into the level 0 of the database. Then, the
/// higher levels are cleared and recomputed from the content of level 0.
pub struct FacetsUpdateBulk<'i> {
    index: &'i Index,
    group_size: u8,
    min_level_size: u8,
    facet_type: FacetType,
    field_ids: Vec<FieldId>,
    // None if level 0 does not need to be updated
    delta_data: Option<Merger<BufReader<File>, MergeDeladdCboRoaringBitmaps>>,
}

impl<'i> FacetsUpdateBulk<'i> {
    pub fn new(
        index: &'i Index,
        field_ids: Vec<FieldId>,
        facet_type: FacetType,
        delta_data: Merger<BufReader<File>, MergeDeladdCboRoaringBitmaps>,
        group_size: u8,
        min_level_size: u8,
    ) -> FacetsUpdateBulk<'i> {
        FacetsUpdateBulk {
            index,
            field_ids,
            group_size,
            min_level_size,
            facet_type,
            delta_data: Some(delta_data),
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
            delta_data: None,
        }
    }

    #[tracing::instrument(level = "trace", skip_all, target = "indexing::facets::bulk")]
    pub fn execute(self, wtxn: &mut heed::RwTxn<'_>) -> Result<()> {
        let Self { index, field_ids, group_size, min_level_size, facet_type, delta_data } = self;

        let db = match facet_type {
            FacetType::String => {
                index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>()
            }
            FacetType::Number => {
                index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>()
            }
        };

        let inner = FacetsUpdateBulkInner { db, delta_data, group_size, min_level_size };

        inner.update(wtxn, &field_ids)?;

        Ok(())
    }
}

/// Implementation of `FacetsUpdateBulk` that is independent of milli's `Index` type
pub(crate) struct FacetsUpdateBulkInner<R: std::io::Read + std::io::Seek> {
    pub db: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    pub delta_data: Option<Merger<R, MergeDeladdCboRoaringBitmaps>>,
    pub group_size: u8,
    pub min_level_size: u8,
}
impl<R: std::io::Read + std::io::Seek> FacetsUpdateBulkInner<R> {
    pub fn update(mut self, wtxn: &mut RwTxn<'_>, field_ids: &[u16]) -> Result<()> {
        self.update_level0(wtxn)?;
        for &field_id in field_ids.iter() {
            self.clear_levels(wtxn, field_id)?;
        }

        for &field_id in field_ids.iter() {
            let level_readers = self.compute_levels_for_field_id(field_id, wtxn)?;

            for level_reader in level_readers {
                let mut cursor = level_reader.into_cursor()?;
                while let Some((k, v)) = cursor.move_on_next()? {
                    self.db.remap_types::<Bytes, Bytes>().put(wtxn, k, v)?;
                }
            }
        }
        Ok(())
    }

    fn clear_levels(&self, wtxn: &mut heed::RwTxn<'_>, field_id: FieldId) -> Result<()> {
        let left = FacetGroupKey::<&[u8]> { field_id, level: 1, left_bound: &[] };
        let right = FacetGroupKey::<&[u8]> { field_id, level: u8::MAX, left_bound: &[] };
        let range = left..=right;
        self.db.delete_range(wtxn, &range).map(drop)?;
        Ok(())
    }

    fn update_level0(&mut self, wtxn: &mut RwTxn<'_>) -> Result<()> {
        let delta_data = match self.delta_data.take() {
            Some(x) => x,
            None => return Ok(()),
        };
        if self.db.is_empty(wtxn)? {
            let mut buffer = Vec::new();
            let mut database = self.db.iter_mut(wtxn)?.remap_types::<Bytes, Bytes>();
            let mut iter = delta_data.into_stream_merger_iter()?;
            while let Some((key, value)) = iter.next()? {
                if !valid_lmdb_key(key) {
                    continue;
                }
                let value = KvReaderDelAdd::from_slice(value);

                // DB is empty, it is safe to ignore Del operations
                let Some(value) = value.get(DelAdd::Addition) else {
                    continue;
                };

                buffer.clear();
                // the group size for level 0
                buffer.push(1);
                // then we extend the buffer with the docids bitmap
                buffer.extend_from_slice(value);
                unsafe {
                    database.put_current_with_options::<Bytes>(PutFlags::APPEND, key, &buffer)?
                };
            }
        } else {
            let mut buffer = Vec::new();
            let database = self.db.remap_types::<Bytes, Bytes>();

            let mut iter = delta_data.into_stream_merger_iter()?;
            while let Some((key, value)) = iter.next()? {
                if !valid_lmdb_key(key) {
                    continue;
                }

                let value = KvReaderDelAdd::from_slice(value);

                // the value is a CboRoaringBitmap, but I still need to prepend the
                // group size for level 0 (= 1) to it
                buffer.clear();
                buffer.push(1);
                // then we extend the buffer with the docids bitmap
                match database.get(wtxn, key)? {
                    Some(prev_value) => {
                        // prev_value is the group size for level 0, followed by the previous bitmap.
                        let old_bitmap = &prev_value[1..];
                        CboRoaringBitmapCodec::merge_deladd_into(value, old_bitmap, &mut buffer)?;
                    }
                    None => {
                        // it is safe to ignore the del in that case.
                        let Some(value) = value.get(DelAdd::Addition) else {
                            // won't put the key in DB as the value would be empty
                            continue;
                        };

                        buffer.extend_from_slice(value);
                    }
                };
                let new_bitmap = &buffer[1..];
                // if the new bitmap is empty, let's remove it
                if CboRoaringBitmapLenCodec::bytes_decode(new_bitmap).unwrap_or_default() == 0 {
                    database.delete(wtxn, key)?;
                } else {
                    database.put(wtxn, key, &buffer)?;
                }
            }
        }
        Ok(())
    }
    fn compute_levels_for_field_id(
        &self,
        field_id: FieldId,
        txn: &RoTxn<'_>,
    ) -> Result<Vec<grenad::Reader<BufReader<File>>>> {
        let subwriters = self.compute_higher_levels(txn, field_id, 32, &mut |_, _| Ok(()))?;

        Ok(subwriters)
    }
    #[allow(clippy::type_complexity)]
    fn read_level_0<'t>(
        &self,
        rtxn: &'t RoTxn<'t>,
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
            .remap_types::<Bytes, Bytes>()
            .prefix_iter(rtxn, level_0_prefix.as_slice())?
            .remap_types::<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>();

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
        rtxn: &'t RoTxn<'t>,
        field_id: u16,
        level: u8,
        handle_group: &mut dyn FnMut(&[RoaringBitmap], &'t [u8]) -> Result<()>,
    ) -> Result<Vec<grenad::Reader<BufReader<File>>>> {
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
                // The conversion of sub_bitmaps.len() to a u8 will always be correct
                // since its length is bounded by max_group_size, which is a u8.
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
                    let key = FacetGroupKeyCodec::<BytesRefCodec>::bytes_encode(&key)
                        .map_err(Error::Encoding)?;
                    let value = FacetGroupValue { size: group_size, bitmap };
                    let value =
                        FacetGroupValueCodec::bytes_encode(&value).map_err(Error::Encoding)?;
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
                let key = FacetGroupKeyCodec::<BytesRefCodec>::bytes_encode(&key)
                    .map_err(Error::Encoding)?;
                let value = FacetGroupValue { size: group_size, bitmap };
                let value = FacetGroupValueCodec::bytes_encode(&value).map_err(Error::Encoding)?;
                cur_writer.insert(key, value)?;
                cur_writer_len += 1;
            }
        }
        // if we inserted enough elements to reach the minimum level size, then we push the writer
        if cur_writer_len >= self.min_level_size as usize {
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

    use big_s::S;
    use maplit::hashset;
    use roaring::RoaringBitmap;

    use crate::documents::mmap_from_objects;
    use crate::heed_codec::facet::OrderedF64Codec;
    use crate::heed_codec::StrRefCodec;
    use crate::index::tests::TempIndex;
    use crate::update::facet::test_helpers::{ordered_string, FacetIndex};
    use crate::{db_snap, milli_snap};

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

    #[test]
    fn bug_3165() {
        // Indexing a number of facet values that falls within certains ranges (e.g. 22_540 qualifies)
        // would lead to a facet DB which was missing some levels.
        // That was because before writing a level into the database, we would
        // check that its size was higher than the minimum level size using
        // a lossy integer conversion: `level_size as u8 >= min_level_size`.
        //
        // This missing level in the facet DBs would make the incremental indexer
        // (and other search algorithms) crash.
        //
        // https://github.com/meilisearch/meilisearch/issues/3165
        let index = TempIndex::new_with_map_size(4096 * 1000 * 100);

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
                settings.set_filterable_fields(hashset! { S("id") });
            })
            .unwrap();

        let mut documents = vec![];
        for i in 0..=22_540 {
            documents.push(
                serde_json::json! {
                    {
                        "id": i as u64,
                    }
                }
                .as_object()
                .unwrap()
                .clone(),
            );
        }

        let documents = mmap_from_objects(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, facet_id_f64_docids, "initial", @"c34f499261f3510d862fa0283bbe843a");
    }

    #[test]
    fn insert_string() {
        let test = |name: &str, group_size: u8, min_level_size: u8| {
            let index = FacetIndex::<StrRefCodec>::new(group_size, 0 /*NA*/, min_level_size);

            let strings = (0..1_000).map(|i| ordered_string(i as usize)).collect::<Vec<_>>();
            let mut elements = Vec::<((u16, &str), RoaringBitmap)>::new();
            for i in 0..1_000u32 {
                // field id = 0, left_bound = i, docids = [i]
                elements.push(((0, &strings[i as usize]), once(i).collect()));
            }
            for i in 0..100u32 {
                // field id = 1, left_bound = i, docids = [i]
                elements.push(((1, &strings[i as usize]), once(i).collect()));
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
}

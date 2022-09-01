use crate::error::InternalError;
use crate::heed_codec::facet::new::{
    FacetGroupValue, FacetGroupValueCodec, FacetKey, FacetKeyCodec, MyByteSlice,
};
use crate::update::index_documents::{create_writer, write_into_lmdb_database, writer_into_reader};
use crate::{FieldId, Index, Result};
use grenad::CompressionType;
use heed::types::ByteSlice;
use heed::{BytesEncode, Error, RoTxn, RwTxn};
use log::debug;
use roaring::RoaringBitmap;
use std::cmp;
use std::fs::File;
use std::num::NonZeroUsize;
use time::OffsetDateTime;

pub struct FacetsUpdateBulk<'i> {
    index: &'i Index,
    database: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    level_group_size: usize,
    min_level_size: usize,
    put_faceted_docids_in_main: fn(&Index, &mut RwTxn, FieldId, &RoaringBitmap) -> heed::Result<()>,
}

impl<'i> FacetsUpdateBulk<'i> {
    pub fn new(
        index: &'i Index,
        database: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
        put_faceted_docids_in_main: fn(
            &Index,
            &mut RwTxn,
            FieldId,
            &RoaringBitmap,
        ) -> heed::Result<()>,
    ) -> FacetsUpdateBulk<'i> {
        FacetsUpdateBulk {
            index,
            database,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            level_group_size: 4,
            min_level_size: 5,
            put_faceted_docids_in_main,
        }
    }

    /// The number of elements from the level below that are represented by a single element in the level above
    ///
    /// This setting is always greater than or equal to 2.
    pub fn level_group_size(&mut self, value: NonZeroUsize) -> &mut Self {
        self.level_group_size = cmp::max(value.get(), 2);
        self
    }

    /// The minimum number of elements that a level is allowed to have.
    pub fn min_level_size(&mut self, value: NonZeroUsize) -> &mut Self {
        self.min_level_size = value.get();
        self
    }

    fn clear_levels(&self, wtxn: &mut heed::RwTxn, field_id: FieldId) -> Result<()> {
        let left = FacetKey::<&[u8]> { field_id, level: 1, left_bound: &[] };
        let right = FacetKey::<&[u8]> { field_id, level: u8::MAX, left_bound: &[] };
        let range = left..=right;
        self.database.delete_range(wtxn, &range).map(drop)?;
        Ok(())
    }

    #[logging_timer::time("FacetsUpdateBulk::{}")]
    pub fn execute(self, wtxn: &mut heed::RwTxn) -> Result<()> {
        self.index.set_updated_at(wtxn, &OffsetDateTime::now_utc())?;
        // We get the faceted fields to be able to create the facet levels.
        let faceted_fields = self.index.faceted_fields_ids(wtxn)?.clone();

        debug!("Computing and writing the facet values levels docids into LMDB on disk...");

        for &field_id in faceted_fields.iter() {
            self.clear_levels(wtxn, field_id)?;
        }

        let mut nested_wtxn = self.index.env.nested_write_txn(wtxn)?;

        for &field_id in faceted_fields.iter() {
            let (level_readers, all_docids) =
                self.compute_levels_for_field_id(field_id, &nested_wtxn)?;

            (self.put_faceted_docids_in_main)(
                &self.index,
                &mut nested_wtxn,
                field_id,
                &all_docids,
            )?;

            for level_reader in level_readers {
                // TODO: append instead of write with merge
                write_into_lmdb_database(
                    &mut nested_wtxn,
                    *self.database.as_polymorph(),
                    level_reader,
                    |_, _| {
                        Err(InternalError::IndexingMergingKeys { process: "facet string levels" })?
                    },
                )?;
            }
        }

        Ok(())
    }

    fn compute_levels_for_field_id(
        &self,
        field_id: FieldId,
        txn: &RoTxn,
    ) -> Result<(Vec<grenad::Reader<File>>, RoaringBitmap)> {
        let algo = FacetsUpdateBulkAlgorithm {
            rtxn: txn,
            db: &self.database,
            field_id,
            level_group_size: self.level_group_size,
            min_level_size: self.min_level_size,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
        };
        // TODO: first check whether there is anything in level 0

        let mut all_docids = RoaringBitmap::new();
        let subwriters = algo.compute_higher_levels(32, &mut |bitmaps, _| {
            for bitmap in bitmaps {
                all_docids |= bitmap;
            }
            Ok(())
        })?;
        drop(algo);

        Ok((subwriters, all_docids))
    }
}

pub struct FacetsUpdateBulkAlgorithm<'t> {
    rtxn: &'t heed::RoTxn<'t>,
    db: &'t heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    chunk_compression_type: CompressionType,
    chunk_compression_level: Option<u32>,
    field_id: u16,
    level_group_size: usize,
    min_level_size: usize,
}
impl<'t> FacetsUpdateBulkAlgorithm<'t> {
    fn read_level_0(
        &self,
        handle_group: &mut dyn FnMut(&[RoaringBitmap], &'t [u8]) -> Result<()>,
    ) -> Result<()> {
        // we read the elements one by one and
        // 1. keep track of the left bound
        // 2. fill the `bitmaps` vector to give it to level 1 once `level_group_size` elements were read
        let mut bitmaps = vec![];

        let mut level_0_prefix = vec![];
        level_0_prefix.extend_from_slice(&self.field_id.to_be_bytes());
        level_0_prefix.push(0);

        let level_0_iter = self
            .db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, ByteSlice>(self.rtxn, level_0_prefix.as_slice())?
            .remap_types::<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>();

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

            if bitmaps.len() == self.level_group_size {
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
    /// 1. a vector of grenad::Reader. The reader at index `i` corresponds to the elements of level `i + 1`
    /// that must be inserted into the database.
    /// 2. a roaring bitmap of all the document ids present in the database
    fn compute_higher_levels(
        &self,
        level: u8,
        handle_group: &mut dyn FnMut(&[RoaringBitmap], &'t [u8]) -> Result<()>,
    ) -> Result<Vec<grenad::Reader<File>>> {
        if level == 0 {
            self.read_level_0(handle_group)?;
            // Level 0 is already in the database
            return Ok(vec![]);
        }
        // level >= 1
        // we compute each element of this level based on the elements of the level below it
        // once we have computed `level_group_size` elements, we give the left bound
        // of those elements, and their bitmaps, to the level above

        let mut cur_writer = create_writer(
            self.chunk_compression_type,
            self.chunk_compression_level,
            tempfile::tempfile()?,
        );
        let mut cur_writer_len = 0;

        let mut group_sizes = vec![];
        let mut left_bounds = vec![];
        let mut bitmaps = vec![];

        // compute the levels below
        // in the callback, we fill `cur_writer` with the correct elements for this level
        let mut sub_writers =
            self.compute_higher_levels(level - 1, &mut |sub_bitmaps, left_bound| {
                let mut combined_bitmap = RoaringBitmap::default();
                for bitmap in sub_bitmaps {
                    combined_bitmap |= bitmap;
                }
                group_sizes.push(sub_bitmaps.len() as u8);
                left_bounds.push(left_bound);

                bitmaps.push(combined_bitmap);
                if bitmaps.len() != self.level_group_size {
                    return Ok(());
                }
                let left_bound = left_bounds.first().unwrap();
                handle_group(&bitmaps, left_bound)?;

                for ((bitmap, left_bound), group_size) in
                    bitmaps.drain(..).zip(left_bounds.drain(..)).zip(group_sizes.drain(..))
                {
                    let key = FacetKey { field_id: self.field_id, level, left_bound };
                    let key =
                        FacetKeyCodec::<MyByteSlice>::bytes_encode(&key).ok_or(Error::Encoding)?;
                    let value = FacetGroupValue { size: group_size, bitmap };
                    let value =
                        FacetGroupValueCodec::bytes_encode(&value).ok_or(Error::Encoding)?;
                    cur_writer.insert(key, value)?;
                    cur_writer_len += 1;
                }
                Ok(())
            })?;
        // don't forget to insert the leftover elements into the writer as well
        if !bitmaps.is_empty() && cur_writer_len >= self.level_group_size * self.min_level_size {
            let left_bound = left_bounds.first().unwrap();
            handle_group(&bitmaps, left_bound)?;
            for ((bitmap, left_bound), group_size) in
                bitmaps.drain(..).zip(left_bounds.drain(..)).zip(group_sizes.drain(..))
            {
                let key = FacetKey { field_id: self.field_id, level, left_bound };
                let key =
                    FacetKeyCodec::<MyByteSlice>::bytes_encode(&key).ok_or(Error::Encoding)?;
                let value = FacetGroupValue { size: group_size, bitmap };
                let value = FacetGroupValueCodec::bytes_encode(&value).ok_or(Error::Encoding)?;
                cur_writer.insert(key, value)?;
                cur_writer_len += 1;
            }
        }
        if cur_writer_len > self.level_group_size * self.min_level_size {
            sub_writers.push(writer_into_reader(cur_writer)?);
        }
        return Ok(sub_writers);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::db_snap;
    use crate::documents::documents_batch_reader_from_objects;
    use crate::index::tests::TempIndex;

    #[test]
    fn test_facets_number() {
        let test =
            |name: &str, group_size: Option<NonZeroUsize>, min_level_size: Option<NonZeroUsize>| {
                let mut index = TempIndex::new_with_map_size(4096 * 1000 * 10); // 40MB
                index.index_documents_config.autogenerate_docids = true;
                index.index_documents_config.facet_level_group_size = group_size;
                index.index_documents_config.facet_min_level_size = min_level_size;

                index
                    .update_settings(|settings| {
                        settings.set_filterable_fields(
                            IntoIterator::into_iter(["facet".to_owned(), "facet2".to_owned()])
                                .collect(),
                        );
                    })
                    .unwrap();

                let mut documents = vec![];
                for i in 0..1_000 {
                    documents.push(serde_json::json!({ "facet": i }).as_object().unwrap().clone());
                }
                for i in 0..100 {
                    documents.push(serde_json::json!({ "facet2": i }).as_object().unwrap().clone());
                }
                let documents = documents_batch_reader_from_objects(documents);

                index.add_documents(documents).unwrap();

                db_snap!(index, facet_id_f64_docids, name);
            };

        test("default", None, None);
        test("tiny_groups_tiny_levels", NonZeroUsize::new(1), NonZeroUsize::new(1));
        test("small_groups_small_levels", NonZeroUsize::new(2), NonZeroUsize::new(2));
        test("small_groups_large_levels", NonZeroUsize::new(2), NonZeroUsize::new(128));
        test("large_groups_small_levels", NonZeroUsize::new(16), NonZeroUsize::new(2));
        test("large_groups_large_levels", NonZeroUsize::new(16), NonZeroUsize::new(256));
    }

    #[test]
    fn test_facets_string() {
        let test = |name: &str,
                    group_size: Option<NonZeroUsize>,
                    min_level_size: Option<NonZeroUsize>| {
            let mut index = TempIndex::new_with_map_size(4096 * 1000 * 10); // 40MB
            index.index_documents_config.autogenerate_docids = true;
            index.index_documents_config.facet_level_group_size = group_size;
            index.index_documents_config.facet_min_level_size = min_level_size;

            index
                .update_settings(|settings| {
                    settings.set_filterable_fields(
                        IntoIterator::into_iter(["facet".to_owned(), "facet2".to_owned()])
                            .collect(),
                    );
                })
                .unwrap();

            let mut documents = vec![];
            for i in 0..100 {
                documents.push(
                    serde_json::json!({ "facet": format!("s{i:X}") }).as_object().unwrap().clone(),
                );
            }
            for i in 0..10 {
                documents.push(
                    serde_json::json!({ "facet2": format!("s{i:X}") }).as_object().unwrap().clone(),
                );
            }
            let documents = documents_batch_reader_from_objects(documents);

            index.add_documents(documents).unwrap();

            db_snap!(index, facet_id_string_docids, name);
        };

        test("default", None, None);
        test("tiny_groups_tiny_levels", NonZeroUsize::new(1), NonZeroUsize::new(1));
    }
}

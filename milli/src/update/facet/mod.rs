use self::incremental::FacetsUpdateIncremental;
use super::FacetsUpdateBulk;
use crate::facet::FacetType;
use crate::heed_codec::facet::{ByteSliceRef, FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::{Index, Result};
use std::fs::File;

pub mod bulk;
pub mod incremental;

pub struct FacetsUpdate<'i> {
    index: &'i Index,
    database: heed::Database<FacetGroupKeyCodec<ByteSliceRef>, FacetGroupValueCodec>,
    facet_type: FacetType,
    new_data: grenad::Reader<File>,
    level_group_size: u8,
    max_level_group_size: u8,
    min_level_size: u8,
}
impl<'i> FacetsUpdate<'i> {
    pub fn new(index: &'i Index, facet_type: FacetType, new_data: grenad::Reader<File>) -> Self {
        let database = match facet_type {
            FacetType::String => {
                index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRef>>()
            }
            FacetType::Number => {
                index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRef>>()
            }
        };
        Self {
            index,
            database,
            level_group_size: 4,
            max_level_group_size: 8,
            min_level_size: 5,
            facet_type,
            new_data,
        }
    }

    // TODO: use the options below?
    // but I don't actually see why they should be configurable
    // /// The minimum number of elements that a level is allowed to have.
    // pub fn level_max_group_size(mut self, value: u8) -> Self {
    //     self.max_level_group_size = std::cmp::max(value, 4);
    //     self
    // }

    // /// The number of elements from the level below that are represented by a single element in the level above
    // ///
    // /// This setting is always greater than or equal to 2.
    // pub fn level_group_size(mut self, value: u8) -> Self {
    //     self.level_group_size = std::cmp::max(value, 2);
    //     self
    // }

    // /// The minimum number of elements that a level is allowed to have.
    // pub fn min_level_size(mut self, value: u8) -> Self {
    //     self.min_level_size = std::cmp::max(value, 2);
    //     self
    // }

    pub fn execute(self, wtxn: &mut heed::RwTxn) -> Result<()> {
        if self.new_data.is_empty() {
            return Ok(());
        }
        // here, come up with a better condition!
        // ideally we'd choose which method to use for each field id individually
        // but I dont' think it's worth the effort yet
        // As a first requirement, we ask that the length of the new data is less
        // than a 1/50th of the length of the database in order to use the incremental
        // method.
        if self.new_data.len() >= (self.database.len(wtxn)? as u64 / 50) {
            let bulk_update = FacetsUpdateBulk::new(self.index, self.facet_type, self.new_data)
                .level_group_size(self.level_group_size)
                .min_level_size(self.min_level_size);
            bulk_update.execute(wtxn)?;
        } else {
            let incremental_update =
                FacetsUpdateIncremental::new(self.index, self.facet_type, self.new_data)
                    .group_size(self.level_group_size)
                    .max_group_size(self.max_level_group_size)
                    .min_level_size(self.min_level_size);
            incremental_update.execute(wtxn)?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::bulk::FacetsUpdateBulkInner;
    use crate::heed_codec::facet::{
        ByteSliceRef, FacetGroupKey, FacetGroupKeyCodec, FacetGroupValue, FacetGroupValueCodec,
    };
    use crate::search::facet::get_highest_level;
    use crate::snapshot_tests::display_bitmap;
    use crate::update::FacetsUpdateIncrementalInner;
    use crate::CboRoaringBitmapCodec;
    use heed::types::ByteSlice;
    use heed::{BytesDecode, BytesEncode, Env, RoTxn, RwTxn};
    use roaring::RoaringBitmap;
    use std::fmt::Display;
    use std::marker::PhantomData;
    use std::rc::Rc;

    // A dummy index that only contains the facet database, used for testing
    pub struct FacetIndex<BoundCodec>
    where
        for<'a> BoundCodec:
            BytesEncode<'a> + BytesDecode<'a, DItem = <BoundCodec as BytesEncode<'a>>::EItem>,
    {
        pub env: Env,
        pub content: heed::Database<FacetGroupKeyCodec<ByteSliceRef>, FacetGroupValueCodec>,
        pub group_size: u8,
        pub min_level_size: u8,
        pub max_group_size: u8,
        _tempdir: Rc<tempfile::TempDir>,
        _phantom: PhantomData<BoundCodec>,
    }

    impl<BoundCodec> FacetIndex<BoundCodec>
    where
        for<'a> BoundCodec:
            BytesEncode<'a> + BytesDecode<'a, DItem = <BoundCodec as BytesEncode<'a>>::EItem>,
    {
        #[cfg(all(test, fuzzing))]
        pub fn open_from_tempdir(
            tempdir: Rc<tempfile::TempDir>,
            group_size: u8,
            max_group_size: u8,
            min_level_size: u8,
        ) -> FacetIndex<BoundCodec> {
            let group_size = std::cmp::min(127, std::cmp::max(group_size, 2)); // 2 <= x <= 127
            let max_group_size = std::cmp::min(127, std::cmp::max(group_size * 2, max_group_size)); // 2*group_size <= x <= 127
            let min_level_size = std::cmp::max(1, min_level_size); // 1 <= x <= inf

            let mut options = heed::EnvOpenOptions::new();
            let options = options.map_size(4096 * 4 * 10 * 100);
            unsafe {
                options.flag(heed::flags::Flags::MdbAlwaysFreePages);
            }
            let env = options.open(tempdir.path()).unwrap();
            let content = env.open_database(None).unwrap().unwrap();

            FacetIndex {
                db: Database {
                    content,
                    group_size,
                    max_group_size,
                    min_level_size,
                    _tempdir: tempdir,
                },
                env,
                _phantom: PhantomData,
            }
        }
        pub fn new(
            group_size: u8,
            max_group_size: u8,
            min_level_size: u8,
        ) -> FacetIndex<BoundCodec> {
            let group_size = std::cmp::min(127, std::cmp::max(group_size, 2)); // 2 <= x <= 127
            let max_group_size = std::cmp::min(127, std::cmp::max(group_size * 2, max_group_size)); // 2*group_size <= x <= 127
            let min_level_size = std::cmp::max(1, min_level_size); // 1 <= x <= inf
            let mut options = heed::EnvOpenOptions::new();
            let options = options.map_size(4096 * 4 * 1000);
            let tempdir = tempfile::TempDir::new().unwrap();
            let env = options.open(tempdir.path()).unwrap();
            let content = env.create_database(None).unwrap();

            FacetIndex {
                content,
                group_size,
                max_group_size,
                min_level_size,
                _tempdir: Rc::new(tempdir),
                env,
                _phantom: PhantomData,
            }
        }
        pub fn insert<'a>(
            &self,
            wtxn: &'a mut RwTxn,
            field_id: u16,
            key: &'a <BoundCodec as BytesEncode<'a>>::EItem,
            docids: &RoaringBitmap,
        ) {
            let update = FacetsUpdateIncrementalInner {
                db: self.content,
                group_size: self.group_size,
                min_level_size: self.min_level_size,
                max_group_size: self.max_group_size,
            };
            let key_bytes = BoundCodec::bytes_encode(&key).unwrap();
            update.insert(wtxn, field_id, &key_bytes, docids).unwrap();
        }
        pub fn delete<'a>(
            &self,
            wtxn: &'a mut RwTxn,
            field_id: u16,
            key: &'a <BoundCodec as BytesEncode<'a>>::EItem,
            value: u32,
        ) {
            let update = FacetsUpdateIncrementalInner {
                db: self.content,
                group_size: self.group_size,
                min_level_size: self.min_level_size,
                max_group_size: self.max_group_size,
            };
            let key_bytes = BoundCodec::bytes_encode(&key).unwrap();
            update.delete(wtxn, field_id, &key_bytes, value).unwrap();
        }

        pub fn bulk_insert<'a, 'b>(
            &self,
            wtxn: &'a mut RwTxn,
            field_ids: &[u16],
            els: impl IntoIterator<
                Item = &'a ((u16, <BoundCodec as BytesEncode<'a>>::EItem), RoaringBitmap),
            >,
        ) where
            for<'c> <BoundCodec as BytesEncode<'c>>::EItem: Sized,
        {
            let mut new_data = vec![];
            let mut writer = grenad::Writer::new(&mut new_data);
            for ((field_id, left_bound), docids) in els {
                let left_bound_bytes = BoundCodec::bytes_encode(left_bound).unwrap().into_owned();
                let key: FacetGroupKey<&[u8]> =
                    FacetGroupKey { field_id: *field_id, level: 0, left_bound: &left_bound_bytes };
                let key = FacetGroupKeyCodec::<ByteSliceRef>::bytes_encode(&key).unwrap();
                let value = CboRoaringBitmapCodec::bytes_encode(&docids).unwrap();
                writer.insert(&key, &value).unwrap();
            }
            writer.finish().unwrap();
            let reader = grenad::Reader::new(std::io::Cursor::new(new_data)).unwrap();

            let update = FacetsUpdateBulkInner {
                db: self.content,
                new_data: Some(reader),
                group_size: self.group_size,
                min_level_size: self.min_level_size,
            };

            update.update(wtxn, field_ids, |_, _, _| Ok(())).unwrap();
        }

        pub fn verify_structure_validity(&self, txn: &RoTxn, field_id: u16) {
            let mut field_id_prefix = vec![];
            field_id_prefix.extend_from_slice(&field_id.to_be_bytes());

            let highest_level = get_highest_level(txn, self.content, field_id).unwrap();

            for level_no in (1..=highest_level).rev() {
                let mut level_no_prefix = vec![];
                level_no_prefix.extend_from_slice(&field_id.to_be_bytes());
                level_no_prefix.push(level_no);

                let mut iter = self
                    .content
                    .as_polymorph()
                    .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(txn, &level_no_prefix)
                    .unwrap();
                while let Some(el) = iter.next() {
                    let (key, value) = el.unwrap();
                    let key = FacetGroupKeyCodec::<ByteSliceRef>::bytes_decode(&key).unwrap();

                    let mut prefix_start_below = vec![];
                    prefix_start_below.extend_from_slice(&field_id.to_be_bytes());
                    prefix_start_below.push(level_no - 1);
                    prefix_start_below.extend_from_slice(&key.left_bound);

                    let start_below = {
                        let mut start_below_iter = self
                            .content
                            .as_polymorph()
                            .prefix_iter::<_, ByteSlice, FacetGroupValueCodec>(
                                txn,
                                &prefix_start_below,
                            )
                            .unwrap();
                        let (key_bytes, _) = start_below_iter.next().unwrap().unwrap();
                        FacetGroupKeyCodec::<ByteSliceRef>::bytes_decode(&key_bytes).unwrap()
                    };

                    assert!(value.size > 0 && value.size < self.max_group_size);

                    let mut actual_size = 0;
                    let mut values_below = RoaringBitmap::new();
                    let mut iter_below = self
                        .content
                        .range(txn, &(start_below..))
                        .unwrap()
                        .take(value.size as usize);
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
    }

    impl<BoundCodec> Display for FacetIndex<BoundCodec>
    where
        for<'a> <BoundCodec as BytesEncode<'a>>::EItem: Sized + Display,
        for<'a> BoundCodec:
            BytesEncode<'a> + BytesDecode<'a, DItem = <BoundCodec as BytesEncode<'a>>::EItem>,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let txn = self.env.read_txn().unwrap();
            let mut iter = self.content.iter(&txn).unwrap();
            while let Some(el) = iter.next() {
                let (key, value) = el.unwrap();
                let FacetGroupKey { field_id, level, left_bound: bound } = key;
                let bound = BoundCodec::bytes_decode(bound).unwrap();
                let FacetGroupValue { size, bitmap } = value;
                writeln!(
                    f,
                    "{field_id:<2} {level:<2} k{bound:<8} {size:<4} {values:?}",
                    values = display_bitmap(&bitmap)
                )?;
            }
            Ok(())
        }
    }
}

#[allow(unused)]
#[cfg(test)]
mod comparison_bench {
    use std::iter::once;

    use rand::Rng;
    use roaring::RoaringBitmap;

    use crate::heed_codec::facet::OrderedF64Codec;

    use super::tests::FacetIndex;

    // This is a simple test to get an intuition on the relative speed
    // of the incremental vs. bulk indexer.
    // It appears that the incremental indexer is about 50 times slower than the
    // bulk indexer.
    #[test]
    fn benchmark_facet_indexing() {
        // then we add 10_000 documents at a time and compare the speed of adding 1, 100, and 1000 documents to it

        let mut facet_value = 0;

        let mut r = rand::thread_rng();

        for i in 1..=20 {
            let size = 50_000 * i;
            let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);

            let mut txn = index.env.write_txn().unwrap();
            let mut elements = Vec::<((u16, f64), RoaringBitmap)>::new();
            for i in 0..size {
                // field id = 0, left_bound = i, docids = [i]
                elements.push(((0, facet_value as f64), once(i).collect()));
                facet_value += 1;
            }
            let timer = std::time::Instant::now();
            index.bulk_insert(&mut txn, &[0], elements.iter());
            let time_spent = timer.elapsed().as_millis();
            println!("bulk {size} : {time_spent}ms");

            txn.commit().unwrap();

            for nbr_doc in [1, 100, 1000, 10_000] {
                let mut txn = index.env.write_txn().unwrap();
                let timer = std::time::Instant::now();
                //
                // insert one document
                //
                for _ in 0..nbr_doc {
                    index.insert(&mut txn, 0, &r.gen(), &once(1).collect());
                }
                let time_spent = timer.elapsed().as_millis();
                println!("    add {nbr_doc} : {time_spent}ms");
                txn.abort().unwrap();
            }
        }
    }
}

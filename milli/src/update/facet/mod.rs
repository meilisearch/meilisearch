/*!
This module implements two different algorithms for updating the `facet_id_string_docids`
and `facet_id_f64_docids` databases. The first algorithm is a "bulk" algorithm, meaning that
it recreates the database from scratch when new elements are added to it. The second algorithm
is incremental: it modifies the database as little as possible.

The databases must be able to return results for queries such as:
1. Filter       : find all the document ids that have a facet value greater than X and/or smaller than Y
2. Min/Max      : find the minimum/maximum facet value among these document ids
3. Sort         : sort these document ids by increasing/decreasing facet values
4. Distribution : given some document ids, make a list of each facet value
   found in these documents along with the number of documents that contain it

The algorithms that implement these queries are found in the `src/search/facet` folder.

To make these queries fast to compute, the database adopts a tree structure:
```text
            ┌───────────────────────────────┬───────────────────────────────┬───────────────┐
┌───────┐   │           "ab" (2)            │           "gaf" (2)           │   "woz" (1)   │
│Level 2│   │                               │                               │               │
└───────┘   │        [a, b, d, f, z]        │        [c, d, e, f, g]        │    [u, y]     │
            ├───────────────┬───────────────┼───────────────┬───────────────┼───────────────┤
┌───────┐   │   "ab" (2)    │   "ba" (2)    │   "gaf" (2)   │  "form" (2)   │   "woz" (2)   │
│Level 1│   │               │               │               │               │               │
└───────┘   │ [a, b, d, z]  │   [a, b, f]   │   [c, d, g]   │    [e, f]     │    [u, y]     │
            ├───────┬───────┼───────┬───────┼───────┬───────┼───────┬───────┼───────┬───────┤
┌───────┐   │  "ab" │  "ac" │  "ba" │ "bac" │ "gaf" │ "gal" │ "form"│ "wow" │ "woz" │  "zz" │
│Level 0│   │       │       │       │       │       │       │       │       │       │       │
└───────┘   │ [a, b]│ [d, z]│ [b, f]│ [a, f]│ [c, d]│  [g]  │  [e]  │ [e, f]│  [y]  │  [u]  │
            └───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘
```
In the diagram above, each cell corresponds to a node in the tree. The first line of the cell
contains the left bound of the range of facet values as well as the number of children of the node.
The second line contains the document ids which have a facet value within the range of the node.
The nodes at level 0 are the leaf nodes. They have 0 children and a single facet value in their range.

In the diagram above, the first cell of level 2 is `ab (2)`. Its range is `ab .. gaf` (because
`gaf` is the left bound of the next node) and it has two children. Its document ids are `[a,b,d,f,z]`.
These documents all contain a facet value that is contained within `ab .. gaf`.

In the database, each node is represented by a key/value pair encoded as a [`FacetGroupKey`] and a
[`FacetGroupValue`], which have the following format:

```text
FacetGroupKey:
- field id  : u16
- level     : u8
- left bound: [u8]    // the facet value encoded using either OrderedF64Codec or Str

FacetGroupValue:
- #children : u8
- docids    : RoaringBitmap
```

When the database is first created using the "bulk" method, each node has a fixed number of children
(except for possibly the last one) given by the `group_size` parameter (default to `FACET_GROUP_SIZE`).
The tree is also built such that the highest level has more than `min_level_size`
(default to `FACET_MIN_LEVEL_SIZE`) elements in it.

When the database is incrementally updated, the number of children of a node can vary between
1 and `max_group_size`. This is done so that most incremental operations do not need to change
the structure of the tree. When the number of children of a node reaches `max_group_size`,
we split the node in two and update the number of children of its parent.

When adding documents to the databases, it is important to determine which method to use to
minimise indexing time. The incremental method is faster when adding few new facet values, but the
bulk method is faster when a large part of the database is modified. Empirically, it seems that
it takes 50x more time to incrementally add N facet values to an existing database than it is to
construct a database of N facet values. This is the heuristic that is used to choose between the
two methods.

Related PR: https://github.com/meilisearch/milli/pull/619
*/

pub const FACET_MAX_GROUP_SIZE: u8 = 8;
pub const FACET_GROUP_SIZE: u8 = 4;
pub const FACET_MIN_LEVEL_SIZE: u8 = 5;

use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufReader;

use grenad::Merger;
use heed::types::{Bytes, DecodeIgnore};
use time::OffsetDateTime;
use tracing::debug;

use self::incremental::FacetsUpdateIncremental;
use super::FacetsUpdateBulk;
use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::heed_codec::BytesRefCodec;
use crate::update::del_add::{DelAdd, KvReaderDelAdd};
use crate::update::MergeFn;
use crate::{try_split_array_at, FieldId, Index, Result};

pub mod bulk;
pub mod incremental;

/// A builder used to add new elements to the `facet_id_string_docids` or `facet_id_f64_docids` databases.
///
/// Depending on the number of new elements and the existing size of the database, we use either
/// a bulk update method or an incremental update method.
pub struct FacetsUpdate<'i> {
    index: &'i Index,
    database: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    facet_type: FacetType,
    delta_data: Merger<BufReader<File>, MergeFn>,
    normalized_delta_data: Option<Merger<BufReader<File>, MergeFn>>,
    group_size: u8,
    max_group_size: u8,
    min_level_size: u8,
    data_size: u64,
}
impl<'i> FacetsUpdate<'i> {
    pub fn new(
        index: &'i Index,
        facet_type: FacetType,
        delta_data: Merger<BufReader<File>, MergeFn>,
        normalized_delta_data: Option<Merger<BufReader<File>, MergeFn>>,
        data_size: u64,
    ) -> Self {
        let database = match facet_type {
            FacetType::String => {
                index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>()
            }
            FacetType::Number => {
                index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>()
            }
        };
        Self {
            index,
            database,
            group_size: FACET_GROUP_SIZE,
            max_group_size: FACET_MAX_GROUP_SIZE,
            min_level_size: FACET_MIN_LEVEL_SIZE,
            facet_type,
            delta_data,
            normalized_delta_data,
            data_size,
        }
    }

    pub fn execute(self, wtxn: &mut heed::RwTxn) -> Result<()> {
        if self.data_size == 0 {
            return Ok(());
        }
        debug!("Computing and writing the facet values levels docids into LMDB on disk...");
        self.index.set_updated_at(wtxn, &OffsetDateTime::now_utc())?;

        // See self::comparison_bench::benchmark_facet_indexing
        if self.data_size >= (self.database.len(wtxn)? / 500) {
            let field_ids =
                self.index.faceted_fields_ids(wtxn)?.iter().copied().collect::<Vec<_>>();
            let bulk_update = FacetsUpdateBulk::new(
                self.index,
                field_ids,
                self.facet_type,
                self.delta_data,
                self.group_size,
                self.min_level_size,
            );
            bulk_update.execute(wtxn)?;
        } else {
            let incremental_update = FacetsUpdateIncremental::new(
                self.index,
                self.facet_type,
                self.delta_data,
                self.group_size,
                self.min_level_size,
                self.max_group_size,
            );
            incremental_update.execute(wtxn)?;
        }

        match self.normalized_delta_data {
            Some(data) => index_facet_search(wtxn, data, self.index),
            None => Ok(()),
        }
    }
}

fn index_facet_search(
    wtxn: &mut heed::RwTxn,
    normalized_delta_data: Merger<BufReader<File>, MergeFn>,
    index: &Index,
) -> Result<()> {
    let mut iter = normalized_delta_data.into_stream_merger_iter()?;
    while let Some((key_bytes, delta_bytes)) = iter.next()? {
        let deladd_reader = KvReaderDelAdd::new(delta_bytes);

        let database_set = index
            .facet_id_normalized_string_strings
            .remap_key_type::<Bytes>()
            .get(wtxn, key_bytes)?
            .unwrap_or_default();

        let add_set = deladd_reader
            .get(DelAdd::Addition)
            .and_then(|bytes| serde_json::from_slice::<BTreeSet<String>>(bytes).ok())
            .unwrap_or_default();

        let del_set = match deladd_reader
            .get(DelAdd::Deletion)
            .and_then(|bytes| serde_json::from_slice::<BTreeSet<String>>(bytes).ok())
        {
            Some(del_set) => {
                let (field_id_bytes, _) = try_split_array_at(key_bytes).unwrap();
                let field_id = FieldId::from_be_bytes(field_id_bytes);
                let mut set = BTreeSet::new();
                for facet in del_set {
                    let key = FacetGroupKey { field_id, level: 0, left_bound: facet.as_str() };
                    // Check if the referenced value doesn't exist anymore before deleting it.
                    if index
                        .facet_id_string_docids
                        .remap_data_type::<DecodeIgnore>()
                        .get(wtxn, &key)?
                        .is_none()
                    {
                        set.insert(facet);
                    }
                }
                set
            }
            None => BTreeSet::new(),
        };

        let set: BTreeSet<_> =
            database_set.difference(&del_set).chain(add_set.iter()).cloned().collect();

        if set.is_empty() {
            index
                .facet_id_normalized_string_strings
                .remap_key_type::<Bytes>()
                .delete(wtxn, key_bytes)?;
        } else {
            index
                .facet_id_normalized_string_strings
                .remap_key_type::<Bytes>()
                .put(wtxn, key_bytes, &set)?;
        }
    }

    // We clear the FST of normalized-for-search to compute everything from scratch.
    index.facet_id_string_fst.clear(wtxn)?;
    // We compute one FST by string facet
    let mut text_fsts = vec![];
    let mut current_fst: Option<(u16, fst::SetBuilder<Vec<u8>>)> = None;
    let database = index.facet_id_normalized_string_strings.remap_data_type::<DecodeIgnore>();
    for result in database.iter(wtxn)? {
        let ((field_id, normalized_facet), _) = result?;
        current_fst = match current_fst.take() {
            Some((fid, fst_builder)) if fid != field_id => {
                let fst = fst_builder.into_set();
                text_fsts.push((fid, fst));
                Some((field_id, fst::SetBuilder::memory()))
            }
            Some((field_id, fst_builder)) => Some((field_id, fst_builder)),
            None => Some((field_id, fst::SetBuilder::memory())),
        };

        if let Some((_, fst_builder)) = current_fst.as_mut() {
            fst_builder.insert(normalized_facet)?;
        }
    }

    if let Some((field_id, fst_builder)) = current_fst {
        let fst = fst_builder.into_set();
        text_fsts.push((field_id, fst));
    }

    // We write those FSTs in LMDB now
    for (field_id, fst) in text_fsts {
        index.facet_id_string_fst.put(wtxn, &field_id, &fst)?;
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use std::cell::Cell;
    use std::fmt::Display;
    use std::iter::FromIterator;
    use std::marker::PhantomData;
    use std::rc::Rc;

    use grenad::MergerBuilder;
    use heed::types::Bytes;
    use heed::{BytesDecode, BytesEncode, Env, RoTxn, RwTxn};
    use roaring::RoaringBitmap;

    use super::bulk::FacetsUpdateBulkInner;
    use crate::heed_codec::facet::{
        FacetGroupKey, FacetGroupKeyCodec, FacetGroupValue, FacetGroupValueCodec,
    };
    use crate::heed_codec::BytesRefCodec;
    use crate::search::facet::get_highest_level;
    use crate::snapshot_tests::display_bitmap;
    use crate::update::del_add::{DelAdd, KvWriterDelAdd};
    use crate::update::index_documents::merge_deladd_cbo_roaring_bitmaps;
    use crate::update::{FacetsUpdateIncrementalInner, MergeFn};
    use crate::CboRoaringBitmapCodec;

    /// Utility function to generate a string whose position in a lexicographically
    /// ordered list is `i`.
    pub fn ordered_string(mut i: usize) -> String {
        // The first string is empty
        if i == 0 {
            return String::new();
        }
        // The others are 5 char long, each between 'a' and 'z'
        let mut s = String::new();
        for _ in 0..5 {
            let (digit, next) = (i % 26, i / 26);
            s.insert(0, char::from_u32('a' as u32 + digit as u32).unwrap());
            i = next;
        }
        s
    }

    /// A dummy index that only contains the facet database, used for testing
    pub struct FacetIndex<BoundCodec>
    where
        for<'a> BoundCodec:
            BytesEncode<'a> + BytesDecode<'a, DItem = <BoundCodec as BytesEncode<'a>>::EItem>,
    {
        pub env: Env,
        pub content: heed::Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
        pub group_size: Cell<u8>,
        pub min_level_size: Cell<u8>,
        pub max_group_size: Cell<u8>,
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
            let group_size = std::cmp::min(16, std::cmp::max(group_size, 2)); // 2 <= x <= 16
            let max_group_size = std::cmp::min(16, std::cmp::max(group_size * 2, max_group_size)); // 2*group_size <= x <= 16
            let min_level_size = std::cmp::min(17, std::cmp::max(1, min_level_size)); // 1 <= x <= 17

            let mut options = heed::EnvOpenOptions::new();
            let options = options.map_size(4096 * 4 * 10 * 1000);
            unsafe {
                options.flag(heed::flags::Flags::MdbAlwaysFreePages);
            }
            let env = options.open(tempdir.path()).unwrap();
            let content = env.open_database(None).unwrap().unwrap();

            FacetIndex {
                content,
                group_size: Cell::new(group_size),
                max_group_size: Cell::new(max_group_size),
                min_level_size: Cell::new(min_level_size),
                _tempdir: tempdir,
                env,
                _phantom: PhantomData,
            }
        }
        pub fn new(
            group_size: u8,
            max_group_size: u8,
            min_level_size: u8,
        ) -> FacetIndex<BoundCodec> {
            let group_size = group_size.clamp(2, 127);
            let max_group_size = std::cmp::min(127, std::cmp::max(group_size * 2, max_group_size)); // 2*group_size <= x <= 127
            let min_level_size = std::cmp::max(1, min_level_size); // 1 <= x <= inf
            let mut options = heed::EnvOpenOptions::new();
            let options = options.map_size(4096 * 4 * 1000 * 100);
            let tempdir = tempfile::TempDir::new().unwrap();
            let env = options.open(tempdir.path()).unwrap();
            let mut wtxn = env.write_txn().unwrap();
            let content = env.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().unwrap();

            FacetIndex {
                content,
                group_size: Cell::new(group_size),
                max_group_size: Cell::new(max_group_size),
                min_level_size: Cell::new(min_level_size),
                _tempdir: Rc::new(tempdir),
                env,
                _phantom: PhantomData,
            }
        }

        #[cfg(all(test, fuzzing))]
        pub fn set_group_size(&self, group_size: u8) {
            // 2 <= x <= 64
            self.group_size.set(std::cmp::min(64, std::cmp::max(group_size, 2)));
        }
        #[cfg(all(test, fuzzing))]
        pub fn set_max_group_size(&self, max_group_size: u8) {
            // 2*group_size <= x <= 128
            let max_group_size = std::cmp::max(4, std::cmp::min(128, max_group_size));
            self.max_group_size.set(max_group_size);
            if self.group_size.get() < max_group_size / 2 {
                self.group_size.set(max_group_size / 2);
            }
        }
        #[cfg(all(test, fuzzing))]
        pub fn set_min_level_size(&self, min_level_size: u8) {
            // 1 <= x <= inf
            self.min_level_size.set(std::cmp::max(1, min_level_size));
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
                group_size: self.group_size.get(),
                min_level_size: self.min_level_size.get(),
                max_group_size: self.max_group_size.get(),
            };
            let key_bytes = BoundCodec::bytes_encode(key).unwrap();
            update.modify(wtxn, field_id, &key_bytes, Some(docids), None).unwrap();
            update.add_or_delete_level(wtxn, field_id).unwrap();
        }
        pub fn delete_single_docid<'a>(
            &self,
            wtxn: &'a mut RwTxn,
            field_id: u16,
            key: &'a <BoundCodec as BytesEncode<'a>>::EItem,
            docid: u32,
        ) {
            self.delete(wtxn, field_id, key, &RoaringBitmap::from_iter(std::iter::once(docid)))
        }

        pub fn delete<'a>(
            &self,
            wtxn: &'a mut RwTxn,
            field_id: u16,
            key: &'a <BoundCodec as BytesEncode<'a>>::EItem,
            docids: &RoaringBitmap,
        ) {
            let update = FacetsUpdateIncrementalInner {
                db: self.content,
                group_size: self.group_size.get(),
                min_level_size: self.min_level_size.get(),
                max_group_size: self.max_group_size.get(),
            };
            let key_bytes = BoundCodec::bytes_encode(key).unwrap();
            update.modify(wtxn, field_id, &key_bytes, None, Some(docids)).unwrap();
            update.add_or_delete_level(wtxn, field_id).unwrap();
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
                let key = FacetGroupKeyCodec::<BytesRefCodec>::bytes_encode(&key).unwrap();
                let mut inner_writer = KvWriterDelAdd::memory();
                let value = CboRoaringBitmapCodec::bytes_encode(docids).unwrap();
                inner_writer.insert(DelAdd::Addition, value).unwrap();
                writer.insert(&key, inner_writer.into_inner().unwrap()).unwrap();
            }
            writer.finish().unwrap();
            let reader = grenad::Reader::new(std::io::Cursor::new(new_data)).unwrap();
            let mut builder = MergerBuilder::new(merge_deladd_cbo_roaring_bitmaps as MergeFn);
            builder.push(reader.into_cursor().unwrap());
            let merger = builder.build();

            let update = FacetsUpdateBulkInner {
                db: self.content,
                delta_data: Some(merger),
                group_size: self.group_size.get(),
                min_level_size: self.min_level_size.get(),
            };

            update.update(wtxn, field_ids).unwrap();
        }

        pub fn verify_structure_validity(&self, txn: &RoTxn, field_id: u16) {
            let mut field_id_prefix = vec![];
            field_id_prefix.extend_from_slice(&field_id.to_be_bytes());

            let highest_level = get_highest_level(txn, self.content, field_id).unwrap();

            for level_no in (1..=highest_level).rev() {
                let mut level_no_prefix = vec![];
                level_no_prefix.extend_from_slice(&field_id.to_be_bytes());
                level_no_prefix.push(level_no);

                let iter = self
                    .content
                    .remap_types::<Bytes, FacetGroupValueCodec>()
                    .prefix_iter(txn, &level_no_prefix)
                    .unwrap();
                for el in iter {
                    let (key, value) = el.unwrap();
                    let key = FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(key).unwrap();

                    let mut prefix_start_below = vec![];
                    prefix_start_below.extend_from_slice(&field_id.to_be_bytes());
                    prefix_start_below.push(level_no - 1);
                    prefix_start_below.extend_from_slice(key.left_bound);

                    let start_below = {
                        let mut start_below_iter = self
                            .content
                            .remap_types::<Bytes, FacetGroupValueCodec>()
                            .prefix_iter(txn, &prefix_start_below)
                            .unwrap();
                        let (key_bytes, _) = start_below_iter.next().unwrap().unwrap();
                        FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(key_bytes).unwrap()
                    };

                    assert!(value.size > 0);

                    let mut actual_size = 0;
                    let mut values_below = RoaringBitmap::new();
                    let iter_below = self
                        .content
                        .range(txn, &(start_below..))
                        .unwrap()
                        .take(value.size as usize);
                    for el in iter_below {
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
            let iter = self.content.iter(&txn).unwrap();
            for el in iter {
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

    use super::test_helpers::FacetIndex;
    use crate::heed_codec::facet::OrderedF64Codec;

    // This is a simple test to get an intuition on the relative speed
    // of the incremental vs. bulk indexer.
    //
    // The benchmark shows the worst-case scenario for the incremental indexer, since
    // each facet value contains only one document ID.
    //
    // In that scenario, it appears that the incremental indexer is about 50 times slower than the
    // bulk indexer.
    // #[test]
    fn benchmark_facet_indexing() {
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
                txn.abort();
            }
        }
    }
}

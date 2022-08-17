/*!
This module initialises the databases that are used to quickly get the list
of documents with a faceted field value falling within a certain range. For
example, they can be used to implement filters such as `x >= 3`.

These databases are `facet_id_string_docids` and `facet_id_f64_docids`.

## Example with numbers

In the case of numbers, we start with a sorted list whose keys are
`(field_id, number_value)` and whose value is a roaring bitmap of the document ids
which contain the value `number_value` for the faceted field `field_id`.

From this list, we want to compute two things:

1. the bitmap of all documents that contain **any** number for each faceted field
2. a structure that allows us to use a (sort of) binary search to find all documents
containing numbers inside a certain range for a faceted field

To achieve goal (2), we recursively split the list into chunks. Every time we split it, we
create a new "level" that is several times smaller than the level below it. The base level,
level 0, is the starting list. Level 1 is composed of chunks of up to N elements. Each element
contains a range and a bitmap of docids. Level 2 is composed of chunks up to N^2 elements, etc.

For example, let's say we have 26 documents which we identify through the letters a-z.
We will focus on a single faceted field. When there are multiple faceted fields, the structure
described below is simply repeated for each field.

What we want to obtain is the following structure for each faceted field:
```text
┌───────┐   ┌───────────────────────────────────────────────────────────────────────────────┐
│  all  │   │                       [a, b, c, d, e, f, g, u, y, z]                          │
└───────┘   └───────────────────────────────────────────────────────────────────────────────┘
            ┌───────────────────────────────┬───────────────────────────────┬───────────────┐
┌───────┐   │            1.2 – 2            │           3.4 – 100           │   102 – 104   │
│Level 2│   │                               │                               │               │
└───────┘   │        [a, b, d, f, z]        │        [c, d, e, f, g]        │    [u, y]     │
            ├───────────────┬───────────────┼───────────────┬───────────────┼───────────────┤
┌───────┐   │   1.2 – 1.3   │    1.6 – 2    │   3.4 – 12    │  12.3 – 100   │   102 – 104   │
│Level 1│   │               │               │               │               │               │
└───────┘   │ [a, b, d, z]  │   [a, b, f]   │   [c, d, g]   │    [e, f]     │    [u, y]     │
            ├───────┬───────┼───────┬───────┼───────┬───────┼───────┬───────┼───────┬───────┤
┌───────┐   │  1.2  │  1.3  │  1.6  │   2   │  3.4  │   12  │  12.3 │  100  │  102  │  104  │
│Level 0│   │       │       │       │       │       │       │       │       │       │       │
└───────┘   │ [a, b]│ [d, z]│ [b, f]│ [a, f]│ [c, d]│  [g]  │  [e]  │ [e, f]│  [y]  │  [u]  │
            └───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘
```

You can read more about this structure (for strings) in `[crate::search::facet::facet_strings]`.

To create the levels, we use a recursive algorithm which makes sure that we only need to iterate
over the elements of level 0 once. It is implemented by [`recursive_compute_levels`].

## Encoding

### Numbers
For numbers we use the same encoding for level 0 and the other levels.

The key is given by `FacetLevelValueF64Codec`. It consists of:
1. The field id            : u16
2. The height of the level : u8
3. The start bound         : f64
4. The end bound           : f64
Note that at level 0, we have start bound == end bound.

The value is a serialised `RoaringBitmap`.

### Strings

For strings, we use a different encoding for level 0 and the other levels.

At level 0, the key is given by `FacetStringLevelZeroCodec`. It consists of:
1. The field id                : u16
2. The height of the level     : u8  <-- always == 0
3. The normalised string value : &str

And the value is given by `FacetStringLevelZeroValueCodec`. It consists of:
1. The original string
2. A serialised `RoaringBitmap`

At level 1, the key is given by `FacetLevelValueU32Codec`. It consists of:
1. The field id                : u16
2. The height of the level     : u8  <-- always >= 1
3. The start bound             : u32
4. The end bound               : u32
where the bounds are indices inside level 0.

The value is given by `FacetStringZeroBoundsValueCodec<CboRoaringBitmapCodec>`.
If the level is 1, then it consists of:
1. The normalised string of the start bound
2. The normalised string of the end bound
3. A serialised `RoaringBitmap`

If the level is higher, then it consists only of the serialised roaring bitmap.

The distinction between the value encoding of level 1 and the levels above it
is to allow us to retrieve the value in level 0 quickly by reading the key of
level 1 (we obtain the string value of the bound and execute a prefix search
in the database).

Therefore, for strings, the structure for a single faceted field looks more like this:
```text
┌───────┐   ┌───────────────────────────────────────────────────────────────────────────────┐
│  all  │   │                       [a, b, c, d, e, f, g, u, y, z]                          │
└───────┘   └───────────────────────────────────────────────────────────────────────────────┘

            ┌───────────────────────────────┬───────────────────────────────┬───────────────┐
┌───────┐   │             0 – 3             │             4 – 7             │     8 – 9     │
│Level 2│   │                               │                               │               │
└───────┘   │        [a, b, d, f, z]        │        [c, d, e, f, g]        │    [u, y]     │
            ├───────────────┬───────────────┼───────────────┬───────────────┼───────────────┤
┌───────┐   │     0 – 1     │     2 – 3     │     4 – 5     │     6 – 7     │     8 – 9     │
│Level 1│   │  "ab" – "ac"  │ "ba" – "bac"  │ "gaf" – "gal" │"form" – "wow" │ "woz" – "zz"  │
└───────┘   │ [a, b, d, z]  │   [a, b, f]   │   [c, d, g]   │    [e, f]     │    [u, y]     │
            ├───────┬───────┼───────┬───────┼───────┬───────┼───────┬───────┼───────┬───────┤
┌───────┐   │  "ab" │  "ac" │  "ba" │ "bac" │ "gaf" │ "gal" │ "form"│ "wow" │ "woz" │  "zz" │
│Level 0│   │  "AB" │ " Ac" │ "ba " │ "Bac" │ " GAF"│ "gal" │ "Form"│ " wow"│ "woz" │  "ZZ" │
└───────┘   │ [a, b]│ [d, z]│ [b, f]│ [a, f]│ [c, d]│  [g]  │  [e]  │ [e, f]│  [y]  │  [u]  │
            └───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘

The first line in a cell is its key (without the field id and level height) and the last two
lines are its values.
```
*/

use std::cmp;
use std::fs::File;
use std::num::{NonZeroU8, NonZeroUsize};
use std::ops::RangeFrom;

use grenad::{CompressionType, Reader, Writer};
use heed::types::{ByteSlice, DecodeIgnore};
use heed::{BytesDecode, BytesEncode, Error};
use log::debug;
use roaring::RoaringBitmap;
use time::OffsetDateTime;

use crate::error::InternalError;
use crate::heed_codec::facet::{
    FacetLevelValueF64Codec, FacetLevelValueU32Codec, FacetStringLevelZeroCodec,
    FacetStringLevelZeroValueCodec, FacetStringZeroBoundsValueCodec,
};
use crate::heed_codec::CboRoaringBitmapCodec;
use crate::update::index_documents::{create_writer, write_into_lmdb_database, writer_into_reader};
use crate::{FieldId, Index, Result};

pub struct Facets<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
}

impl<'t, 'u, 'i> Facets<'t, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'i, 'u>, index: &'i Index) -> Facets<'t, 'u, 'i> {
        Facets {
            wtxn,
            index,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            level_group_size: NonZeroUsize::new(4).unwrap(),
            min_level_size: NonZeroUsize::new(5).unwrap(),
        }
    }

    /// The number of elements from the level below that are represented by a single element in the level above
    ///
    /// This setting is always greater than or equal to 2.
    pub fn level_group_size(&mut self, value: NonZeroUsize) -> &mut Self {
        self.level_group_size = NonZeroUsize::new(cmp::max(value.get(), 2)).unwrap();
        self
    }

    /// The minimum number of elements that a level is allowed to have.
    pub fn min_level_size(&mut self, value: NonZeroUsize) -> &mut Self {
        self.min_level_size = value;
        self
    }

    #[logging_timer::time("Facets::{}")]
    pub fn execute(self) -> Result<()> {
        self.index.set_updated_at(self.wtxn, &OffsetDateTime::now_utc())?;
        // We get the faceted fields to be able to create the facet levels.
        let faceted_fields = self.index.faceted_fields_ids(self.wtxn)?;

        debug!("Computing and writing the facet values levels docids into LMDB on disk...");

        for field_id in faceted_fields {
            // Clear the facet string levels.
            clear_field_string_levels(
                self.wtxn,
                self.index.facet_id_string_docids.remap_types::<ByteSlice, DecodeIgnore>(),
                field_id,
            )?;

            let (facet_string_levels, string_documents_ids) = compute_facet_strings_levels(
                self.wtxn,
                self.index.facet_id_string_docids,
                self.chunk_compression_type,
                self.chunk_compression_level,
                self.level_group_size,
                self.min_level_size,
                field_id,
            )?;

            self.index.put_string_faceted_documents_ids(
                self.wtxn,
                field_id,
                &string_documents_ids,
            )?;
            for facet_strings_level in facet_string_levels {
                write_into_lmdb_database(
                    self.wtxn,
                    *self.index.facet_id_string_docids.as_polymorph(),
                    facet_strings_level,
                    |_, _| {
                        Err(InternalError::IndexingMergingKeys { process: "facet string levels" })?
                    },
                )?;
            }

            // Clear the facet number levels.
            clear_field_number_levels(self.wtxn, self.index.facet_id_f64_docids, field_id)?;

            let (facet_number_levels, number_documents_ids) = compute_facet_number_levels(
                self.wtxn,
                self.index.facet_id_f64_docids,
                self.chunk_compression_type,
                self.chunk_compression_level,
                self.level_group_size,
                self.min_level_size,
                field_id,
            )?;

            self.index.put_number_faceted_documents_ids(
                self.wtxn,
                field_id,
                &number_documents_ids,
            )?;

            for facet_number_level in facet_number_levels {
                write_into_lmdb_database(
                    self.wtxn,
                    *self.index.facet_id_f64_docids.as_polymorph(),
                    facet_number_level,
                    |_, _| {
                        Err(InternalError::IndexingMergingKeys { process: "facet number levels" })?
                    },
                )?;
            }
        }

        Ok(())
    }
}

/// Compute the content of the database levels from its level 0 for the given field id.
///
/// ## Returns:
/// 1. a vector of grenad::Reader. The reader at index `i` corresponds to the elements of level `i + 1`
/// that must be inserted into the database.
/// 2. a roaring bitmap of all the document ids present in the database
fn compute_facet_number_levels<'t>(
    rtxn: &'t heed::RoTxn,
    db: heed::Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
    compression_type: CompressionType,
    compression_level: Option<u32>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
    field_id: FieldId,
) -> Result<(Vec<Reader<File>>, RoaringBitmap)> {
    let first_level_size = db
        .remap_key_type::<ByteSlice>()
        .prefix_iter(rtxn, &field_id.to_be_bytes())?
        .remap_types::<DecodeIgnore, DecodeIgnore>()
        .fold(Ok(0usize), |count, result| result.and(count).map(|c| c + 1))?;

    let level_0_start = (field_id, 0, f64::MIN, f64::MIN);

    // Groups sizes are always a power of the original level_group_size and therefore a group
    // always maps groups of the previous level and never splits previous levels groups in half.
    let group_size_iter = (1u8..)
        .map(|l| (l, level_group_size.get().pow(l as u32)))
        .take_while(|(_, s)| first_level_size / *s >= min_level_size.get())
        .collect::<Vec<_>>();

    let mut number_document_ids = RoaringBitmap::new();

    if let Some((top_level, _)) = group_size_iter.last() {
        let subwriters =
            recursive_compute_levels::<FacetLevelValueF64Codec, CboRoaringBitmapCodec, f64>(
                rtxn,
                db,
                compression_type,
                compression_level,
                *top_level,
                level_0_start,
                &(level_0_start..),
                first_level_size,
                level_group_size,
                &mut |bitmaps, _, _| {
                    for bitmap in bitmaps {
                        number_document_ids |= bitmap;
                    }
                    Ok(())
                },
                &|_i, (_field_id, _level, left, _right)| *left,
                &|bitmap| bitmap,
                &|writer, level, left, right, docids| {
                    write_number_entry(writer, field_id, level.get(), left, right, &docids)?;
                    Ok(())
                },
            )?;

        Ok((subwriters, number_document_ids))
    } else {
        let mut documents_ids = RoaringBitmap::new();
        for result in db.range(rtxn, &(level_0_start..))?.take(first_level_size) {
            let (_key, docids) = result?;
            documents_ids |= docids;
        }

        Ok((vec![], documents_ids))
    }
}

/// Compute the content of the database levels from its level 0 for the given field id.
///
/// ## Returns:
/// 1. a vector of grenad::Reader. The reader at index `i` corresponds to the elements of level `i + 1`
/// that must be inserted into the database.
/// 2. a roaring bitmap of all the document ids present in the database
fn compute_facet_strings_levels<'t>(
    rtxn: &'t heed::RoTxn,
    db: heed::Database<FacetStringLevelZeroCodec, FacetStringLevelZeroValueCodec>,
    compression_type: CompressionType,
    compression_level: Option<u32>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
    field_id: FieldId,
) -> Result<(Vec<Reader<File>>, RoaringBitmap)> {
    let first_level_size = db
        .remap_key_type::<ByteSlice>()
        .prefix_iter(rtxn, &field_id.to_be_bytes())?
        .remap_types::<DecodeIgnore, DecodeIgnore>()
        .fold(Ok(0usize), |count, result| result.and(count).map(|c| c + 1))?;

    let level_0_start = (field_id, "");

    // Groups sizes are always a power of the original level_group_size and therefore a group
    // always maps groups of the previous level and never splits previous levels groups in half.
    let group_size_iter = (1u8..)
        .map(|l| (l, level_group_size.get().pow(l as u32)))
        .take_while(|(_, s)| first_level_size / *s >= min_level_size.get())
        .collect::<Vec<_>>();

    let mut strings_document_ids = RoaringBitmap::new();

    if let Some((top_level, _)) = group_size_iter.last() {
        let subwriters = recursive_compute_levels::<
            FacetStringLevelZeroCodec,
            FacetStringLevelZeroValueCodec,
            (u32, &str),
        >(
            rtxn,
            db,
            compression_type,
            compression_level,
            *top_level,
            level_0_start,
            &(level_0_start..),
            first_level_size,
            level_group_size,
            &mut |bitmaps, _, _| {
                for bitmap in bitmaps {
                    strings_document_ids |= bitmap;
                }
                Ok(())
            },
            &|i, (_field_id, value)| (i as u32, *value),
            &|value| value.1,
            &|writer, level, start_bound, end_bound, docids| {
                write_string_entry(writer, field_id, level, start_bound, end_bound, docids)?;
                Ok(())
            },
        )?;

        Ok((subwriters, strings_document_ids))
    } else {
        let mut documents_ids = RoaringBitmap::new();
        for result in db.range(rtxn, &(level_0_start..))?.take(first_level_size) {
            let (_key, (_original_value, docids)) = result?;
            documents_ids |= docids;
        }

        Ok((vec![], documents_ids))
    }
}

/**
Compute a level from the levels below it, with the elements of level 0 already existing in the given `db`.

This function is generic to work with both numbers and strings. The generic type parameters are:
* `KeyCodec`/`ValueCodec`: the codecs used to read the elements of the database.
* `Bound`: part of the range in the levels structure. For example, for numbers, the `Bound` is `f64`
because each chunk in a level contains a range such as (1.2 ..= 4.5).

## Arguments
* `rtxn` : LMDB read transaction
* `db`: a database which already contains a `level 0`
* `compression_type`/`compression_level`: parameters used to create the `grenad::Writer` that
will contain the new levels
* `level` : the height of the level to create, or `0` to read elements from level 0.
* `level_0_start` : a key in the database that points to the beginning of its level 0
* `level_0_range` : equivalent to `level_0_start..`
* `level_0_size` : the number of elements in level 0
* `level_group_size` : the number of elements from the level below that are represented by a
single element of the new level
* `computed_group_bitmap` : a callback that is called whenever at most `level_group_size` elements
from the level below were read/created. Its arguments are:
    0. the list of bitmaps from each read/created element of the level below
    1. the start bound corresponding to the first element
    2. the end bound corresponding to the last element
* `bound_from_db_key` : finds the `Bound` from a key in the database
* `bitmap_from_db_value` : finds the `RoaringBitmap` from a value in the database
* `write_entry` : writes an element of a level into the writer. The arguments are:
    0. the writer
    1. the height of the level
    2. the start bound
    3. the end bound
    4. the docids of all elements between the start and end bound

## Return
A vector of grenad::Reader. The reader at index `i` corresponds to the elements of level `i + 1`
that must be inserted into the database.
*/
fn recursive_compute_levels<'t, KeyCodec, ValueCodec, Bound>(
    rtxn: &'t heed::RoTxn,
    db: heed::Database<KeyCodec, ValueCodec>,
    compression_type: CompressionType,
    compression_level: Option<u32>,
    level: u8,
    level_0_start: <KeyCodec as BytesDecode<'t>>::DItem,
    level_0_range: &'t RangeFrom<<KeyCodec as BytesDecode<'t>>::DItem>,
    level_0_size: usize,
    level_group_size: NonZeroUsize,
    computed_group_bitmap: &mut dyn FnMut(&[RoaringBitmap], Bound, Bound) -> Result<()>,
    bound_from_db_key: &dyn for<'a> Fn(usize, &'a <KeyCodec as BytesDecode<'t>>::DItem) -> Bound,
    bitmap_from_db_value: &dyn Fn(<ValueCodec as BytesDecode<'t>>::DItem) -> RoaringBitmap,
    write_entry: &dyn Fn(&mut Writer<File>, NonZeroU8, Bound, Bound, RoaringBitmap) -> Result<()>,
) -> Result<Vec<Reader<File>>>
where
    KeyCodec: for<'a> BytesEncode<'a>
        + for<'a> BytesDecode<'a, DItem = <KeyCodec as BytesEncode<'a>>::EItem>,
    for<'a> <KeyCodec as BytesEncode<'a>>::EItem: Sized,
    ValueCodec: for<'a> BytesEncode<'a>
        + for<'a> BytesDecode<'a, DItem = <ValueCodec as BytesEncode<'a>>::EItem>,
    for<'a> <ValueCodec as BytesEncode<'a>>::EItem: Sized,
    Bound: Copy,
{
    if level == 0 {
        // base case for the recursion

        // we read the elements one by one and
        // 1. keep track of the start and end bounds
        // 2. fill the `bitmaps` vector to give it to level 1 once `level_group_size` elements were read
        let mut bitmaps = vec![];

        let mut start_bound = bound_from_db_key(0, &level_0_start);
        let mut end_bound = bound_from_db_key(0, &level_0_start);
        let mut first_iteration_for_new_group = true;
        for (i, db_result_item) in db.range(rtxn, level_0_range)?.take(level_0_size).enumerate() {
            let (key, value) = db_result_item?;

            let bound = bound_from_db_key(i, &key);
            let docids = bitmap_from_db_value(value);

            if first_iteration_for_new_group {
                start_bound = bound;
                first_iteration_for_new_group = false;
            }
            end_bound = bound;
            bitmaps.push(docids);

            if bitmaps.len() == level_group_size.get() {
                computed_group_bitmap(&bitmaps, start_bound, end_bound)?;
                first_iteration_for_new_group = true;
                bitmaps.clear();
            }
        }
        // don't forget to give the leftover bitmaps as well
        if !bitmaps.is_empty() {
            computed_group_bitmap(&bitmaps, start_bound, end_bound)?;
            bitmaps.clear();
        }
        // level 0 is already stored in the DB
        return Ok(vec![]);
    } else {
        // level >= 1
        // we compute each element of this level based on the elements of the level below it
        // once we have computed `level_group_size` elements, we give the start and end bounds
        // of those elements, and their bitmaps, to the level above

        let mut cur_writer =
            create_writer(compression_type, compression_level, tempfile::tempfile()?);

        let mut range_for_bitmaps = vec![];
        let mut bitmaps = vec![];

        // compute the levels below
        // in the callback, we fill `cur_writer` with the correct elements for this level
        let mut sub_writers = recursive_compute_levels(
            rtxn,
            db,
            compression_type,
            compression_level,
            level - 1,
            level_0_start,
            level_0_range,
            level_0_size,
            level_group_size,
            &mut |sub_bitmaps: &[RoaringBitmap], start_range, end_range| {
                let mut combined_bitmap = RoaringBitmap::default();
                for bitmap in sub_bitmaps {
                    combined_bitmap |= bitmap;
                }
                range_for_bitmaps.push((start_range, end_range));

                bitmaps.push(combined_bitmap);
                if bitmaps.len() == level_group_size.get() {
                    let start_bound = range_for_bitmaps.first().unwrap().0;
                    let end_bound = range_for_bitmaps.last().unwrap().1;
                    computed_group_bitmap(&bitmaps, start_bound, end_bound)?;
                    for (bitmap, (start_bound, end_bound)) in
                        bitmaps.drain(..).zip(range_for_bitmaps.drain(..))
                    {
                        write_entry(
                            &mut cur_writer,
                            NonZeroU8::new(level).unwrap(),
                            start_bound,
                            end_bound,
                            bitmap,
                        )?;
                    }
                }
                Ok(())
            },
            bound_from_db_key,
            bitmap_from_db_value,
            write_entry,
        )?;
        // don't forget to insert the leftover elements into the writer as well
        if !bitmaps.is_empty() {
            let start_range = range_for_bitmaps.first().unwrap().0;
            let end_range = range_for_bitmaps.last().unwrap().1;
            computed_group_bitmap(&bitmaps, start_range, end_range)?;
            for (bitmap, (left, right)) in bitmaps.drain(..).zip(range_for_bitmaps.drain(..)) {
                write_entry(&mut cur_writer, NonZeroU8::new(level).unwrap(), left, right, bitmap)?;
            }
        }

        sub_writers.push(writer_into_reader(cur_writer)?);
        return Ok(sub_writers);
    }
}

fn clear_field_number_levels<'t>(
    wtxn: &'t mut heed::RwTxn,
    db: heed::Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
    field_id: FieldId,
) -> heed::Result<()> {
    let left = (field_id, 1, f64::MIN, f64::MIN);
    let right = (field_id, u8::MAX, f64::MAX, f64::MAX);
    let range = left..=right;
    db.delete_range(wtxn, &range).map(drop)
}

fn clear_field_string_levels<'t>(
    wtxn: &'t mut heed::RwTxn,
    db: heed::Database<ByteSlice, DecodeIgnore>,
    field_id: FieldId,
) -> heed::Result<()> {
    let left = (field_id, NonZeroU8::new(1).unwrap(), u32::MIN, u32::MIN);
    let right = (field_id, NonZeroU8::new(u8::MAX).unwrap(), u32::MAX, u32::MAX);
    let range = left..=right;
    db.remap_key_type::<FacetLevelValueU32Codec>().delete_range(wtxn, &range).map(drop)
}

fn write_number_entry(
    writer: &mut Writer<File>,
    field_id: FieldId,
    level: u8,
    left: f64,
    right: f64,
    ids: &RoaringBitmap,
) -> Result<()> {
    let key = (field_id, level, left, right);
    let key = FacetLevelValueF64Codec::bytes_encode(&key).ok_or(Error::Encoding)?;
    let data = CboRoaringBitmapCodec::bytes_encode(&ids).ok_or(Error::Encoding)?;
    writer.insert(&key, &data)?;
    Ok(())
}
fn write_string_entry(
    writer: &mut Writer<File>,
    field_id: FieldId,
    level: NonZeroU8,
    (left_id, left_value): (u32, &str),
    (right_id, right_value): (u32, &str),
    docids: RoaringBitmap,
) -> Result<()> {
    let key = (field_id, level, left_id, right_id);
    let key = FacetLevelValueU32Codec::bytes_encode(&key).ok_or(Error::Encoding)?;
    let data = match level.get() {
        1 => (Some((left_value, right_value)), docids),
        _ => (None, docids),
    };
    let data = FacetStringZeroBoundsValueCodec::<CboRoaringBitmapCodec>::bytes_encode(&data)
        .ok_or(Error::Encoding)?;
    writer.insert(&key, &data)?;
    Ok(())
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

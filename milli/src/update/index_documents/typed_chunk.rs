use std::fs::File;

use heed::types::ByteSlice;
use heed::{BytesDecode, RwTxn};
use roaring::RoaringBitmap;

use super::helpers::{
    roaring_bitmap_from_u32s_array, serialize_roaring_bitmap, valid_lmdb_key, CursorClonableMmap,
};
use crate::heed_codec::facet::{decode_prefix_string, encode_prefix_string};
use crate::update::index_documents::helpers::into_clonable_grenad;
use crate::{BoRoaringBitmapCodec, CboRoaringBitmapCodec, Index, Result};

pub(crate) enum TypedChunk {
    DocidWordPositions(grenad::Reader<CursorClonableMmap>),
    FieldIdDocidFacetStrings(grenad::Reader<CursorClonableMmap>),
    FieldIdDocidFacetNumbers(grenad::Reader<CursorClonableMmap>),
    Documents(grenad::Reader<CursorClonableMmap>),
    FieldIdWordcountDocids(grenad::Reader<File>),
    NewDocumentsIds(RoaringBitmap),
    WordDocids(grenad::Reader<File>),
    WordLevelPositionDocids(grenad::Reader<File>),
    WordPairProximityDocids(grenad::Reader<File>),
    FieldIdFacetStringDocids(grenad::Reader<File>),
    FieldIdFacetNumberDocids(grenad::Reader<File>),
}

/// Write typed chunk in the corresponding LMDB database of the provided index.
/// Return new documents seen.
pub(crate) fn write_typed_chunk_into_index(
    typed_chunk: TypedChunk,
    index: &Index,
    wtxn: &mut RwTxn,
    index_is_empty: bool,
) -> Result<(RoaringBitmap, bool)> {
    let mut is_merged_database = false;
    match typed_chunk {
        TypedChunk::DocidWordPositions(docid_word_positions_iter) => {
            write_entries_into_database(
                docid_word_positions_iter,
                &index.docid_word_positions,
                wtxn,
                index_is_empty,
                |value, buffer| {
                    // ensure that values are unique and ordered
                    let positions = roaring_bitmap_from_u32s_array(value);
                    BoRoaringBitmapCodec::serialize_into(&positions, buffer);
                    Ok(buffer)
                },
                |new_values, db_values, buffer| {
                    let new_values = roaring_bitmap_from_u32s_array(new_values);
                    let positions = match BoRoaringBitmapCodec::bytes_decode(db_values) {
                        Some(db_values) => new_values | db_values,
                        None => new_values, // should not happen
                    };
                    BoRoaringBitmapCodec::serialize_into(&positions, buffer);
                    Ok(())
                },
            )?;
        }
        TypedChunk::Documents(mut obkv_documents_iter) => {
            while let Some((key, value)) = obkv_documents_iter.next()? {
                index.documents.remap_types::<ByteSlice, ByteSlice>().put(wtxn, key, value)?;
            }
        }
        TypedChunk::FieldIdWordcountDocids(fid_word_count_docids_iter) => {
            append_entries_into_database(
                fid_word_count_docids_iter,
                &index.field_id_word_count_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                merge_cbo_roaring_bitmaps,
            )?;
            is_merged_database = true;
        }
        TypedChunk::NewDocumentsIds(documents_ids) => {
            return Ok((documents_ids, is_merged_database))
        }
        TypedChunk::WordDocids(word_docids_iter) => {
            let mut word_docids_iter = unsafe { into_clonable_grenad(word_docids_iter) }?;
            append_entries_into_database(
                word_docids_iter.clone(),
                &index.word_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                merge_roaring_bitmaps,
            )?;

            // create fst from word docids
            let mut builder = fst::SetBuilder::memory();
            while let Some((word, _value)) = word_docids_iter.next()? {
                // This is a lexicographically ordered word position
                // we use the key to construct the words fst.
                builder.insert(word)?;
            }
            let fst = builder.into_set().map_data(std::borrow::Cow::Owned).unwrap();
            let db_fst = index.words_fst(wtxn)?;

            // merge new fst with database fst
            let union_stream = fst.op().add(db_fst.stream()).union();
            let mut builder = fst::SetBuilder::memory();
            builder.extend_stream(union_stream)?;
            let fst = builder.into_set();
            index.put_words_fst(wtxn, &fst)?;
            is_merged_database = true;
        }
        TypedChunk::WordLevelPositionDocids(word_level_position_docids_iter) => {
            append_entries_into_database(
                word_level_position_docids_iter,
                &index.word_level_position_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                merge_cbo_roaring_bitmaps,
            )?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetNumberDocids(facet_id_f64_docids_iter) => {
            append_entries_into_database(
                facet_id_f64_docids_iter,
                &index.facet_id_f64_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                merge_cbo_roaring_bitmaps,
            )?;
            is_merged_database = true;
        }
        TypedChunk::WordPairProximityDocids(word_pair_proximity_docids_iter) => {
            append_entries_into_database(
                word_pair_proximity_docids_iter,
                &index.word_pair_proximity_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                merge_cbo_roaring_bitmaps,
            )?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdDocidFacetNumbers(mut fid_docid_facet_number) => {
            let index_fid_docid_facet_numbers =
                index.field_id_docid_facet_f64s.remap_types::<ByteSlice, ByteSlice>();
            while let Some((key, value)) = fid_docid_facet_number.next()? {
                if valid_lmdb_key(key) {
                    index_fid_docid_facet_numbers.put(wtxn, key, &value)?;
                }
            }
        }
        TypedChunk::FieldIdDocidFacetStrings(mut fid_docid_facet_string) => {
            let index_fid_docid_facet_strings =
                index.field_id_docid_facet_strings.remap_types::<ByteSlice, ByteSlice>();
            while let Some((key, value)) = fid_docid_facet_string.next()? {
                if valid_lmdb_key(key) {
                    index_fid_docid_facet_strings.put(wtxn, key, &value)?;
                }
            }
        }
        TypedChunk::FieldIdFacetStringDocids(facet_id_string_docids) => {
            append_entries_into_database(
                facet_id_string_docids,
                &index.facet_id_string_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                |new_values, db_values, buffer| {
                    let (_, new_values) = decode_prefix_string(new_values).unwrap();
                    let new_values = RoaringBitmap::deserialize_from(new_values)?;
                    let (db_original, db_values) = decode_prefix_string(db_values).unwrap();
                    let db_values = RoaringBitmap::deserialize_from(db_values)?;
                    let values = new_values | db_values;
                    encode_prefix_string(db_original, buffer)?;
                    Ok(values.serialize_into(buffer)?)
                },
            )?;
            is_merged_database = true;
        }
    }

    Ok((RoaringBitmap::new(), is_merged_database))
}

fn merge_roaring_bitmaps(new_value: &[u8], db_value: &[u8], buffer: &mut Vec<u8>) -> Result<()> {
    let new_value = RoaringBitmap::deserialize_from(new_value)?;
    let db_value = RoaringBitmap::deserialize_from(db_value)?;
    let value = new_value | db_value;
    Ok(serialize_roaring_bitmap(&value, buffer)?)
}

fn merge_cbo_roaring_bitmaps(
    new_value: &[u8],
    db_value: &[u8],
    buffer: &mut Vec<u8>,
) -> Result<()> {
    let new_value = CboRoaringBitmapCodec::deserialize_from(new_value)?;
    let db_value = CboRoaringBitmapCodec::deserialize_from(db_value)?;
    let value = new_value | db_value;
    Ok(CboRoaringBitmapCodec::serialize_into(&value, buffer))
}

/// Write provided entries in database using serialize_value function.
/// merge_values function is used if an entry already exist in the database.
fn write_entries_into_database<R, K, V, FS, FM>(
    mut data: grenad::Reader<R>,
    database: &heed::Database<K, V>,
    wtxn: &mut RwTxn,
    index_is_empty: bool,
    serialize_value: FS,
    merge_values: FM,
) -> Result<()>
where
    R: std::io::Read,
    FS: for<'a> Fn(&'a [u8], &'a mut Vec<u8>) -> Result<&'a [u8]>,
    FM: Fn(&[u8], &[u8], &mut Vec<u8>) -> Result<()>,
{
    let mut buffer = Vec::new();
    let database = database.remap_types::<ByteSlice, ByteSlice>();

    while let Some((key, value)) = data.next()? {
        if valid_lmdb_key(key) {
            buffer.clear();
            let value = if index_is_empty {
                serialize_value(value, &mut buffer)?
            } else {
                match database.get(wtxn, key)? {
                    Some(prev_value) => {
                        merge_values(value, prev_value, &mut buffer)?;
                        &buffer[..]
                    }
                    None => serialize_value(value, &mut buffer)?,
                }
            };
            database.put(wtxn, key, value)?;
        }
    }

    Ok(())
}

/// Write provided entries in database using serialize_value function.
/// merge_values function is used if an entry already exist in the database.
/// All provided entries must be ordered.
/// If the index is not empty, write_entries_into_database is called instead.
fn append_entries_into_database<R, K, V, FS, FM>(
    mut data: grenad::Reader<R>,
    database: &heed::Database<K, V>,
    wtxn: &mut RwTxn,
    index_is_empty: bool,
    serialize_value: FS,
    merge_values: FM,
) -> Result<()>
where
    R: std::io::Read,
    FS: for<'a> Fn(&'a [u8], &'a mut Vec<u8>) -> Result<&'a [u8]>,
    FM: Fn(&[u8], &[u8], &mut Vec<u8>) -> Result<()>,
{
    if !index_is_empty {
        return write_entries_into_database(
            data,
            database,
            wtxn,
            false,
            serialize_value,
            merge_values,
        );
    }

    let mut buffer = Vec::new();
    let mut database = database.iter_mut(wtxn)?.remap_types::<ByteSlice, ByteSlice>();

    while let Some((key, value)) = data.next()? {
        if valid_lmdb_key(key) {
            buffer.clear();
            let value = serialize_value(value, &mut buffer)?;
            unsafe { database.append(key, value)? };
        }
    }

    Ok(())
}

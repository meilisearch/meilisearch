use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io;

use charabia::{Language, Script};
use grenad::MergerBuilder;
use heed::types::ByteSlice;
use heed::{BytesDecode, RwTxn};
use roaring::RoaringBitmap;

use super::helpers::{
    self, merge_ignore_values, roaring_bitmap_from_u32s_array, serialize_roaring_bitmap,
    valid_lmdb_key, CursorClonableMmap,
};
use super::{ClonableMmap, MergeFn};
use crate::facet::FacetType;
use crate::update::facet::FacetsUpdate;
use crate::update::index_documents::helpers::as_cloneable_grenad;
use crate::{
    lat_lng_to_xyz, BoRoaringBitmapCodec, CboRoaringBitmapCodec, DocumentId, GeoPoint, Index,
    Result,
};

pub(crate) enum TypedChunk {
    DocidWordPositions(grenad::Reader<CursorClonableMmap>),
    FieldIdDocidFacetStrings(grenad::Reader<CursorClonableMmap>),
    FieldIdDocidFacetNumbers(grenad::Reader<CursorClonableMmap>),
    Documents(grenad::Reader<CursorClonableMmap>),
    FieldIdWordcountDocids(grenad::Reader<File>),
    NewDocumentsIds(RoaringBitmap),
    WordDocids {
        word_docids_reader: grenad::Reader<File>,
        exact_word_docids_reader: grenad::Reader<File>,
    },
    WordPositionDocids(grenad::Reader<File>),
    WordFidDocids(grenad::Reader<File>),
    WordPairProximityDocids(grenad::Reader<File>),
    FieldIdFacetStringDocids(grenad::Reader<File>),
    FieldIdFacetNumberDocids(grenad::Reader<File>),
    FieldIdFacetExistsDocids(grenad::Reader<File>),
    GeoPoints(grenad::Reader<File>),
    ScriptLanguageDocids(HashMap<(Script, Language), RoaringBitmap>),
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
        TypedChunk::Documents(obkv_documents_iter) => {
            let mut cursor = obkv_documents_iter.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
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
        TypedChunk::WordDocids { word_docids_reader, exact_word_docids_reader } => {
            let word_docids_iter = unsafe { as_cloneable_grenad(&word_docids_reader) }?;
            append_entries_into_database(
                word_docids_iter.clone(),
                &index.word_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                merge_roaring_bitmaps,
            )?;

            let exact_word_docids_iter = unsafe { as_cloneable_grenad(&exact_word_docids_reader) }?;
            append_entries_into_database(
                exact_word_docids_iter.clone(),
                &index.exact_word_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                merge_roaring_bitmaps,
            )?;

            // create fst from word docids
            let fst = merge_word_docids_reader_into_fst(word_docids_iter, exact_word_docids_iter)?;
            let db_fst = index.words_fst(wtxn)?;

            // merge new fst with database fst
            let union_stream = fst.op().add(db_fst.stream()).union();
            let mut builder = fst::SetBuilder::memory();
            builder.extend_stream(union_stream)?;
            let fst = builder.into_set();
            index.put_words_fst(wtxn, &fst)?;
            is_merged_database = true;
        }
        TypedChunk::WordPositionDocids(word_position_docids_iter) => {
            append_entries_into_database(
                word_position_docids_iter,
                &index.word_position_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                merge_cbo_roaring_bitmaps,
            )?;
            is_merged_database = true;
        }
        TypedChunk::WordFidDocids(word_fid_docids_iter) => {
            append_entries_into_database(
                word_fid_docids_iter,
                &index.word_fid_docids,
                wtxn,
                index_is_empty,
                |value, _buffer| Ok(value),
                merge_cbo_roaring_bitmaps,
            )?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetNumberDocids(facet_id_number_docids_iter) => {
            let indexer = FacetsUpdate::new(index, FacetType::Number, facet_id_number_docids_iter);
            indexer.execute(wtxn)?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetStringDocids(facet_id_string_docids_iter) => {
            let indexer = FacetsUpdate::new(index, FacetType::String, facet_id_string_docids_iter);
            indexer.execute(wtxn)?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetExistsDocids(facet_id_exists_docids) => {
            append_entries_into_database(
                facet_id_exists_docids,
                &index.facet_id_exists_docids,
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
        TypedChunk::FieldIdDocidFacetNumbers(fid_docid_facet_number) => {
            let index_fid_docid_facet_numbers =
                index.field_id_docid_facet_f64s.remap_types::<ByteSlice, ByteSlice>();
            let mut cursor = fid_docid_facet_number.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                if valid_lmdb_key(key) {
                    index_fid_docid_facet_numbers.put(wtxn, key, value)?;
                }
            }
        }
        TypedChunk::FieldIdDocidFacetStrings(fid_docid_facet_string) => {
            let index_fid_docid_facet_strings =
                index.field_id_docid_facet_strings.remap_types::<ByteSlice, ByteSlice>();
            let mut cursor = fid_docid_facet_string.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                if valid_lmdb_key(key) {
                    index_fid_docid_facet_strings.put(wtxn, key, value)?;
                }
            }
        }
        TypedChunk::GeoPoints(geo_points) => {
            let mut rtree = index.geo_rtree(wtxn)?.unwrap_or_default();
            let mut geo_faceted_docids = index.geo_faceted_documents_ids(wtxn)?;

            let mut cursor = geo_points.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                // convert the key back to a u32 (4 bytes)
                let docid = key.try_into().map(DocumentId::from_be_bytes).unwrap();

                // convert the latitude and longitude back to a f64 (8 bytes)
                let (lat, tail) = helpers::try_split_array_at::<u8, 8>(value).unwrap();
                let (lng, _) = helpers::try_split_array_at::<u8, 8>(tail).unwrap();
                let point = [f64::from_ne_bytes(lat), f64::from_ne_bytes(lng)];
                let xyz_point = lat_lng_to_xyz(&point);

                rtree.insert(GeoPoint::new(xyz_point, (docid, point)));
                geo_faceted_docids.insert(docid);
            }
            index.put_geo_rtree(wtxn, &rtree)?;
            index.put_geo_faceted_documents_ids(wtxn, &geo_faceted_docids)?;
        }
        TypedChunk::ScriptLanguageDocids(hash_pair) => {
            let mut buffer = Vec::new();
            for (key, value) in hash_pair {
                buffer.clear();
                let final_value = match index.script_language_docids.get(wtxn, &key)? {
                    Some(db_values) => {
                        let mut db_value_buffer = Vec::new();
                        serialize_roaring_bitmap(&db_values, &mut db_value_buffer)?;
                        let mut new_value_buffer = Vec::new();
                        serialize_roaring_bitmap(&value, &mut new_value_buffer)?;
                        merge_roaring_bitmaps(&new_value_buffer, &db_value_buffer, &mut buffer)?;
                        RoaringBitmap::deserialize_from(&buffer[..])?
                    }
                    None => value,
                };
                index.script_language_docids.put(wtxn, &key, &final_value)?;
            }
        }
    }

    Ok((RoaringBitmap::new(), is_merged_database))
}

fn merge_word_docids_reader_into_fst(
    word_docids_iter: grenad::Reader<io::Cursor<ClonableMmap>>,
    exact_word_docids_iter: grenad::Reader<io::Cursor<ClonableMmap>>,
) -> Result<fst::Set<Vec<u8>>> {
    let mut merger_builder = MergerBuilder::new(merge_ignore_values as MergeFn);
    merger_builder.push(word_docids_iter.into_cursor()?);
    merger_builder.push(exact_word_docids_iter.into_cursor()?);
    let mut iter = merger_builder.build().into_stream_merger_iter()?;
    let mut builder = fst::SetBuilder::memory();

    while let Some((k, _)) = iter.next()? {
        builder.insert(k)?;
    }

    Ok(builder.into_set())
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
    Ok(CboRoaringBitmapCodec::merge_into(
        &[Cow::Borrowed(db_value), Cow::Borrowed(new_value)],
        buffer,
    )?)
}

/// Write provided entries in database using serialize_value function.
/// merge_values function is used if an entry already exist in the database.
fn write_entries_into_database<R, K, V, FS, FM>(
    data: grenad::Reader<R>,
    database: &heed::Database<K, V>,
    wtxn: &mut RwTxn,
    index_is_empty: bool,
    serialize_value: FS,
    merge_values: FM,
) -> Result<()>
where
    R: io::Read + io::Seek,
    FS: for<'a> Fn(&'a [u8], &'a mut Vec<u8>) -> Result<&'a [u8]>,
    FM: Fn(&[u8], &[u8], &mut Vec<u8>) -> Result<()>,
{
    let mut buffer = Vec::new();
    let database = database.remap_types::<ByteSlice, ByteSlice>();

    let mut cursor = data.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
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
    data: grenad::Reader<R>,
    database: &heed::Database<K, V>,
    wtxn: &mut RwTxn,
    index_is_empty: bool,
    serialize_value: FS,
    merge_values: FM,
) -> Result<()>
where
    R: io::Read + io::Seek,
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

    let mut cursor = data.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        if valid_lmdb_key(key) {
            buffer.clear();
            let value = serialize_value(value, &mut buffer)?;
            unsafe { database.append(key, value)? };
        }
    }

    Ok(())
}

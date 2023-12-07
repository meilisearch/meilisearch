use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, BufReader};

use bytemuck::allocation::pod_collect_to_vec;
use charabia::{Language, Script};
use grenad::MergerBuilder;
use heed::types::Bytes;
use heed::{PutFlags, RwTxn};
use log::error;
use obkv::{KvReader, KvWriter};
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use super::helpers::{
    self, merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap, merge_ignore_values,
    valid_lmdb_key, CursorClonableMmap,
};
use super::{ClonableMmap, MergeFn};
use crate::distance::NDotProductPoint;
use crate::error::UserError;
use crate::external_documents_ids::{DocumentOperation, DocumentOperationKind};
use crate::facet::FacetType;
use crate::index::db_name::DOCUMENTS;
use crate::index::Hnsw;
use crate::update::del_add::{deladd_serialize_add_side, DelAdd, KvReaderDelAdd};
use crate::update::facet::FacetsUpdate;
use crate::update::index_documents::helpers::{as_cloneable_grenad, try_split_array_at};
use crate::update::{available_documents_ids, AvailableDocumentsIds};
use crate::{lat_lng_to_xyz, DocumentId, FieldId, GeoPoint, Index, Result, SerializationError};

pub(crate) enum TypedChunk {
    FieldIdDocidFacetStrings(grenad::Reader<CursorClonableMmap>),
    FieldIdDocidFacetNumbers(grenad::Reader<CursorClonableMmap>),
    Documents(grenad::Reader<CursorClonableMmap>),
    FieldIdWordCountDocids(grenad::Reader<BufReader<File>>),
    WordDocids {
        word_docids_reader: grenad::Reader<BufReader<File>>,
        exact_word_docids_reader: grenad::Reader<BufReader<File>>,
        word_fid_docids_reader: grenad::Reader<BufReader<File>>,
    },
    WordPositionDocids(grenad::Reader<BufReader<File>>),
    WordPairProximityDocids(grenad::Reader<BufReader<File>>),
    FieldIdFacetStringDocids(grenad::Reader<BufReader<File>>),
    FieldIdFacetNumberDocids(grenad::Reader<BufReader<File>>),
    FieldIdFacetExistsDocids(grenad::Reader<BufReader<File>>),
    FieldIdFacetIsNullDocids(grenad::Reader<BufReader<File>>),
    FieldIdFacetIsEmptyDocids(grenad::Reader<BufReader<File>>),
    GeoPoints(grenad::Reader<BufReader<File>>),
    VectorPoints {
        remove_vectors: grenad::Reader<BufReader<File>>,
        embeddings: Option<grenad::Reader<BufReader<File>>>,
        expected_dimension: usize,
        manual_vectors: grenad::Reader<BufReader<File>>,
    },
    ScriptLanguageDocids(HashMap<(Script, Language), (RoaringBitmap, RoaringBitmap)>),
}

impl TypedChunk {
    pub fn to_debug_string(&self) -> String {
        match self {
            TypedChunk::FieldIdDocidFacetStrings(grenad) => {
                format!("FieldIdDocidFacetStrings {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::FieldIdDocidFacetNumbers(grenad) => {
                format!("FieldIdDocidFacetNumbers {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::Documents(grenad) => {
                format!("Documents {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::FieldIdWordCountDocids(grenad) => {
                format!("FieldIdWordcountDocids {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::WordDocids {
                word_docids_reader,
                exact_word_docids_reader,
                word_fid_docids_reader,
            } => format!(
                "WordDocids {{ word_docids_reader: {}, exact_word_docids_reader: {}, word_fid_docids_reader: {} }}",
                word_docids_reader.len(),
                exact_word_docids_reader.len(),
                word_fid_docids_reader.len()
            ),
            TypedChunk::WordPositionDocids(grenad) => {
                format!("WordPositionDocids {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::WordPairProximityDocids(grenad) => {
                format!("WordPairProximityDocids {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::FieldIdFacetStringDocids(grenad) => {
                format!("FieldIdFacetStringDocids {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::FieldIdFacetNumberDocids(grenad) => {
                format!("FieldIdFacetNumberDocids {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::FieldIdFacetExistsDocids(grenad) => {
                format!("FieldIdFacetExistsDocids {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::FieldIdFacetIsNullDocids(grenad) => {
                format!("FieldIdFacetIsNullDocids {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::FieldIdFacetIsEmptyDocids(grenad) => {
                format!("FieldIdFacetIsEmptyDocids {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::GeoPoints(grenad) => {
                format!("GeoPoints {{ number_of_entries: {} }}", grenad.len())
            }
            TypedChunk::VectorPoints{ remove_vectors, manual_vectors, embeddings, expected_dimension } => {
                format!("VectorPoints {{ remove_vectors: {}, manual_vectors: {}, embeddings: {}, dimension: {} }}", remove_vectors.len(), manual_vectors.len(), embeddings.as_ref().map(|e| e.len()).unwrap_or_default(), expected_dimension)
            }
            TypedChunk::ScriptLanguageDocids(sl_map) => {
                format!("ScriptLanguageDocids {{ number_of_entries: {} }}", sl_map.len())
            }
        }
    }
}

/// Write typed chunk in the corresponding LMDB database of the provided index.
/// Return new documents seen.
pub(crate) fn write_typed_chunk_into_index(
    typed_chunk: TypedChunk,
    index: &Index,
    wtxn: &mut RwTxn,
    index_is_empty: bool,
) -> Result<(RoaringBitmap, bool)> {
    puffin::profile_function!(typed_chunk.to_debug_string());

    let mut is_merged_database = false;
    match typed_chunk {
        TypedChunk::Documents(obkv_documents_iter) => {
            let mut operations: Vec<DocumentOperation> = Default::default();

            let mut docids = index.documents_ids(wtxn)?;
            let mut cursor = obkv_documents_iter.into_cursor()?;
            while let Some((key, reader)) = cursor.move_on_next()? {
                let mut writer: KvWriter<_, FieldId> = KvWriter::memory();
                let reader: KvReader<FieldId> = KvReader::new(reader);

                let (document_id_bytes, external_id_bytes) = try_split_array_at(key)
                    .ok_or(SerializationError::Decoding { db_name: Some(DOCUMENTS) })?;
                let docid = DocumentId::from_be_bytes(document_id_bytes);
                let external_id = std::str::from_utf8(external_id_bytes)?;

                for (field_id, value) in reader.iter() {
                    let del_add_reader = KvReaderDelAdd::new(value);

                    if let Some(addition) = del_add_reader.get(DelAdd::Addition) {
                        writer.insert(field_id, addition)?;
                    }
                }

                let db = index.documents.remap_data_type::<Bytes>();

                if !writer.is_empty() {
                    db.put(wtxn, &docid, &writer.into_inner().unwrap())?;
                    operations.push(DocumentOperation {
                        external_id: external_id.to_string(),
                        internal_id: docid,
                        kind: DocumentOperationKind::Create,
                    });
                    docids.insert(docid);
                } else {
                    db.delete(wtxn, &docid)?;
                    operations.push(DocumentOperation {
                        external_id: external_id.to_string(),
                        internal_id: docid,
                        kind: DocumentOperationKind::Delete,
                    });
                    docids.remove(docid);
                }
            }
            let external_documents_docids = index.external_documents_ids();
            external_documents_docids.apply(wtxn, operations)?;
            index.put_documents_ids(wtxn, &docids)?;
        }
        TypedChunk::FieldIdWordCountDocids(fid_word_count_docids_iter) => {
            append_entries_into_database(
                fid_word_count_docids_iter,
                &index.field_id_word_count_docids,
                wtxn,
                index_is_empty,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::WordDocids {
            word_docids_reader,
            exact_word_docids_reader,
            word_fid_docids_reader,
        } => {
            let word_docids_iter = unsafe { as_cloneable_grenad(&word_docids_reader) }?;
            append_entries_into_database(
                word_docids_iter.clone(),
                &index.word_docids,
                wtxn,
                index_is_empty,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;

            let exact_word_docids_iter = unsafe { as_cloneable_grenad(&exact_word_docids_reader) }?;
            append_entries_into_database(
                exact_word_docids_iter.clone(),
                &index.exact_word_docids,
                wtxn,
                index_is_empty,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;

            let word_fid_docids_iter = unsafe { as_cloneable_grenad(&word_fid_docids_reader) }?;
            append_entries_into_database(
                word_fid_docids_iter,
                &index.word_fid_docids,
                wtxn,
                index_is_empty,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
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
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
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
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetIsNullDocids(facet_id_is_null_docids) => {
            append_entries_into_database(
                facet_id_is_null_docids,
                &index.facet_id_is_null_docids,
                wtxn,
                index_is_empty,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetIsEmptyDocids(facet_id_is_empty_docids) => {
            append_entries_into_database(
                facet_id_is_empty_docids,
                &index.facet_id_is_empty_docids,
                wtxn,
                index_is_empty,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::WordPairProximityDocids(word_pair_proximity_docids_iter) => {
            append_entries_into_database(
                word_pair_proximity_docids_iter,
                &index.word_pair_proximity_docids,
                wtxn,
                index_is_empty,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdDocidFacetNumbers(fid_docid_facet_number) => {
            let index_fid_docid_facet_numbers =
                index.field_id_docid_facet_f64s.remap_types::<Bytes, Bytes>();
            let mut cursor = fid_docid_facet_number.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                let reader = KvReaderDelAdd::new(value);
                if valid_lmdb_key(key) {
                    match (reader.get(DelAdd::Deletion), reader.get(DelAdd::Addition)) {
                        (None, None) => {}
                        (None, Some(new)) => index_fid_docid_facet_numbers.put(wtxn, key, new)?,
                        (Some(_), None) => {
                            index_fid_docid_facet_numbers.delete(wtxn, key)?;
                        }
                        (Some(_), Some(new)) => {
                            index_fid_docid_facet_numbers.put(wtxn, key, new)?
                        }
                    }
                }
            }
        }
        TypedChunk::FieldIdDocidFacetStrings(fid_docid_facet_string) => {
            let index_fid_docid_facet_strings =
                index.field_id_docid_facet_strings.remap_types::<Bytes, Bytes>();
            let mut cursor = fid_docid_facet_string.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                let reader = KvReaderDelAdd::new(value);
                if valid_lmdb_key(key) {
                    match (reader.get(DelAdd::Deletion), reader.get(DelAdd::Addition)) {
                        (None, None) => {}
                        (None, Some(new)) => index_fid_docid_facet_strings.put(wtxn, key, new)?,
                        (Some(_), None) => {
                            index_fid_docid_facet_strings.delete(wtxn, key)?;
                        }
                        (Some(_), Some(new)) => {
                            index_fid_docid_facet_strings.put(wtxn, key, new)?
                        }
                    }
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

                let deladd_obkv = KvReaderDelAdd::new(value);
                if let Some(value) = deladd_obkv.get(DelAdd::Deletion) {
                    let geopoint = extract_geo_point(value, docid);
                    rtree.remove(&geopoint);
                    geo_faceted_docids.remove(docid);
                }
                if let Some(value) = deladd_obkv.get(DelAdd::Addition) {
                    let geopoint = extract_geo_point(value, docid);
                    rtree.insert(geopoint);
                    geo_faceted_docids.insert(docid);
                }
            }
            index.put_geo_rtree(wtxn, &rtree)?;
            index.put_geo_faceted_documents_ids(wtxn, &geo_faceted_docids)?;
        }
        TypedChunk::VectorPoints {
            remove_vectors,
            manual_vectors,
            embeddings,
            expected_dimension,
        } => {
            if remove_vectors.is_empty()
                && manual_vectors.is_empty()
                && embeddings.as_ref().map_or(true, |e| e.is_empty())
            {
                return Ok((RoaringBitmap::new(), is_merged_database));
            }

            let mut unavailable_vector_ids = index.unavailable_vector_ids(&wtxn)?;
            /// FIXME: allow customizing distance
            /// FIXME: allow customizing index
            let writer = arroy::Writer::prepare(wtxn, index.vector_arroy, 0, expected_dimension)?;

            // remove vectors for docids we want them removed
            let mut cursor = remove_vectors.into_cursor()?;
            while let Some((key, _)) = cursor.move_on_next()? {
                let docid = key.try_into().map(DocumentId::from_be_bytes).unwrap();

                let Some(to_remove_vector_ids) = index.docid_vector_ids.get(&wtxn, &docid)? else {
                    continue;
                };
                unavailable_vector_ids -= to_remove_vector_ids;

                for item in to_remove_vector_ids {
                    writer.del_item(wtxn, item)?;
                }
            }

            let mut available_vector_ids =
                AvailableDocumentsIds::from_documents_ids(&unavailable_vector_ids);
            // add generated embeddings
            if let Some(embeddings) = embeddings {
                let mut cursor = embeddings.into_cursor()?;
                while let Some((key, value)) = cursor.move_on_next()? {
                    let docid = key.try_into().map(DocumentId::from_be_bytes).unwrap();
                    let data = pod_collect_to_vec(value);
                    // it is a code error to have embeddings and not expected_dimension
                    let embeddings =
                        crate::vector::Embeddings::from_inner(data, expected_dimension)
                            // code error if we somehow got the wrong dimension
                            .unwrap();

                    let mut new_vector_ids = RoaringBitmap::new();
                    for embedding in embeddings.iter() {
                        /// FIXME: error when you get over 9000
                        let next_vector_id = available_vector_ids.next().unwrap();
                        unavailable_vector_ids.insert(next_vector_id);

                        new_vector_ids.insert(next_vector_id);

                        index.vector_id_docid.put(wtxn, &next_vector_id, &docid)?;

                        writer.add_item(wtxn, next_vector_id, embedding)?;
                    }
                    index.docid_vector_ids.put(wtxn, &docid, &new_vector_ids)?;
                }
            }

            // perform the manual diff
            let mut cursor = manual_vectors.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                // convert the key back to a u32 (4 bytes)
                let (left, _index) = try_split_array_at(key).unwrap();
                let docid = DocumentId::from_be_bytes(left);

                let vector_deladd_obkv = KvReaderDelAdd::new(value);
                if let Some(value) = vector_deladd_obkv.get(DelAdd::Deletion) {
                    let vector = pod_collect_to_vec(value);
                    let Some(mut docid_vector_ids) = index.docid_vector_ids.get(&wtxn, &docid)?
                    else {
                        error!("Unable to delete the vector: {:?}", vector);
                        continue;
                    };
                    for item in docid_vector_ids {
                        /// FIXME: comparing the vectors by equality is inefficient, and dangerous by perfect equality
                        let candidate = writer.item_vector(&wtxn, item)?.expect("Inconsistent dbs");
                        if candidate == vector {
                            writer.del_item(wtxn, item)?;
                            unavailable_vector_ids.remove(item);
                            index.vector_id_docid.delete(wtxn, &item)?;
                            docid_vector_ids.remove(item);
                            break;
                        }
                    }
                    index.docid_vector_ids.put(wtxn, &docid, &docid_vector_ids)?;
                }
                let mut available_vector_ids =
                    AvailableDocumentsIds::from_documents_ids(&unavailable_vector_ids);

                if let Some(value) = vector_deladd_obkv.get(DelAdd::Addition) {
                    let vector = pod_collect_to_vec(value);
                    let next_vector_id = available_vector_ids.next().unwrap();

                    writer.add_item(wtxn, next_vector_id, &vector)?;
                    unavailable_vector_ids.insert(next_vector_id);
                    index.vector_id_docid.put(wtxn, &next_vector_id, &docid)?;
                    let mut docid_vector_ids =
                        index.docid_vector_ids.get(&wtxn, &docid)?.unwrap_or_default();
                    docid_vector_ids.insert(next_vector_id);
                    index.docid_vector_ids.put(wtxn, &docid, &docid_vector_ids)?;
                }
            }

            log::debug!("There are {} entries in the arroy so far", unavailable_vector_ids.len());
            index.put_unavailable_vector_ids(wtxn, unavailable_vector_ids)?;
        }
        TypedChunk::ScriptLanguageDocids(sl_map) => {
            for (key, (deletion, addition)) in sl_map {
                let mut db_key_exists = false;
                let final_value = match index.script_language_docids.get(wtxn, &key)? {
                    Some(db_values) => {
                        db_key_exists = true;
                        (db_values - deletion) | addition
                    }
                    None => addition,
                };

                if final_value.is_empty() {
                    // If the database entry exists, delete it.
                    if db_key_exists {
                        index.script_language_docids.delete(wtxn, &key)?;
                    }
                } else {
                    index.script_language_docids.put(wtxn, &key, &final_value)?;
                }
            }
        }
    }

    Ok((RoaringBitmap::new(), is_merged_database))
}

/// Converts the latitude and longitude back to an xyz GeoPoint.
fn extract_geo_point(value: &[u8], docid: DocumentId) -> GeoPoint {
    let (lat, tail) = helpers::try_split_array_at::<u8, 8>(value).unwrap();
    let (lng, _) = helpers::try_split_array_at::<u8, 8>(tail).unwrap();
    let point = [f64::from_ne_bytes(lat), f64::from_ne_bytes(lng)];
    let xyz_point = lat_lng_to_xyz(&point);
    GeoPoint::new(xyz_point, (docid, point))
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
    FM: for<'a> Fn(&[u8], &[u8], &'a mut Vec<u8>) -> Result<Option<&'a [u8]>>,
{
    puffin::profile_function!(format!("number of entries: {}", data.len()));

    let mut buffer = Vec::new();
    let database = database.remap_types::<Bytes, Bytes>();

    let mut cursor = data.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        if valid_lmdb_key(key) {
            buffer.clear();
            let value = if index_is_empty {
                Some(serialize_value(value, &mut buffer)?)
            } else {
                match database.get(wtxn, key)? {
                    Some(prev_value) => merge_values(value, prev_value, &mut buffer)?,
                    None => Some(serialize_value(value, &mut buffer)?),
                }
            };
            match value {
                Some(value) => database.put(wtxn, key, value)?,
                None => {
                    database.delete(wtxn, key)?;
                }
            }
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
    FM: for<'a> Fn(&[u8], &[u8], &'a mut Vec<u8>) -> Result<Option<&'a [u8]>>,
    K: for<'a> heed::BytesDecode<'a>,
{
    puffin::profile_function!(format!("number of entries: {}", data.len()));

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
    let mut database = database.iter_mut(wtxn)?.remap_types::<Bytes, Bytes>();

    let mut cursor = data.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        if valid_lmdb_key(key) {
            debug_assert!(
                K::bytes_decode(key).is_ok(),
                "Couldn't decode key with the database decoder, key length: {} - key bytes: {:x?}",
                key.len(),
                &key
            );
            buffer.clear();
            let value = serialize_value(value, &mut buffer)?;
            unsafe {
                // safety: We do not keep a reference to anything that lives inside the database
                database.put_current_with_options::<Bytes>(PutFlags::APPEND, key, value)?
            };
        }
    }

    Ok(())
}

use std::collections::HashSet;
use std::fs::File;

use grenad::Merger;
use heed::RoTxn;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde_json::Value;

use super::cache::CboCachedSorter;
use super::perm_json_p;
use crate::facet::value_encoding::f64_into_bytes;
use crate::update::new::{DocumentChange, ItemsPool, KvReaderFieldId};
use crate::update::{create_sorter, GrenadParameters, MergeDeladdCboRoaringBitmaps};
use crate::{
    normalize_facet, FieldId, GlobalFieldsIdsMap, Index, InternalError, Result, UserError,
    MAX_FACET_VALUE_LENGTH,
};

pub trait FacetedExtractor {
    fn run_extraction(
        index: &Index,
        fields_ids_map: &GlobalFieldsIdsMap,
        indexer: GrenadParameters,
        document_changes: impl IntoParallelIterator<Item = Result<DocumentChange>>,
    ) -> Result<Merger<File, MergeDeladdCboRoaringBitmaps>> {
        let max_memory = indexer.max_memory_by_thread();

        let rtxn = index.read_txn()?;
        let attributes_to_extract = Self::attributes_to_extract(&rtxn, index)?;
        let attributes_to_extract: Vec<_> =
            attributes_to_extract.iter().map(|s| s.as_ref()).collect();

        let context_pool = ItemsPool::new(|| {
            Ok((
                index.read_txn()?,
                fields_ids_map.clone(),
                Vec::new(),
                CboCachedSorter::new(
                    // TODO use a better value
                    100.try_into().unwrap(),
                    create_sorter(
                        grenad::SortAlgorithm::Stable,
                        MergeDeladdCboRoaringBitmaps,
                        indexer.chunk_compression_type,
                        indexer.chunk_compression_level,
                        indexer.max_nb_chunks,
                        max_memory,
                    ),
                ),
            ))
        });

        document_changes.into_par_iter().try_for_each(|document_change| {
            context_pool.with(|(rtxn, fields_ids_map, buffer, cached_sorter)| {
                Self::extract_document_change(
                    &*rtxn,
                    index,
                    buffer,
                    fields_ids_map,
                    &attributes_to_extract,
                    cached_sorter,
                    document_change?,
                )
            })
        })?;

        let mut builder = grenad::MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
        for (_rtxn, _fields_ids_map, _buffer, cache) in context_pool.into_items() {
            let sorter = cache.into_sorter()?;
            let readers = sorter.into_reader_cursors()?;
            builder.extend(readers);
        }

        Ok(builder.build())
    }

    fn extract_document_change(
        rtxn: &RoTxn,
        index: &Index,
        buffer: &mut Vec<u8>,
        fields_ids_map: &mut GlobalFieldsIdsMap,
        attributes_to_extract: &[&str],
        cached_sorter: &mut CboCachedSorter<MergeDeladdCboRoaringBitmaps>,
        document_change: DocumentChange,
    ) -> Result<()> {
        match document_change {
            DocumentChange::Deletion(inner) => {
                let mut facet_del_fn = |fid, value: &Value| -> Result<()> {
                    buffer.clear();
                    match Self::build_key(fid, value, buffer) {
                        // TODO manage errors
                        Some(key) => Ok(cached_sorter.insert_del_u32(&key, inner.docid()).unwrap()),
                        None => Ok(()),
                    }
                };

                extract_document_facets(
                    attributes_to_extract,
                    inner.current(rtxn, index)?.unwrap(),
                    fields_ids_map,
                    &mut facet_del_fn,
                )
            }
            DocumentChange::Update(inner) => {
                let mut facet_del_fn = |fid, value: &Value| -> Result<()> {
                    buffer.clear();
                    match Self::build_key(fid, value, buffer) {
                        // TODO manage errors
                        Some(key) => Ok(cached_sorter.insert_del_u32(&key, inner.docid()).unwrap()),
                        None => Ok(()),
                    }
                };

                extract_document_facets(
                    attributes_to_extract,
                    inner.current(rtxn, index)?.unwrap(),
                    fields_ids_map,
                    &mut facet_del_fn,
                )?;

                let mut facet_add_fn = |fid, value: &Value| -> Result<()> {
                    buffer.clear();
                    match Self::build_key(fid, value, buffer) {
                        // TODO manage errors
                        Some(key) => Ok(cached_sorter.insert_add_u32(&key, inner.docid()).unwrap()),
                        None => Ok(()),
                    }
                };

                extract_document_facets(
                    attributes_to_extract,
                    inner.new(),
                    fields_ids_map,
                    &mut facet_add_fn,
                )
            }
            DocumentChange::Insertion(inner) => {
                let mut facet_add_fn = |fid, value: &Value| -> Result<()> {
                    buffer.clear();
                    match Self::build_key(fid, value, buffer) {
                        // TODO manage errors
                        Some(key) => Ok(cached_sorter.insert_add_u32(&key, inner.docid()).unwrap()),
                        None => Ok(()),
                    }
                };

                extract_document_facets(
                    attributes_to_extract,
                    inner.new(),
                    fields_ids_map,
                    &mut facet_add_fn,
                )
            }
        }
    }

    // TODO avoid owning the strings here.
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>>;

    fn build_key<'b>(field_id: FieldId, value: &Value, output: &'b mut Vec<u8>)
        -> Option<&'b [u8]>;
}

pub struct FieldIdFacetNumberDocidsExtractor;
impl FacetedExtractor for FieldIdFacetNumberDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        let number = value.as_number()?;
        let n = number.as_f64()?;
        let ordered = f64_into_bytes(n)?;

        // fid - level - orderedf64 - orignalf64
        output.extend_from_slice(&field_id.to_be_bytes());
        output.push(1); // level 0
        output.extend_from_slice(&ordered);
        output.extend_from_slice(&n.to_be_bytes());

        Some(&*output)
    }
}

pub struct FieldIdFacetStringDocidsExtractor;
impl FacetedExtractor for FieldIdFacetStringDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        let string = value.as_str()?;
        let normalize = normalize_facet(string);
        let truncated = truncate_str(&normalize);

        // fid - level - normalized string
        output.extend_from_slice(&field_id.to_be_bytes());
        output.push(1); // level 0
        output.extend_from_slice(truncated.as_bytes());

        Some(&*output)
    }
}

pub struct FieldIdFacetIsNullDocidsExtractor;
impl FacetedExtractor for FieldIdFacetIsNullDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        if value.is_null() {
            output.extend_from_slice(&field_id.to_be_bytes());
            Some(&*output)
        } else {
            None
        }
    }
}

pub struct FieldIdFacetExistsDocidsExtractor;
impl FacetedExtractor for FieldIdFacetExistsDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        _value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        output.extend_from_slice(&field_id.to_be_bytes());
        Some(&*output)
    }
}

pub struct FieldIdFacetIsEmptyDocidsExtractor;
impl FacetedExtractor for FieldIdFacetIsEmptyDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        let is_empty = match value {
            Value::Null | Value::Bool(_) | Value::Number(_) => false,
            Value::String(s) => s.is_empty(),
            Value::Array(a) => a.is_empty(),
            Value::Object(o) => o.is_empty(),
        };

        if is_empty {
            output.extend_from_slice(&field_id.to_be_bytes());
            Some(&*output)
        } else {
            None
        }
    }
}

pub fn extract_document_facets(
    attributes_to_extract: &[&str],
    obkv: &KvReaderFieldId,
    field_id_map: &mut GlobalFieldsIdsMap,
    facet_fn: &mut impl FnMut(FieldId, &Value) -> Result<()>,
) -> Result<()> {
    let mut field_name = String::new();
    for (field_id, field_bytes) in obkv {
        let Some(field_name) = field_id_map.name(field_id).map(|s| {
            field_name.clear();
            field_name.push_str(s);
            &field_name
        }) else {
            unreachable!("field id not found in field id map");
        };

        let mut tokenize_field = |name: &str, value: &Value| match field_id_map.id_or_insert(name) {
            Some(field_id) => facet_fn(field_id, value),
            None => Err(UserError::AttributeLimitReached.into()),
        };

        // if the current field is searchable or contains a searchable attribute
        if perm_json_p::select_field(field_name, Some(attributes_to_extract), &[]) {
            // parse json.
            match serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)? {
                Value::Object(object) => perm_json_p::seek_leaf_values_in_object(
                    &object,
                    Some(attributes_to_extract),
                    &[], // skip no attributes
                    field_name,
                    &mut tokenize_field,
                )?,
                Value::Array(array) => perm_json_p::seek_leaf_values_in_array(
                    &array,
                    Some(attributes_to_extract),
                    &[], // skip no attributes
                    field_name,
                    &mut tokenize_field,
                )?,
                value => tokenize_field(field_name, &value)?,
            }
        }
    }

    Ok(())
}

/// Truncates a string to the biggest valid LMDB key size.
fn truncate_str(s: &str) -> &str {
    let index = s
        .char_indices()
        .map(|(idx, _)| idx)
        .chain(std::iter::once(s.len()))
        .take_while(|idx| idx <= &MAX_FACET_VALUE_LENGTH)
        .last();

    &s[..index.unwrap_or(0)]
}

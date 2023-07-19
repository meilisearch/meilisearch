use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;
use std::fs::File;
use std::io;
use std::mem::size_of;

use heed::zerocopy::AsBytes;
use heed::BytesEncode;
use roaring::RoaringBitmap;
use serde_json::{from_slice, Value};

use super::helpers::{create_sorter, keep_first, sorter_into_reader, GrenadParameters};
use crate::error::InternalError;
use crate::facet::value_encoding::f64_into_bytes;
use crate::update::index_documents::{create_writer, writer_into_reader};
use crate::{CboRoaringBitmapCodec, DocumentId, FieldId, Result, BEU32, MAX_FACET_VALUE_LENGTH};

/// The extracted facet values stored in grenad files by type.
pub struct ExtractedFacetValues {
    pub docid_fid_facet_numbers_chunk: grenad::Reader<File>,
    pub docid_fid_facet_strings_chunk: grenad::Reader<File>,
    pub fid_facet_is_null_docids_chunk: grenad::Reader<File>,
    pub fid_facet_is_empty_docids_chunk: grenad::Reader<File>,
    pub fid_facet_exists_docids_chunk: grenad::Reader<File>,
}

/// Extracts the facet values of each faceted field of each document.
///
/// Returns the generated grenad reader containing the docid the fid and the orginal value as key
/// and the normalized value as value extracted from the given chunk of documents.
#[logging_timer::time]
pub fn extract_fid_docid_facet_values<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    faceted_fields: &HashSet<FieldId>,
) -> Result<ExtractedFacetValues> {
    puffin::profile_function!();

    let max_memory = indexer.max_memory_by_thread();

    let mut fid_docid_facet_numbers_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        keep_first,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 2),
    );

    let mut fid_docid_facet_strings_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        keep_first,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 2),
    );

    let mut facet_exists_docids = BTreeMap::<FieldId, RoaringBitmap>::new();
    let mut facet_is_null_docids = BTreeMap::<FieldId, RoaringBitmap>::new();
    let mut facet_is_empty_docids = BTreeMap::<FieldId, RoaringBitmap>::new();

    let mut key_buffer = Vec::new();
    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((docid_bytes, value)) = cursor.move_on_next()? {
        let obkv = obkv::KvReader::new(value);

        for (field_id, field_bytes) in obkv.iter() {
            if faceted_fields.contains(&field_id) {
                key_buffer.clear();

                // Set key to the field_id
                // Note: this encoding is consistent with FieldIdCodec
                key_buffer.extend_from_slice(&field_id.to_be_bytes());

                // Here, we know already that the document must be added to the “field id exists” database
                let document: [u8; 4] = docid_bytes[..4].try_into().ok().unwrap();
                let document = BEU32::from(document).get();

                facet_exists_docids.entry(field_id).or_default().insert(document);

                // For the other extraction tasks, prefix the key with the field_id and the document_id
                key_buffer.extend_from_slice(docid_bytes);

                let value = from_slice(field_bytes).map_err(InternalError::SerdeJson)?;

                match extract_facet_values(&value) {
                    FilterableValues::Null => {
                        facet_is_null_docids.entry(field_id).or_default().insert(document);
                    }
                    FilterableValues::Empty => {
                        facet_is_empty_docids.entry(field_id).or_default().insert(document);
                    }
                    FilterableValues::Values { numbers, strings } => {
                        // insert facet numbers in sorter
                        for number in numbers {
                            key_buffer.truncate(size_of::<FieldId>() + size_of::<DocumentId>());
                            if let Some(value_bytes) = f64_into_bytes(number) {
                                key_buffer.extend_from_slice(&value_bytes);
                                key_buffer.extend_from_slice(&number.to_be_bytes());

                                fid_docid_facet_numbers_sorter
                                    .insert(&key_buffer, ().as_bytes())?;
                            }
                        }

                        // insert normalized and original facet string in sorter
                        for (normalized, original) in
                            strings.into_iter().filter(|(n, _)| !n.is_empty())
                        {
                            let normalized_truncated_value: String = normalized
                                .char_indices()
                                .take_while(|(idx, _)| idx + 4 < MAX_FACET_VALUE_LENGTH)
                                .map(|(_, c)| c)
                                .collect();

                            key_buffer.truncate(size_of::<FieldId>() + size_of::<DocumentId>());
                            key_buffer.extend_from_slice(normalized_truncated_value.as_bytes());
                            fid_docid_facet_strings_sorter
                                .insert(&key_buffer, original.as_bytes())?;
                        }
                    }
                }
            }
        }
    }

    let mut facet_exists_docids_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );
    for (fid, bitmap) in facet_exists_docids.into_iter() {
        let bitmap_bytes = CboRoaringBitmapCodec::bytes_encode(&bitmap).unwrap();
        facet_exists_docids_writer.insert(fid.to_be_bytes(), &bitmap_bytes)?;
    }
    let facet_exists_docids_reader = writer_into_reader(facet_exists_docids_writer)?;

    let mut facet_is_null_docids_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );
    for (fid, bitmap) in facet_is_null_docids.into_iter() {
        let bitmap_bytes = CboRoaringBitmapCodec::bytes_encode(&bitmap).unwrap();
        facet_is_null_docids_writer.insert(fid.to_be_bytes(), &bitmap_bytes)?;
    }
    let facet_is_null_docids_reader = writer_into_reader(facet_is_null_docids_writer)?;

    let mut facet_is_empty_docids_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );
    for (fid, bitmap) in facet_is_empty_docids.into_iter() {
        let bitmap_bytes = CboRoaringBitmapCodec::bytes_encode(&bitmap).unwrap();
        facet_is_empty_docids_writer.insert(fid.to_be_bytes(), &bitmap_bytes)?;
    }
    let facet_is_empty_docids_reader = writer_into_reader(facet_is_empty_docids_writer)?;

    Ok(ExtractedFacetValues {
        docid_fid_facet_numbers_chunk: sorter_into_reader(fid_docid_facet_numbers_sorter, indexer)?,
        docid_fid_facet_strings_chunk: sorter_into_reader(fid_docid_facet_strings_sorter, indexer)?,
        fid_facet_is_null_docids_chunk: facet_is_null_docids_reader,
        fid_facet_is_empty_docids_chunk: facet_is_empty_docids_reader,
        fid_facet_exists_docids_chunk: facet_exists_docids_reader,
    })
}

/// Represent what a document field contains.
enum FilterableValues {
    /// Corresponds to the JSON `null` value.
    Null,
    /// Corresponds to either, an empty string `""`, an empty array `[]`, or an empty object `{}`.
    Empty,
    /// Represents all the numbers and strings values found in this document field.
    Values { numbers: Vec<f64>, strings: Vec<(String, String)> },
}

fn extract_facet_values(value: &Value) -> FilterableValues {
    fn inner_extract_facet_values(
        value: &Value,
        can_recurse: bool,
        output_numbers: &mut Vec<f64>,
        output_strings: &mut Vec<(String, String)>,
    ) {
        match value {
            Value::Null => (),
            Value::Bool(b) => output_strings.push((b.to_string(), b.to_string())),
            Value::Number(number) => {
                if let Some(float) = number.as_f64() {
                    output_numbers.push(float);
                }
            }
            Value::String(original) => {
                let normalized = crate::normalize_facet(original);
                output_strings.push((normalized, original.clone()));
            }
            Value::Array(values) => {
                if can_recurse {
                    for value in values {
                        inner_extract_facet_values(value, false, output_numbers, output_strings);
                    }
                }
            }
            Value::Object(_) => (),
        }
    }

    match value {
        Value::Null => FilterableValues::Null,
        Value::String(s) if s.is_empty() => FilterableValues::Empty,
        Value::Array(a) if a.is_empty() => FilterableValues::Empty,
        Value::Object(o) if o.is_empty() => FilterableValues::Empty,
        otherwise => {
            let mut numbers = Vec::new();
            let mut strings = Vec::new();
            inner_extract_facet_values(otherwise, true, &mut numbers, &mut strings);
            FilterableValues::Values { numbers, strings }
        }
    }
}

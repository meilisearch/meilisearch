use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;
use std::fs::File;
use std::io;
use std::mem::size_of;

use heed::zerocopy::AsBytes;
use heed::BytesEncode;
use roaring::RoaringBitmap;
use serde_json::Value;

use super::helpers::{create_sorter, keep_first, sorter_into_reader, GrenadParameters};
use crate::error::InternalError;
use crate::facet::value_encoding::f64_into_bytes;
use crate::update::index_documents::{create_writer, writer_into_reader};
use crate::{CboRoaringBitmapCodec, DocumentId, FieldId, Result, BEU32};

/// Extracts the facet values of each faceted field of each document.
///
/// Returns the generated grenad reader containing the docid the fid and the orginal value as key
/// and the normalized value as value extracted from the given chunk of documents.
#[logging_timer::time]
pub fn extract_fid_docid_facet_values<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    faceted_fields: &HashSet<FieldId>,
) -> Result<(grenad::Reader<File>, grenad::Reader<File>, grenad::Reader<File>)> {
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

                let value =
                    serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)?;

                let (numbers, strings) = extract_facet_values(&value);

                // insert facet numbers in sorter
                for number in numbers {
                    key_buffer.truncate(size_of::<FieldId>() + size_of::<DocumentId>());
                    if let Some(value_bytes) = f64_into_bytes(number) {
                        key_buffer.extend_from_slice(&value_bytes);
                        key_buffer.extend_from_slice(&number.to_be_bytes());

                        fid_docid_facet_numbers_sorter.insert(&key_buffer, ().as_bytes())?;
                    }
                }

                // insert  normalized and original facet string in sorter
                for (normalized, original) in strings.into_iter().filter(|(n, _)| !n.is_empty()) {
                    key_buffer.truncate(size_of::<FieldId>() + size_of::<DocumentId>());
                    key_buffer.extend_from_slice(normalized.as_bytes());
                    fid_docid_facet_strings_sorter.insert(&key_buffer, original.as_bytes())?;
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

    Ok((
        sorter_into_reader(fid_docid_facet_numbers_sorter, indexer)?,
        sorter_into_reader(fid_docid_facet_strings_sorter, indexer)?,
        facet_exists_docids_reader,
    ))
}

fn extract_facet_values(value: &Value) -> (Vec<f64>, Vec<(String, String)>) {
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
                let normalized = original.trim().to_lowercase();
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

    let mut facet_number_values = Vec::new();
    let mut facet_string_values = Vec::new();
    inner_extract_facet_values(value, true, &mut facet_number_values, &mut facet_string_values);

    (facet_number_values, facet_string_values)
}

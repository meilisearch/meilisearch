use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, BufReader};
use std::mem::size_of;

use bytemuck::bytes_of;
use grenad::Sorter;
use heed::BytesEncode;
use itertools::{merge_join_by, EitherOrBoth, Itertools};
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use serde_json::{from_slice, Value};
use FilterableValues::{Empty, Null, Values};

use super::helpers::{create_sorter, sorter_into_reader, GrenadParameters, KeepFirst};
use crate::error::InternalError;
use crate::facet::value_encoding::f64_into_bytes;
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::{create_writer, writer_into_reader};
use crate::update::settings::InnerIndexSettingsDiff;
use crate::{CboRoaringBitmapCodec, DocumentId, FieldId, Result, MAX_FACET_VALUE_LENGTH};

/// The length of the elements that are always in the buffer when inserting new values.
const TRUNCATE_SIZE: usize = size_of::<FieldId>() + size_of::<DocumentId>();

/// The extracted facet values stored in grenad files by type.
pub struct ExtractedFacetValues {
    pub fid_docid_facet_numbers_chunk: grenad::Reader<BufReader<File>>,
    pub fid_docid_facet_strings_chunk: grenad::Reader<BufReader<File>>,
    pub fid_facet_is_null_docids_chunk: grenad::Reader<BufReader<File>>,
    pub fid_facet_is_empty_docids_chunk: grenad::Reader<BufReader<File>>,
    pub fid_facet_exists_docids_chunk: grenad::Reader<BufReader<File>>,
}

/// Extracts the facet values of each faceted field of each document.
///
/// Returns the generated grenad reader containing the docid the fid and the original value as key
/// and the normalized value as value extracted from the given chunk of documents.
/// We need the fid of the geofields to correctly parse them as numbers if they were sent as strings initially.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_fid_docid_facet_values<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    settings_diff: &InnerIndexSettingsDiff,
) -> Result<ExtractedFacetValues> {
    let max_memory = indexer.max_memory_by_thread();

    let mut fid_docid_facet_numbers_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        KeepFirst,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 2),
        true,
    );

    let mut fid_docid_facet_strings_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        KeepFirst,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 2),
        true,
    );

    // The tuples represents the Del and Add side for a bitmap
    let mut facet_exists_docids = BTreeMap::<FieldId, (RoaringBitmap, RoaringBitmap)>::new();
    let mut facet_is_null_docids = BTreeMap::<FieldId, (RoaringBitmap, RoaringBitmap)>::new();
    let mut facet_is_empty_docids = BTreeMap::<FieldId, (RoaringBitmap, RoaringBitmap)>::new();

    // We create two buffers for mutable ref issues with closures.
    let mut numbers_key_buffer = Vec::new();
    let mut strings_key_buffer = Vec::new();

    let old_faceted_fids: BTreeSet<_> =
        settings_diff.list_faceted_fields_from_fid_map(DelAdd::Deletion);
    let new_faceted_fids: BTreeSet<_> =
        settings_diff.list_faceted_fields_from_fid_map(DelAdd::Addition);

    if !settings_diff.settings_update_only || settings_diff.reindex_facets() {
        let mut cursor = obkv_documents.into_cursor()?;
        while let Some((docid_bytes, value)) = cursor.move_on_next()? {
            let obkv = obkv::KvReader::from_slice(value);
            let get_document_json_value = move |field_id, side| {
                obkv.get(field_id)
                    .map(KvReaderDelAdd::from_slice)
                    .and_then(|kv| kv.get(side))
                    .map(from_slice)
                    .transpose()
                    .map_err(InternalError::SerdeJson)
            };
            // iterate over the faceted fields instead of over the whole document.
            for eob in
                merge_join_by(old_faceted_fids.iter(), new_faceted_fids.iter(), |old, new| {
                    old.cmp(new)
                })
            {
                let (field_id, del_value, add_value) = match eob {
                    EitherOrBoth::Left(&field_id) => {
                        let del_value = get_document_json_value(field_id, DelAdd::Deletion)?;

                        // deletion only
                        (field_id, del_value, None)
                    }
                    EitherOrBoth::Right(&field_id) => {
                        let add_value = get_document_json_value(field_id, DelAdd::Addition)?;

                        // addition only
                        (field_id, None, add_value)
                    }
                    EitherOrBoth::Both(&field_id, _) => {
                        // during settings update, recompute the changing settings only unless a global change is detected.
                        if settings_diff.settings_update_only
                            && !settings_diff.global_facet_settings_changed()
                        {
                            continue;
                        }

                        let del_value = get_document_json_value(field_id, DelAdd::Deletion)?;
                        let add_value = get_document_json_value(field_id, DelAdd::Addition)?;

                        (field_id, del_value, add_value)
                    }
                };

                if del_value.is_some() || add_value.is_some() {
                    numbers_key_buffer.clear();
                    strings_key_buffer.clear();

                    // Set key to the field_id
                    // Note: this encoding is consistent with FieldIdCodec
                    numbers_key_buffer.extend_from_slice(&field_id.to_be_bytes());
                    strings_key_buffer.extend_from_slice(&field_id.to_be_bytes());

                    let document: [u8; 4] = docid_bytes[..4].try_into().ok().unwrap();
                    let document = DocumentId::from_be_bytes(document);

                    // For the other extraction tasks, prefix the key with the field_id and the document_id
                    numbers_key_buffer.extend_from_slice(docid_bytes);
                    strings_key_buffer.extend_from_slice(docid_bytes);

                    // We insert the document id on the Del and the Add side if the field exists.
                    let (del_exists, add_exists) =
                        facet_exists_docids.entry(field_id).or_default();
                    let (del_is_null, add_is_null) =
                        facet_is_null_docids.entry(field_id).or_default();
                    let (del_is_empty, add_is_empty) =
                        facet_is_empty_docids.entry(field_id).or_default();

                    if del_value.is_some() {
                        del_exists.insert(document);
                    }
                    if add_value.is_some() {
                        add_exists.insert(document);
                    }

                    let del_geo_support = settings_diff
                        .old
                        .geo_fields_ids
                        .is_some_and(|(lat, lng)| field_id == lat || field_id == lng);
                    let add_geo_support = settings_diff
                        .new
                        .geo_fields_ids
                        .is_some_and(|(lat, lng)| field_id == lat || field_id == lng);
                    let del_filterable_values =
                        del_value.map(|value| extract_facet_values(&value, del_geo_support));
                    let add_filterable_values =
                        add_value.map(|value| extract_facet_values(&value, add_geo_support));

                    // Those closures are just here to simplify things a bit.
                    let mut insert_numbers_diff = |del_numbers, add_numbers| {
                        insert_numbers_diff(
                            &mut fid_docid_facet_numbers_sorter,
                            &mut numbers_key_buffer,
                            del_numbers,
                            add_numbers,
                        )
                    };
                    let mut insert_strings_diff = |del_strings, add_strings| {
                        insert_strings_diff(
                            &mut fid_docid_facet_strings_sorter,
                            &mut strings_key_buffer,
                            del_strings,
                            add_strings,
                        )
                    };

                    match (del_filterable_values, add_filterable_values) {
                        (None, None) => (),
                        (Some(del_filterable_values), None) => match del_filterable_values {
                            Null => {
                                del_is_null.insert(document);
                            }
                            Empty => {
                                del_is_empty.insert(document);
                            }
                            Values { numbers, strings } => {
                                insert_numbers_diff(numbers, vec![])?;
                                insert_strings_diff(strings, vec![])?;
                            }
                        },
                        (None, Some(add_filterable_values)) => match add_filterable_values {
                            Null => {
                                add_is_null.insert(document);
                            }
                            Empty => {
                                add_is_empty.insert(document);
                            }
                            Values { numbers, strings } => {
                                insert_numbers_diff(vec![], numbers)?;
                                insert_strings_diff(vec![], strings)?;
                            }
                        },
                        (Some(del_filterable_values), Some(add_filterable_values)) => {
                            match (del_filterable_values, add_filterable_values) {
                                (Null, Null) | (Empty, Empty) => (),
                                (Null, Empty) => {
                                    del_is_null.insert(document);
                                    add_is_empty.insert(document);
                                }
                                (Empty, Null) => {
                                    del_is_empty.insert(document);
                                    add_is_null.insert(document);
                                }
                                (Null, Values { numbers, strings }) => {
                                    insert_numbers_diff(vec![], numbers)?;
                                    insert_strings_diff(vec![], strings)?;
                                    del_is_null.insert(document);
                                }
                                (Empty, Values { numbers, strings }) => {
                                    insert_numbers_diff(vec![], numbers)?;
                                    insert_strings_diff(vec![], strings)?;
                                    del_is_empty.insert(document);
                                }
                                (Values { numbers, strings }, Null) => {
                                    add_is_null.insert(document);
                                    insert_numbers_diff(numbers, vec![])?;
                                    insert_strings_diff(strings, vec![])?;
                                }
                                (Values { numbers, strings }, Empty) => {
                                    add_is_empty.insert(document);
                                    insert_numbers_diff(numbers, vec![])?;
                                    insert_strings_diff(strings, vec![])?;
                                }
                                (
                                    Values { numbers: del_numbers, strings: del_strings },
                                    Values { numbers: add_numbers, strings: add_strings },
                                ) => {
                                    insert_numbers_diff(del_numbers, add_numbers)?;
                                    insert_strings_diff(del_strings, add_strings)?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut buffer = Vec::new();
    let mut facet_exists_docids_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );
    for (fid, (del_bitmap, add_bitmap)) in facet_exists_docids.into_iter() {
        deladd_obkv_cbo_roaring_bitmaps(&mut buffer, &del_bitmap, &add_bitmap)?;
        facet_exists_docids_writer.insert(fid.to_be_bytes(), &buffer)?;
    }
    let facet_exists_docids_reader = writer_into_reader(facet_exists_docids_writer)?;

    let mut facet_is_null_docids_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );
    for (fid, (del_bitmap, add_bitmap)) in facet_is_null_docids.into_iter() {
        deladd_obkv_cbo_roaring_bitmaps(&mut buffer, &del_bitmap, &add_bitmap)?;
        facet_is_null_docids_writer.insert(fid.to_be_bytes(), &buffer)?;
    }
    let facet_is_null_docids_reader = writer_into_reader(facet_is_null_docids_writer)?;

    let mut facet_is_empty_docids_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );
    for (fid, (del_bitmap, add_bitmap)) in facet_is_empty_docids.into_iter() {
        deladd_obkv_cbo_roaring_bitmaps(&mut buffer, &del_bitmap, &add_bitmap)?;
        facet_is_empty_docids_writer.insert(fid.to_be_bytes(), &buffer)?;
    }
    let facet_is_empty_docids_reader = writer_into_reader(facet_is_empty_docids_writer)?;

    Ok(ExtractedFacetValues {
        fid_docid_facet_numbers_chunk: sorter_into_reader(fid_docid_facet_numbers_sorter, indexer)?,
        fid_docid_facet_strings_chunk: sorter_into_reader(fid_docid_facet_strings_sorter, indexer)?,
        fid_facet_is_null_docids_chunk: facet_is_null_docids_reader,
        fid_facet_is_empty_docids_chunk: facet_is_empty_docids_reader,
        fid_facet_exists_docids_chunk: facet_exists_docids_reader,
    })
}

/// Generates a vector of bytes containing a DelAdd obkv with two bitmaps.
fn deladd_obkv_cbo_roaring_bitmaps(
    buffer: &mut Vec<u8>,
    del_bitmap: &RoaringBitmap,
    add_bitmap: &RoaringBitmap,
) -> io::Result<()> {
    buffer.clear();
    let mut obkv = KvWriterDelAdd::new(buffer);
    let del_bitmap_bytes = CboRoaringBitmapCodec::bytes_encode(del_bitmap).unwrap();
    let add_bitmap_bytes = CboRoaringBitmapCodec::bytes_encode(add_bitmap).unwrap();
    obkv.insert(DelAdd::Deletion, del_bitmap_bytes)?;
    obkv.insert(DelAdd::Addition, add_bitmap_bytes)?;
    obkv.finish()
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

/// Computes the diff between both Del and Add numbers and
/// only inserts the parts that differ in the sorter.
fn insert_numbers_diff(
    fid_docid_facet_numbers_sorter: &mut Sorter<KeepFirst>,
    key_buffer: &mut Vec<u8>,
    mut del_numbers: Vec<f64>,
    mut add_numbers: Vec<f64>,
) -> Result<()> {
    // We sort and dedup the float numbers
    del_numbers.sort_unstable_by_key(|f| OrderedFloat(*f));
    add_numbers.sort_unstable_by_key(|f| OrderedFloat(*f));
    del_numbers.dedup_by_key(|f| OrderedFloat(*f));
    add_numbers.dedup_by_key(|f| OrderedFloat(*f));

    let merged_numbers_iter = itertools::merge_join_by(
        del_numbers.into_iter().map(OrderedFloat),
        add_numbers.into_iter().map(OrderedFloat),
        |del, add| del.cmp(add),
    );

    // insert facet numbers in sorter
    for eob in merged_numbers_iter {
        key_buffer.truncate(TRUNCATE_SIZE);
        match eob {
            EitherOrBoth::Both(_, _) => (), // no need to touch anything
            EitherOrBoth::Left(OrderedFloat(number)) => {
                if let Some(value_bytes) = f64_into_bytes(number) {
                    key_buffer.extend_from_slice(&value_bytes);
                    key_buffer.extend_from_slice(&number.to_be_bytes());

                    // We insert only the Del part of the Obkv to inform
                    // that we only want to remove all those numbers.
                    let mut obkv = KvWriterDelAdd::memory();
                    obkv.insert(DelAdd::Deletion, bytes_of(&()))?;
                    let bytes = obkv.into_inner()?;
                    fid_docid_facet_numbers_sorter.insert(&key_buffer, bytes)?;
                }
            }
            EitherOrBoth::Right(OrderedFloat(number)) => {
                if let Some(value_bytes) = f64_into_bytes(number) {
                    key_buffer.extend_from_slice(&value_bytes);
                    key_buffer.extend_from_slice(&number.to_be_bytes());

                    // We insert only the Add part of the Obkv to inform
                    // that we only want to remove all those numbers.
                    let mut obkv = KvWriterDelAdd::memory();
                    obkv.insert(DelAdd::Addition, bytes_of(&()))?;
                    let bytes = obkv.into_inner()?;
                    fid_docid_facet_numbers_sorter.insert(&key_buffer, bytes)?;
                }
            }
        }
    }

    Ok(())
}

/// Computes the diff between both Del and Add strings and
/// only inserts the parts that differ in the sorter.
fn insert_strings_diff(
    fid_docid_facet_strings_sorter: &mut Sorter<KeepFirst>,
    key_buffer: &mut Vec<u8>,
    mut del_strings: Vec<(String, String)>,
    mut add_strings: Vec<(String, String)>,
) -> Result<()> {
    // We sort and dedup the normalized and original strings
    del_strings.sort_unstable();
    add_strings.sort_unstable();
    del_strings.dedup();
    add_strings.dedup();

    let del_strings = del_strings.iter().chunk_by(|(normalized, _)| normalized);
    let add_strings = add_strings.iter().chunk_by(|(normalized, _)| normalized);

    let merged_strings_iter = itertools::merge_join_by(
        del_strings.into_iter().filter(|(n, _)| !n.is_empty()),
        add_strings.into_iter().filter(|(n, _)| !n.is_empty()),
        |(normalized_del, _), (normalized_add, _)| normalized_del.cmp(normalized_add),
    );

    // insert normalized and original facet string in sorter
    for eob in merged_strings_iter {
        key_buffer.truncate(TRUNCATE_SIZE);
        let (side, normalized, original) = match eob {
            EitherOrBoth::Both((normalized, del), (_, add)) => {
                let merged_strings_iter =
                    itertools::merge_join_by(del, add, |(_, original_del), (_, original_add)| {
                        original_del.cmp(original_add)
                    });

                // FIXME: we're in a bit of a pickle here, because we're only saving **one** original value per side,
                // but we possibly have multiple original values that changed in the case where the field is an
                // array of multiple values that normalize to the same value.
                // (e.g. "foo" = ["bar", "Bar", "bAr", "baR"]. I'm not judging why you would do that ¯\_(ツ)_/¯)
                //
                // We'll work best effort by ignoring when the same value appears in both sides, deleting the first
                // value that is only in the old version, and adding the first value that is only in the new version
                let mut obkv = KvWriterDelAdd::memory();
                let mut del = None;
                let mut add = None;
                let mut both = None;

                for eob in merged_strings_iter {
                    match eob {
                        EitherOrBoth::Both((_normalized, original), _) => {
                            both = match both {
                                Some(both) => Some(both),
                                None => Some(original),
                            }
                        }
                        EitherOrBoth::Left((_normalized, original)) => {
                            del = match del {
                                Some(del) => Some(del),
                                None => Some(original),
                            };
                        }
                        EitherOrBoth::Right((_normalized, original)) => {
                            add = match add {
                                Some(add) => Some(add),
                                None => Some(original),
                            }
                        }
                    }
                }

                if let Some(del) = del {
                    obkv.insert(DelAdd::Deletion, del)?;
                }
                if let Some(add) = add
                    // prefer the newly added, but if there is none, keep a value in the list of values
                    // since the normalized value appears both in old and new, we should never remove it.
                    .or(both)
                {
                    obkv.insert(DelAdd::Addition, add)?;
                }

                let truncated = truncate_str(normalized);
                key_buffer.extend_from_slice(truncated.as_bytes());

                let bytes = obkv.into_inner()?;
                fid_docid_facet_strings_sorter.insert(&key_buffer, bytes)?;
                continue;
            }
            EitherOrBoth::Left((_normalized, mut original)) => {
                // FIXME: we only consider the first value for the purpose of facet search
                // another structure is needed, able to retain all originals associated with a normalized value.
                let Some((normalized, original)) = original.next() else {
                    continue;
                };
                (DelAdd::Deletion, normalized, original)
            }
            EitherOrBoth::Right((_normalized, mut original)) => {
                // FIXME: we only consider the first value for the purpose of facet search
                // another structure is needed, able to retain all originals associated with a normalized value.
                let Some((normalized, original)) = original.next() else {
                    continue;
                };
                (DelAdd::Addition, normalized, original)
            }
        };
        let truncated = truncate_str(normalized);
        key_buffer.extend_from_slice(truncated.as_bytes());

        let mut obkv = KvWriterDelAdd::memory();
        obkv.insert(side, original)?;
        let bytes = obkv.into_inner()?;
        fid_docid_facet_strings_sorter.insert(&key_buffer, bytes)?;
    }

    Ok(())
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

/// Extracts the facet values of a JSON field.
fn extract_facet_values(value: &Value, geo_field: bool) -> FilterableValues {
    fn inner_extract_facet_values(
        value: &Value,
        can_recurse: bool,
        output_numbers: &mut Vec<f64>,
        output_strings: &mut Vec<(String, String)>,
        geo_field: bool,
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
                // if we're working on a geofield it MUST be something we can parse or else there was an internal error
                // in the enrich pipeline. But since the enrich pipeline worked, we want to avoid crashing at all costs.
                if geo_field {
                    if let Ok(float) = original.parse() {
                        output_numbers.push(float);
                    } else {
                        tracing::warn!(
                            "Internal error, could not parse a geofield that has been validated. Please open an issue."
                        )
                    }
                }
                let normalized = crate::normalize_facet(original);
                output_strings.push((normalized, original.clone()));
            }
            Value::Array(values) => {
                if can_recurse {
                    for value in values {
                        inner_extract_facet_values(
                            value,
                            false,
                            output_numbers,
                            output_strings,
                            geo_field,
                        );
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
            inner_extract_facet_values(otherwise, true, &mut numbers, &mut strings, geo_field);
            FilterableValues::Values { numbers, strings }
        }
    }
}

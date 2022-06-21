use std::io::{Read, Seek};
use std::iter;
use std::result::Result as StdResult;

use serde_json::Value;

use crate::documents::{DocumentsBatchIndex, DocumentsBatchReader, EnrichedDocumentsBatchReader};
use crate::error::{GeoError, InternalError, UserError};
use crate::update::index_documents::{obkv_to_object, writer_into_reader};
use crate::{FieldId, Index, Object, Result};

/// The symbol used to define levels in a nested primary key.
const PRIMARY_KEY_SPLIT_SYMBOL: char = '.';

/// The default primary that is used when not specified.
const DEFAULT_PRIMARY_KEY: &str = "id";

/// This function validates and enrich the documents by checking that:
///  - we can infer a primary key,
///  - all the documents id exist and are extracted,
///  - the validity of them but also,
///  - the validity of the `_geo` field depending on the settings.
pub fn validate_and_enrich_documents_batch<R: Read + Seek>(
    rtxn: &heed::RoTxn,
    index: &Index,
    autogenerate_docids: bool,
    reader: DocumentsBatchReader<R>,
) -> Result<StdResult<EnrichedDocumentsBatchReader<R>, UserError>> {
    let mut cursor = reader.into_cursor();
    let mut documents_batch_index = cursor.documents_batch_index().clone();
    let mut external_ids = tempfile::tempfile().map(grenad::Writer::new)?;

    // The primary key *field id* that has already been set for this index or the one
    // we will guess by searching for the first key that contains "id" as a substring.
    let primary_key = match index.primary_key(rtxn)? {
        Some(primary_key) if primary_key.contains(PRIMARY_KEY_SPLIT_SYMBOL) => {
            PrimaryKey::nested(primary_key)
        }
        Some(primary_key) => match documents_batch_index.id(primary_key) {
            Some(id) => PrimaryKey::flat(primary_key, id),
            None if autogenerate_docids => {
                PrimaryKey::flat(primary_key, documents_batch_index.insert(primary_key))
            }
            None => {
                return match cursor.next_document()? {
                    Some(first_document) => Ok(Err(UserError::MissingDocumentId {
                        primary_key: primary_key.to_string(),
                        document: obkv_to_object(&first_document, &documents_batch_index)?,
                    })),
                    None => Ok(Err(UserError::MissingPrimaryKey)),
                };
            }
        },
        None => {
            let guessed = documents_batch_index
                .iter()
                .filter(|(_, name)| name.to_lowercase().contains(DEFAULT_PRIMARY_KEY))
                .min_by_key(|(fid, _)| *fid);
            match guessed {
                Some((id, name)) => PrimaryKey::flat(name.as_str(), *id),
                None if autogenerate_docids => PrimaryKey::flat(
                    DEFAULT_PRIMARY_KEY,
                    documents_batch_index.insert(DEFAULT_PRIMARY_KEY),
                ),
                None => return Ok(Err(UserError::MissingPrimaryKey)),
            }
        }
    };

    // If the settings specifies that a _geo field must be used therefore we must check the
    // validity of it in all the documents of this batch and this is when we return `Some`.
    let geo_field_id = match documents_batch_index.id("_geo") {
        Some(geo_field_id) if index.sortable_fields(rtxn)?.contains("_geo") => Some(geo_field_id),
        _otherwise => None,
    };

    let mut count = 0;
    while let Some(document) = cursor.next_document()? {
        let document_id = match fetch_document_id(
            &document,
            &documents_batch_index,
            primary_key,
            autogenerate_docids,
            count,
        )? {
            Ok(document_id) => document_id,
            Err(user_error) => return Ok(Err(user_error)),
        };

        external_ids.insert(count.to_be_bytes(), &document_id)?;

        if let Some(geo_value) = geo_field_id.and_then(|fid| document.get(fid)) {
            if let Err(user_error) = validate_geo_from_json(Value::from(document_id), geo_value)? {
                return Ok(Err(UserError::from(user_error)));
            }
        }
        count += 1;
    }

    let external_ids = writer_into_reader(external_ids)?;
    let reader = EnrichedDocumentsBatchReader::new(
        cursor.into_reader(),
        primary_key.primary_key().to_string(),
        external_ids,
    )?;

    Ok(Ok(reader))
}

/// Retrieve the document id after validating it, returning a `UserError`
/// if the id is invalid or can't be guessed.
fn fetch_document_id(
    document: &obkv::KvReader<FieldId>,
    documents_batch_index: &DocumentsBatchIndex,
    primary_key: PrimaryKey,
    autogenerate_docids: bool,
    count: u32,
) -> Result<StdResult<String, UserError>> {
    match primary_key {
        PrimaryKey::Flat { name: primary_key, field_id: primary_key_id } => {
            match document.get(primary_key_id) {
                Some(document_id_bytes) => {
                    let document_id = serde_json::from_slice(document_id_bytes)
                        .map_err(InternalError::SerdeJson)?;
                    match validate_document_id_value(document_id)? {
                        Ok(document_id) => Ok(Ok(document_id)),
                        Err(user_error) => Ok(Err(user_error)),
                    }
                }
                None if autogenerate_docids => {
                    Ok(Ok(format!("{{auto-generated id of the {}nth document}}", count)))
                }
                None => Ok(Err(UserError::MissingDocumentId {
                    primary_key: primary_key.to_string(),
                    document: obkv_to_object(&document, &documents_batch_index)?,
                })),
            }
        }
        nested @ PrimaryKey::Nested { .. } => {
            let mut matching_documents_ids = Vec::new();
            for (first_level_name, right) in nested.possible_level_names() {
                if let Some(field_id) = documents_batch_index.id(first_level_name) {
                    if let Some(value_bytes) = document.get(field_id) {
                        let object = serde_json::from_slice(value_bytes)
                            .map_err(InternalError::SerdeJson)?;
                        fetch_matching_values(object, right, &mut matching_documents_ids);

                        if matching_documents_ids.len() >= 2 {
                            return Ok(Err(UserError::TooManyDocumentIds {
                                primary_key: nested.primary_key().to_string(),
                                document: obkv_to_object(&document, &documents_batch_index)?,
                            }));
                        }
                    }
                }
            }

            match matching_documents_ids.pop() {
                Some(document_id) => match validate_document_id_value(document_id)? {
                    Ok(document_id) => Ok(Ok(document_id)),
                    Err(user_error) => Ok(Err(user_error)),
                },
                None => Ok(Err(UserError::MissingDocumentId {
                    primary_key: nested.primary_key().to_string(),
                    document: obkv_to_object(&document, &documents_batch_index)?,
                })),
            }
        }
    }
}

/// A type that represent the type of primary key that has been set
/// for this index, a classic flat one or a nested one.
#[derive(Debug, Clone, Copy)]
enum PrimaryKey<'a> {
    Flat { name: &'a str, field_id: FieldId },
    Nested { name: &'a str },
}

impl PrimaryKey<'_> {
    fn flat(name: &str, field_id: FieldId) -> PrimaryKey {
        PrimaryKey::Flat { name, field_id }
    }

    fn nested(name: &str) -> PrimaryKey {
        PrimaryKey::Nested { name }
    }

    fn primary_key(&self) -> &str {
        match self {
            PrimaryKey::Flat { name, .. } => name,
            PrimaryKey::Nested { name } => name,
        }
    }

    /// Returns an `Iterator` that gives all the possible fields names the primary key
    /// can have depending of the first level name and deepnes of the objects.
    fn possible_level_names(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        let name = self.primary_key();
        iter::successors(Some((name, "")), |(curr, _)| curr.rsplit_once(PRIMARY_KEY_SPLIT_SYMBOL))
    }
}

fn contained_in(selector: &str, key: &str) -> bool {
    selector.starts_with(key)
        && selector[key.len()..]
            .chars()
            .next()
            .map(|c| c == PRIMARY_KEY_SPLIT_SYMBOL)
            .unwrap_or(true)
}

pub fn fetch_matching_values(value: Value, selector: &str, output: &mut Vec<Value>) {
    match value {
        Value::Object(object) => fetch_matching_values_in_object(object, selector, "", output),
        otherwise => output.push(otherwise),
    }
}

pub fn fetch_matching_values_in_object(
    object: Object,
    selector: &str,
    base_key: &str,
    output: &mut Vec<Value>,
) {
    for (key, value) in object {
        let base_key = if base_key.is_empty() {
            key.to_string()
        } else {
            format!("{}{}{}", base_key, PRIMARY_KEY_SPLIT_SYMBOL, key)
        };

        // here if the user only specified `doggo` we need to iterate in all the fields of `doggo`
        // so we check the contained_in on both side.
        let should_continue =
            contained_in(selector, &base_key) || contained_in(&base_key, selector);

        if should_continue {
            match value {
                Value::Object(object) => {
                    fetch_matching_values_in_object(object, selector, &base_key, output)
                }
                value => output.push(value),
            }
        }
    }
}

/// Returns a trimmed version of the document id or `None` if it is invalid.
pub fn validate_document_id(document_id: &str) -> Option<&str> {
    let document_id = document_id.trim();
    if !document_id.is_empty()
        && document_id.chars().all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_'))
    {
        Some(document_id)
    } else {
        None
    }
}

/// Parses a Json encoded document id and validate it, returning a user error when it is one.
pub fn validate_document_id_value(document_id: Value) -> Result<StdResult<String, UserError>> {
    match document_id {
        Value::String(string) => match validate_document_id(&string) {
            Some(s) if s.len() == string.len() => Ok(Ok(string)),
            Some(s) => Ok(Ok(s.to_string())),
            None => Ok(Err(UserError::InvalidDocumentId { document_id: Value::String(string) })),
        },
        Value::Number(number) if number.is_i64() => Ok(Ok(number.to_string())),
        content => Ok(Err(UserError::InvalidDocumentId { document_id: content.clone() })),
    }
}

/// Try to extract an `f64` from a JSON `Value` and return the `Value`
/// in the `Err` variant if it failed.
pub fn extract_float_from_value(value: Value) -> StdResult<f64, Value> {
    match value {
        Value::Number(ref n) => n.as_f64().ok_or(value),
        Value::String(ref s) => s.parse::<f64>().map_err(|_| value),
        value => Err(value),
    }
}

pub fn validate_geo_from_json(document_id: Value, bytes: &[u8]) -> Result<StdResult<(), GeoError>> {
    match serde_json::from_slice(bytes).map_err(InternalError::SerdeJson)? {
        Value::Object(mut object) => match (object.remove("lat"), object.remove("lng")) {
            (Some(lat), Some(lng)) => {
                match (extract_float_from_value(lat), extract_float_from_value(lng)) {
                    (Ok(_), Ok(_)) => Ok(Ok(())),
                    (Err(value), Ok(_)) => Ok(Err(GeoError::BadLatitude { document_id, value })),
                    (Ok(_), Err(value)) => Ok(Err(GeoError::BadLongitude { document_id, value })),
                    (Err(lat), Err(lng)) => {
                        Ok(Err(GeoError::BadLatitudeAndLongitude { document_id, lat, lng }))
                    }
                }
            }
            (None, Some(_)) => Ok(Err(GeoError::MissingLatitude { document_id })),
            (Some(_), None) => Ok(Err(GeoError::MissingLongitude { document_id })),
            (None, None) => Ok(Err(GeoError::MissingLatitudeAndLongitude { document_id })),
        },
        value => Ok(Err(GeoError::NotAnObject { document_id, value })),
    }
}

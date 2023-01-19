use std::io::{Read, Seek};
use std::result::Result as StdResult;
use std::{fmt, iter};

use serde::{Deserialize, Serialize};
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
///
/// # Panics
///
/// - if reader.is_empty(), this function may panic in some cases
pub fn enrich_documents_batch<R: Read + Seek>(
    rtxn: &heed::RoTxn,
    index: &Index,
    autogenerate_docids: bool,
    reader: DocumentsBatchReader<R>,
) -> Result<StdResult<EnrichedDocumentsBatchReader<R>, UserError>> {
    let (mut cursor, mut documents_batch_index) = reader.into_cursor_and_fields_index();

    let mut external_ids = tempfile::tempfile().map(grenad::Writer::new)?;
    let mut uuid_buffer = [0; uuid::fmt::Hyphenated::LENGTH];

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
                    None => unreachable!("Called with reader.is_empty()"),
                };
            }
        },
        None => {
            let mut guesses: Vec<(u16, &str)> = documents_batch_index
                .iter()
                .filter(|(_, name)| name.to_lowercase().ends_with(DEFAULT_PRIMARY_KEY))
                .map(|(field_id, name)| (*field_id, name.as_str()))
                .collect();

            // sort the keys in a deterministic, obvious way, so that fields are always in the same order.
            guesses.sort_by(|(_, left_name), (_, right_name)| {
                // shortest name first
                left_name.len().cmp(&right_name.len()).then_with(
                    // then alphabetical order
                    || left_name.cmp(right_name),
                )
            });

            match guesses.as_slice() {
                [] if autogenerate_docids => PrimaryKey::flat(
                    DEFAULT_PRIMARY_KEY,
                    documents_batch_index.insert(DEFAULT_PRIMARY_KEY),
                ),
                [] => return Ok(Err(UserError::NoPrimaryKeyCandidateFound)),
                [(field_id, name)] => {
                    log::info!("Primary key was not specified in index. Inferred to '{name}'");
                    PrimaryKey::flat(name, *field_id)
                }
                multiple => {
                    return Ok(Err(UserError::MultiplePrimaryKeyCandidatesFound {
                        candidates: multiple
                            .iter()
                            .map(|(_, candidate)| candidate.to_string())
                            .collect(),
                    }));
                }
            }
        }
    };

    // If the settings specifies that a _geo field must be used therefore we must check the
    // validity of it in all the documents of this batch and this is when we return `Some`.
    let geo_field_id = match documents_batch_index.id("_geo") {
        Some(geo_field_id)
            if index.sortable_fields(rtxn)?.contains("_geo")
                || index.filterable_fields(rtxn)?.contains("_geo") =>
        {
            Some(geo_field_id)
        }
        _otherwise => None,
    };

    let mut count = 0;
    while let Some(document) = cursor.next_document()? {
        let document_id = match fetch_or_generate_document_id(
            &document,
            &documents_batch_index,
            primary_key,
            autogenerate_docids,
            &mut uuid_buffer,
            count,
        )? {
            Ok(document_id) => document_id,
            Err(user_error) => return Ok(Err(user_error)),
        };

        if let Some(geo_value) = geo_field_id.and_then(|fid| document.get(fid)) {
            if let Err(user_error) = validate_geo_from_json(&document_id, geo_value)? {
                return Ok(Err(UserError::from(user_error)));
            }
        }

        let document_id = serde_json::to_vec(&document_id).map_err(InternalError::SerdeJson)?;
        external_ids.insert(count.to_be_bytes(), document_id)?;

        count += 1;
    }

    let external_ids = writer_into_reader(external_ids)?;
    let primary_key_name = primary_key.name().to_string();
    let reader = EnrichedDocumentsBatchReader::new(
        DocumentsBatchReader::new(cursor, documents_batch_index),
        primary_key_name,
        external_ids,
    )?;

    Ok(Ok(reader))
}

/// Retrieve the document id after validating it, returning a `UserError`
/// if the id is invalid or can't be guessed.
fn fetch_or_generate_document_id(
    document: &obkv::KvReader<FieldId>,
    documents_batch_index: &DocumentsBatchIndex,
    primary_key: PrimaryKey,
    autogenerate_docids: bool,
    uuid_buffer: &mut [u8; uuid::fmt::Hyphenated::LENGTH],
    count: u32,
) -> Result<StdResult<DocumentId, UserError>> {
    match primary_key {
        PrimaryKey::Flat { name: primary_key, field_id: primary_key_id } => {
            match document.get(primary_key_id) {
                Some(document_id_bytes) => {
                    let document_id = serde_json::from_slice(document_id_bytes)
                        .map_err(InternalError::SerdeJson)?;
                    match validate_document_id_value(document_id)? {
                        Ok(document_id) => Ok(Ok(DocumentId::retrieved(document_id))),
                        Err(user_error) => Ok(Err(user_error)),
                    }
                }
                None if autogenerate_docids => {
                    let uuid = uuid::Uuid::new_v4().as_hyphenated().encode_lower(uuid_buffer);
                    Ok(Ok(DocumentId::generated(uuid.to_string(), count)))
                }
                None => Ok(Err(UserError::MissingDocumentId {
                    primary_key: primary_key.to_string(),
                    document: obkv_to_object(document, documents_batch_index)?,
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
                                primary_key: nested.name().to_string(),
                                document: obkv_to_object(document, documents_batch_index)?,
                            }));
                        }
                    }
                }
            }

            match matching_documents_ids.pop() {
                Some(document_id) => match validate_document_id_value(document_id)? {
                    Ok(document_id) => Ok(Ok(DocumentId::retrieved(document_id))),
                    Err(user_error) => Ok(Err(user_error)),
                },
                None => Ok(Err(UserError::MissingDocumentId {
                    primary_key: nested.name().to_string(),
                    document: obkv_to_object(document, documents_batch_index)?,
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

    fn name(&self) -> &str {
        match self {
            PrimaryKey::Flat { name, .. } => name,
            PrimaryKey::Nested { name } => name,
        }
    }

    /// Returns an `Iterator` that gives all the possible fields names the primary key
    /// can have depending of the first level name and deepnes of the objects.
    fn possible_level_names(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        let name = self.name();
        name.match_indices(PRIMARY_KEY_SPLIT_SYMBOL)
            .map(move |(i, _)| (&name[..i], &name[i + PRIMARY_KEY_SPLIT_SYMBOL.len_utf8()..]))
            .chain(iter::once((name, "")))
    }
}

/// A type that represents a document id that has been retrieved from a document or auto-generated.
///
/// In case the document id has been auto-generated, the document nth is kept to help
/// users debug if there is an issue with the document itself.
#[derive(Serialize, Deserialize, Clone)]
pub enum DocumentId {
    Retrieved { value: String },
    Generated { value: String, document_nth: u32 },
}

impl DocumentId {
    fn retrieved(value: String) -> DocumentId {
        DocumentId::Retrieved { value }
    }

    fn generated(value: String, document_nth: u32) -> DocumentId {
        DocumentId::Generated { value, document_nth }
    }

    fn debug(&self) -> String {
        format!("{:?}", self)
    }

    pub fn is_generated(&self) -> bool {
        matches!(self, DocumentId::Generated { .. })
    }

    pub fn value(&self) -> &str {
        match self {
            DocumentId::Retrieved { value } => value,
            DocumentId::Generated { value, .. } => value,
        }
    }
}

impl fmt::Debug for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocumentId::Retrieved { value } => write!(f, "{:?}", value),
            DocumentId::Generated { value, document_nth } => {
                write!(f, "{{{:?}}} of the {}nth document", value, document_nth)
            }
        }
    }
}

fn starts_with(selector: &str, key: &str) -> bool {
    selector.strip_prefix(key).map_or(false, |tail| {
        tail.chars().next().map(|c| c == PRIMARY_KEY_SPLIT_SYMBOL).unwrap_or(true)
    })
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

        if starts_with(selector, &base_key) {
            match value {
                Value::Object(object) => {
                    fetch_matching_values_in_object(object, selector, &base_key, output)
                }
                value => output.push(value),
            }
        }
    }
}

pub fn validate_document_id(document_id: &str) -> Option<&str> {
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
        content => Ok(Err(UserError::InvalidDocumentId { document_id: content })),
    }
}

/// Try to extract an `f64` from a JSON `Value` and return the `Value`
/// in the `Err` variant if it failed.
pub fn extract_finite_float_from_value(value: Value) -> StdResult<f64, Value> {
    let number = match value {
        Value::Number(ref n) => match n.as_f64() {
            Some(number) => number,
            None => return Err(value),
        },
        Value::String(ref s) => match s.parse::<f64>() {
            Ok(number) => number,
            Err(_) => return Err(value),
        },
        value => return Err(value),
    };

    if number.is_finite() {
        Ok(number)
    } else {
        Err(value)
    }
}

pub fn validate_geo_from_json(id: &DocumentId, bytes: &[u8]) -> Result<StdResult<(), GeoError>> {
    use GeoError::*;
    let debug_id = || {
        serde_json::from_slice(id.value().as_bytes()).unwrap_or_else(|_| Value::from(id.debug()))
    };
    match serde_json::from_slice(bytes).map_err(InternalError::SerdeJson)? {
        Value::Object(mut object) => match (object.remove("lat"), object.remove("lng")) {
            (Some(lat), Some(lng)) => {
                match (extract_finite_float_from_value(lat), extract_finite_float_from_value(lng)) {
                    (Ok(_), Ok(_)) => Ok(Ok(())),
                    (Err(value), Ok(_)) => Ok(Err(BadLatitude { document_id: debug_id(), value })),
                    (Ok(_), Err(value)) => Ok(Err(BadLongitude { document_id: debug_id(), value })),
                    (Err(lat), Err(lng)) => {
                        Ok(Err(BadLatitudeAndLongitude { document_id: debug_id(), lat, lng }))
                    }
                }
            }
            (None, Some(_)) => Ok(Err(MissingLatitude { document_id: debug_id() })),
            (Some(_), None) => Ok(Err(MissingLongitude { document_id: debug_id() })),
            (None, None) => Ok(Err(MissingLatitudeAndLongitude { document_id: debug_id() })),
        },
        value => Ok(Err(NotAnObject { document_id: debug_id(), value })),
    }
}

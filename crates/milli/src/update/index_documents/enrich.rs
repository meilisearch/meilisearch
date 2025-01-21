use std::fmt;
use std::io::{BufWriter, Read, Seek};
use std::result::Result as StdResult;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::constants::RESERVED_GEO_FIELD_NAME;
use crate::documents::{
    DocumentIdExtractionError, DocumentsBatchIndex, DocumentsBatchReader,
    EnrichedDocumentsBatchReader, PrimaryKey, DEFAULT_PRIMARY_KEY,
};
use crate::error::{GeoError, InternalError, UserError};
use crate::update::index_documents::{obkv_to_object, writer_into_reader};
use crate::{FieldId, Index, Result};

/// This function validates and enrich the documents by checking that:
///  - we can infer a primary key,
///  - all the documents id exist and are extracted,
///  - the validity of them but also,
///  - the validity of the `_geo` field depending on the settings.
///
/// # Panics
///
/// - if reader.is_empty(), this function may panic in some cases
#[tracing::instrument(level = "trace", skip_all, target = "indexing::documents")]
pub fn enrich_documents_batch<R: Read + Seek>(
    rtxn: &heed::RoTxn<'_>,
    index: &Index,
    autogenerate_docids: bool,
    reader: DocumentsBatchReader<R>,
) -> Result<StdResult<EnrichedDocumentsBatchReader<R>, UserError>> {
    let (mut cursor, mut documents_batch_index) = reader.into_cursor_and_fields_index();

    let mut external_ids = tempfile::tempfile().map(BufWriter::new).map(grenad::Writer::new)?;
    let mut uuid_buffer = [0; uuid::fmt::Hyphenated::LENGTH];

    // The primary key *field id* that has already been set for this index or the one
    // we will guess by searching for the first key that contains "id" as a substring.
    let primary_key = match index.primary_key(rtxn)? {
        Some(primary_key) => match PrimaryKey::new(primary_key, &documents_batch_index) {
            Some(primary_key) => primary_key,
            None if autogenerate_docids => PrimaryKey::Flat {
                name: primary_key,
                field_id: documents_batch_index.insert(primary_key),
            },
            None => {
                return match cursor.next_document()? {
                    Some(first_document) => Ok(Err(UserError::MissingDocumentId {
                        primary_key: primary_key.to_string(),
                        document: obkv_to_object(first_document, &documents_batch_index)?,
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
                [] if autogenerate_docids => PrimaryKey::Flat {
                    name: DEFAULT_PRIMARY_KEY,
                    field_id: documents_batch_index.insert(DEFAULT_PRIMARY_KEY),
                },
                [] => return Ok(Err(UserError::NoPrimaryKeyCandidateFound)),
                [(field_id, name)] => {
                    tracing::info!("Primary key was not specified in index. Inferred to '{name}'");
                    PrimaryKey::Flat { name, field_id: *field_id }
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
    let geo_field_id = match documents_batch_index.id(RESERVED_GEO_FIELD_NAME) {
        Some(geo_field_id)
            if index.sortable_fields(rtxn)?.contains(RESERVED_GEO_FIELD_NAME)
                || index.filterable_fields(rtxn)?.contains(RESERVED_GEO_FIELD_NAME) =>
        {
            Some(geo_field_id)
        }
        _otherwise => None,
    };

    let mut count = 0;
    while let Some(document) = cursor.next_document()? {
        let document_id = match fetch_or_generate_document_id(
            document,
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
#[tracing::instrument(level = "trace", skip(uuid_buffer, documents_batch_index, document)
target = "indexing::documents")]
fn fetch_or_generate_document_id(
    document: &obkv::KvReader<FieldId>,
    documents_batch_index: &DocumentsBatchIndex,
    primary_key: PrimaryKey<'_>,
    autogenerate_docids: bool,
    uuid_buffer: &mut [u8; uuid::fmt::Hyphenated::LENGTH],
    count: u32,
) -> Result<StdResult<DocumentId, UserError>> {
    Ok(match primary_key.document_id(document, documents_batch_index)? {
        Ok(document_id) => Ok(DocumentId::Retrieved { value: document_id }),
        Err(DocumentIdExtractionError::InvalidDocumentId(user_error)) => Err(user_error),
        Err(DocumentIdExtractionError::MissingDocumentId) if autogenerate_docids => {
            let uuid = uuid::Uuid::new_v4().as_hyphenated().encode_lower(uuid_buffer);
            Ok(DocumentId::Generated { value: uuid.to_string(), document_nth: count })
        }
        Err(DocumentIdExtractionError::MissingDocumentId) => Err(UserError::MissingDocumentId {
            primary_key: primary_key.name().to_string(),
            document: obkv_to_object(document, documents_batch_index)?,
        }),
        Err(DocumentIdExtractionError::TooManyDocumentIds(_)) => {
            Err(UserError::TooManyDocumentIds {
                primary_key: primary_key.name().to_string(),
                document: obkv_to_object(document, documents_batch_index)?,
            })
        }
    })
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
                    (Ok(_), Ok(_)) if !object.is_empty() => Ok(Err(UnexpectedExtraFields {
                        document_id: debug_id(),
                        value: object.into(),
                    })),
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
        Value::Null => Ok(Ok(())),
        value => Ok(Err(NotAnObject { document_id: debug_id(), value })),
    }
}

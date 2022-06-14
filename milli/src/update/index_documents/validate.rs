use std::io::{Read, Seek};
use std::result::Result as StdResult;

use serde_json::Value;

use crate::error::{GeoError, InternalError, UserError};
use crate::update::index_documents::{obkv_to_object, DocumentsBatchReader};
use crate::{Index, Result};

/// This function validates a documents by checking that:
///  - we can infer a primary key,
///  - all the documents id exist and,
///  - the validity of them but also,
///  - the validity of the `_geo` field depending on the settings.
pub fn validate_documents_batch<R: Read + Seek>(
    rtxn: &heed::RoTxn,
    index: &Index,
    reader: DocumentsBatchReader<R>,
) -> Result<StdResult<DocumentsBatchReader<R>, UserError>> {
    let mut cursor = reader.into_cursor();
    let documents_batch_index = cursor.documents_batch_index().clone();

    // The primary key *field id* that has already been set for this index or the one
    // we will guess by searching for the first key that contains "id" as a substring.
    let (primary_key, primary_key_id) = match index.primary_key(rtxn)? {
        Some(primary_key) => match documents_batch_index.id(primary_key) {
            Some(id) => (primary_key, id),
            None => {
                return match cursor.next_document()? {
                    Some(first_document) => Ok(Err(UserError::MissingDocumentId {
                        primary_key: primary_key.to_string(),
                        document: obkv_to_object(&first_document, &documents_batch_index)?,
                    })),
                    // If there is no document in this batch the best we can do is to return this error.
                    None => Ok(Err(UserError::MissingPrimaryKey)),
                };
            }
        },
        None => {
            let guessed = documents_batch_index
                .iter()
                .filter(|(_, name)| name.contains("id"))
                .min_by_key(|(fid, _)| *fid);
            match guessed {
                Some((id, name)) => (name.as_str(), *id),
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

    while let Some(document) = cursor.next_document()? {
        let document_id = match document.get(primary_key_id) {
            Some(document_id_bytes) => match validate_document_id_from_json(document_id_bytes)? {
                Ok(document_id) => document_id,
                Err(user_error) => return Ok(Err(user_error)),
            },
            None => {
                return Ok(Err(UserError::MissingDocumentId {
                    primary_key: primary_key.to_string(),
                    document: obkv_to_object(&document, &documents_batch_index)?,
                }))
            }
        };

        if let Some(geo_value) = geo_field_id.and_then(|fid| document.get(fid)) {
            if let Err(user_error) = validate_geo_from_json(Value::from(document_id), geo_value)? {
                return Ok(Err(UserError::from(user_error)));
            }
        }
    }

    Ok(Ok(cursor.into_reader()))
}

/// Returns a trimmed version of the document id or `None` if it is invalid.
pub fn validate_document_id(document_id: &str) -> Option<&str> {
    let id = document_id.trim();
    if !id.is_empty()
        && id.chars().all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_'))
    {
        Some(id)
    } else {
        None
    }
}

/// Parses a Json encoded document id and validate it, returning a user error when it is one.
pub fn validate_document_id_from_json(bytes: &[u8]) -> Result<StdResult<String, UserError>> {
    match serde_json::from_slice(bytes).map_err(InternalError::SerdeJson)? {
        Value::String(string) => match validate_document_id(&string) {
            Some(s) if s.len() == string.len() => Ok(Ok(string)),
            Some(s) => Ok(Ok(s.to_string())),
            None => {
                return Ok(Err(UserError::InvalidDocumentId { document_id: Value::String(string) }))
            }
        },
        Value::Number(number) => Ok(Ok(number.to_string())),
        content => return Ok(Err(UserError::InvalidDocumentId { document_id: content.clone() })),
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
    let result = match serde_json::from_slice(bytes).map_err(InternalError::SerdeJson)? {
        Value::Object(mut object) => match (object.remove("lat"), object.remove("lng")) {
            (Some(lat), Some(lng)) => {
                match (extract_float_from_value(lat), extract_float_from_value(lng)) {
                    (Ok(_), Ok(_)) => Ok(()),
                    (Err(value), Ok(_)) => Err(GeoError::BadLatitude { document_id, value }),
                    (Ok(_), Err(value)) => Err(GeoError::BadLongitude { document_id, value }),
                    (Err(lat), Err(lng)) => {
                        Err(GeoError::BadLatitudeAndLongitude { document_id, lat, lng })
                    }
                }
            }
            (None, Some(_)) => Err(GeoError::MissingLatitude { document_id }),
            (Some(_), None) => Err(GeoError::MissingLongitude { document_id }),
            (None, None) => Err(GeoError::MissingLatitudeAndLongitude { document_id }),
        },
        value => Err(GeoError::NotAnObject { document_id, value }),
    };

    Ok(result)
}

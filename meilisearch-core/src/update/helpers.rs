use std::fmt::Write as _;

use indexmap::IndexMap;
use meilisearch_schema::IndexedPos;
use meilisearch_types::DocumentId;
use ordered_float::OrderedFloat;
use serde_json::Value;

use crate::Number;
use crate::raw_indexer::RawIndexer;
use crate::serde::SerializerError;
use crate::store::DiscoverIds;

/// Returns the number of words indexed or `None` if the type is unindexable.
pub fn index_value<A>(
    indexer: &mut RawIndexer<A>,
    document_id: DocumentId,
    indexed_pos: IndexedPos,
    value: &Value,
) -> Option<usize>
where A: AsRef<[u8]>,
{
    println!("indexing value: {}", value);
    match value {
        Value::Null => None,
        Value::Bool(boolean) => {
            let text = boolean.to_string();
            let number_of_words = indexer.index_text(document_id, indexed_pos, &text);
            Some(number_of_words)
        },
        Value::Number(number) => {
            let text = number.to_string();
            Some(indexer.index_text(document_id, indexed_pos, &text))
        },
        Value::String(string) => {
            Some(indexer.index_text(document_id, indexed_pos, &string))
        },
        Value::Array(_) => {
            let text = value_to_string(value);
            Some(indexer.index_text(document_id, indexed_pos, &text))
        },
        Value::Object(_) => {
            let text = value_to_string(value);
            Some(indexer.index_text(document_id, indexed_pos, &text))
        },
    }
}

/// Transforms the JSON Value type into a String.
pub fn value_to_string(value: &Value) -> String {
    fn internal_value_to_string(string: &mut String, value: &Value) {
        match value {
            Value::Null => (),
            Value::Bool(boolean) => { let _ = write!(string, "{}", &boolean); },
            Value::Number(number) => { let _ = write!(string, "{}", &number); },
            Value::String(text) => string.push_str(&text),
            Value::Array(array) => {
                for value in array {
                    internal_value_to_string(string, value);
                    let _ = string.write_str(". ");
                }
            },
            Value::Object(object) => {
                for (key, value) in object {
                    string.push_str(key);
                    let _ = string.write_str(". ");
                    internal_value_to_string(string, value);
                    let _ = string.write_str(". ");
                }
            },
        }
    }

    let mut string = String::new();
    internal_value_to_string(&mut string, value);
    string
}

/// Transforms the JSON Value type into a Number.
pub fn value_to_number(value: &Value) -> Option<Number> {
    use std::str::FromStr;

    match value {
        Value::Null => None,
        Value::Bool(boolean) => Some(Number::Unsigned(*boolean as u64)),
        Value::Number(number) => {
            match (number.as_i64(), number.as_u64(), number.as_f64()) {
                (Some(n), _, _) => Some(Number::Signed(n)),
                (_, Some(n), _) => Some(Number::Unsigned(n)),
                (_, _, Some(n)) => Some(Number::Float(OrderedFloat(n))),
                (None, None, None) => None,
            }
        },
        Value::String(string) => Number::from_str(string).ok(),
        Value::Array(_array) => None,
        Value::Object(_object) => None,
    }
}

/// Validates a string representation to be a correct document id and returns
/// the corresponding id or generate a new one, this is the way we produce documents ids.
pub fn discover_document_id<F>(
    docid: &str,
    external_docids_get: F,
    available_docids: &mut DiscoverIds<'_>,
) -> Result<DocumentId, SerializerError>
where
    F: FnOnce(&str) -> Option<u32>
{
    if docid.chars().all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_') {
        match external_docids_get(docid) {
            Some(id) => Ok(DocumentId(id)),
            None => {
                let internal_id = available_docids.next().expect("no more ids available");
                Ok(internal_id)
            },
        }
    } else {
        Err(SerializerError::InvalidDocumentIdFormat)
    }
}

/// Extracts and validates the document id of a document.
pub fn extract_document_id<F>(
    primary_key: &str,
    document: &IndexMap<String, Value>,
    external_docids_get: F,
    available_docids: &mut DiscoverIds<'_>,
) -> Result<(DocumentId, String), SerializerError>
where
    F: FnOnce(&str) -> Option<u32>
{
    match document.get(primary_key) {
        Some(value) => {
            let docid = match value {
                Value::Number(number) => number.to_string(),
                Value::String(string) => string.clone(),
                _ => return Err(SerializerError::InvalidDocumentIdFormat),
            };
            discover_document_id(&docid, external_docids_get, available_docids).map(|id| (id, docid))
        }
        None => Err(SerializerError::DocumentIdNotFound),
    }
}

mod builder;
mod enriched;
mod reader;
mod serde_impl;

use std::fmt::{self, Debug};
use std::io;
use std::str::Utf8Error;

use bimap::BiHashMap;
pub use builder::DocumentsBatchBuilder;
pub use enriched::{EnrichedDocument, EnrichedDocumentsBatchCursor, EnrichedDocumentsBatchReader};
use obkv::KvReader;
pub use reader::{DocumentsBatchCursor, DocumentsBatchCursorError, DocumentsBatchReader};
use serde::{Deserialize, Serialize};

use crate::error::{FieldIdMapMissingEntry, InternalError};
use crate::{FieldId, Object, Result};

/// The key that is used to store the `DocumentsBatchIndex` datastructure,
/// it is the absolute last key of the list.
const DOCUMENTS_BATCH_INDEX_KEY: [u8; 8] = u64::MAX.to_be_bytes();

/// Helper function to convert an obkv reader into a JSON object.
pub fn obkv_to_object(obkv: &KvReader<FieldId>, index: &DocumentsBatchIndex) -> Result<Object> {
    obkv.iter()
        .map(|(field_id, value)| {
            let field_name = index
                .name(field_id)
                .ok_or(FieldIdMapMissingEntry::FieldId { field_id, process: "obkv_to_object" })?;
            let value = serde_json::from_slice(value).map_err(InternalError::SerdeJson)?;
            Ok((field_name.to_string(), value))
        })
        .collect()
}

/// A bidirectional map that links field ids to their name in a document batch.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct DocumentsBatchIndex(pub BiHashMap<FieldId, String>);

impl DocumentsBatchIndex {
    /// Insert the field in the map, or return it's field id if it doesn't already exists.
    pub fn insert(&mut self, field: &str) -> FieldId {
        match self.0.get_by_right(field) {
            Some(field_id) => *field_id,
            None => {
                let field_id = self.0.len() as FieldId;
                self.0.insert(field_id, field.to_string());
                field_id
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> bimap::hash::Iter<FieldId, String> {
        self.0.iter()
    }

    pub fn name(&self, id: FieldId) -> Option<&str> {
        self.0.get_by_left(&id).map(AsRef::as_ref)
    }

    pub fn id(&self, name: &str) -> Option<FieldId> {
        self.0.get_by_right(name).cloned()
    }

    pub fn recreate_json(&self, document: &obkv::KvReaderU16) -> Result<Object> {
        let mut map = Object::new();

        for (k, v) in document.iter() {
            // TODO: TAMO: update the error type
            let key =
                self.0.get_by_left(&k).ok_or(crate::error::InternalError::DatabaseClosing)?.clone();
            let value = serde_json::from_slice::<serde_json::Value>(v)
                .map_err(crate::error::InternalError::SerdeJson)?;
            map.insert(key, value);
        }

        Ok(map)
    }
}

#[derive(Debug)]
pub enum Error {
    ParseFloat { error: std::num::ParseFloatError, line: usize, value: String },
    InvalidDocumentFormat,
    InvalidEnrichedData,
    InvalidUtf8(Utf8Error),
    Csv(csv::Error),
    Json(serde_json::Error),
    Serialize(serde_json::Error),
    Grenad(grenad::Error),
    Io(io::Error),
}

impl From<csv::Error> for Error {
    fn from(e: csv::Error) -> Self {
        Self::Csv(e)
    }
}

impl From<io::Error> for Error {
    fn from(other: io::Error) -> Self {
        Self::Io(other)
    }
}

impl From<serde_json::Error> for Error {
    fn from(other: serde_json::Error) -> Self {
        Self::Json(other)
    }
}

impl From<grenad::Error> for Error {
    fn from(other: grenad::Error) -> Self {
        Self::Grenad(other)
    }
}

impl From<Utf8Error> for Error {
    fn from(other: Utf8Error) -> Self {
        Self::InvalidUtf8(other)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ParseFloat { error, line, value } => {
                write!(f, "Error parsing number {:?} at line {}: {}", value, line, error)
            }
            Error::InvalidDocumentFormat => {
                f.write_str("Invalid document addition format, missing the documents batch index.")
            }
            Error::InvalidEnrichedData => f.write_str("Invalid enriched data."),
            Error::InvalidUtf8(e) => write!(f, "{}", e),
            Error::Io(e) => write!(f, "{}", e),
            Error::Serialize(e) => write!(f, "{}", e),
            Error::Grenad(e) => write!(f, "{}", e),
            Error::Csv(e) => write!(f, "{}", e),
            Error::Json(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
pub fn objects_from_json_value(json: serde_json::Value) -> Vec<crate::Object> {
    let documents = match json {
        object @ serde_json::Value::Object(_) => vec![object],
        serde_json::Value::Array(objects) => objects,
        invalid => {
            panic!("an array of objects must be specified, {:#?} is not an array", invalid)
        }
    };
    let mut objects = vec![];
    for document in documents {
        let object = match document {
            serde_json::Value::Object(object) => object,
            invalid => panic!("an object must be specified, {:#?} is not an object", invalid),
        };
        objects.push(object);
    }
    objects
}

/// Macro used to generate documents, with the same syntax as `serde_json::json`
#[cfg(test)]
macro_rules! documents {
    ($data:tt) => {{
        let documents = serde_json::json!($data);
        let documents = $crate::documents::objects_from_json_value(documents);
        $crate::documents::documents_batch_reader_from_objects(documents)
    }};
}

#[cfg(test)]
pub fn documents_batch_reader_from_objects(
    objects: impl IntoIterator<Item = Object>,
) -> DocumentsBatchReader<std::io::Cursor<Vec<u8>>> {
    let mut builder = DocumentsBatchBuilder::new(Vec::new());
    for object in objects {
        builder.append_json_object(&object).unwrap();
    }
    let vector = builder.into_inner().unwrap();
    DocumentsBatchReader::from_reader(std::io::Cursor::new(vector)).unwrap()
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn create_documents_no_errors() {
        let value = json!({
            "number": 1,
            "string": "this is a field",
            "array": ["an", "array"],
            "object": {
                "key": "value",
            },
            "bool": true
        });

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_json_object(value.as_object().unwrap()).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut documents, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        assert_eq!(index.iter().count(), 5);
        let reader = documents.next_document().unwrap().unwrap();
        assert_eq!(reader.iter().count(), 5);
        assert!(documents.next_document().unwrap().is_none());
    }

    #[test]
    fn test_add_multiple_documents() {
        let doc1 = json!({
            "bool": true,
        });
        let doc2 = json!({
            "toto": false,
        });

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_json_object(doc1.as_object().unwrap()).unwrap();
        builder.append_json_object(doc2.as_object().unwrap()).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut documents, index) = DocumentsBatchReader::from_reader(io::Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();
        assert_eq!(index.iter().count(), 2);
        let reader = documents.next_document().unwrap().unwrap();
        assert_eq!(reader.iter().count(), 1);
        assert!(documents.next_document().unwrap().is_some());
        assert!(documents.next_document().unwrap().is_none());
    }

    #[test]
    fn test_nested() {
        let docs_reader = documents!([{
            "hello": {
                "toto": ["hello"]
            }
        }]);

        let (mut cursor, _) = docs_reader.into_cursor_and_fields_index();
        let doc = cursor.next_document().unwrap().unwrap();
        let nested: Value = serde_json::from_slice(doc.get(0).unwrap()).unwrap();
        assert_eq!(nested, json!({ "toto": ["hello"] }));
    }

    #[test]
    fn out_of_order_json_fields() {
        let _documents = documents!([
            {"id": 1,"b": 0},
            {"id": 2,"a": 0,"b": 0},
        ]);
    }

    #[test]
    fn out_of_order_csv_fields() {
        let csv1_content = "id:number,b\n1,0";
        let csv1 = csv::Reader::from_reader(Cursor::new(csv1_content));

        let csv2_content = "id:number,a,b\n2,0,0";
        let csv2 = csv::Reader::from_reader(Cursor::new(csv2_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv1).unwrap();
        builder.append_csv(csv2).unwrap();
        let vector = builder.into_inner().unwrap();

        DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap();
    }
}

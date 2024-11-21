mod builder;
mod enriched;
mod primary_key;
mod reader;
mod serde_impl;

use std::fmt::Debug;
use std::io;
use std::str::Utf8Error;

use bimap::BiHashMap;
pub use builder::DocumentsBatchBuilder;
pub use enriched::{EnrichedDocument, EnrichedDocumentsBatchCursor, EnrichedDocumentsBatchReader};
use obkv::KvReader;
pub use primary_key::{
    validate_document_id_str, validate_document_id_value, DocumentIdExtractionError, FieldIdMapper,
    PrimaryKey, DEFAULT_PRIMARY_KEY,
};
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

    pub fn iter(&self) -> bimap::hash::Iter<'_, FieldId, String> {
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

impl FieldIdMapper for DocumentsBatchIndex {
    fn id(&self, name: &str) -> Option<FieldId> {
        self.id(name)
    }

    fn name(&self, id: FieldId) -> Option<&str> {
        self.name(id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error parsing number {value:?} at line {line}: {error}")]
    ParseFloat { error: std::num::ParseFloatError, line: usize, value: String },
    #[error("Error parsing boolean {value:?} at line {line}: {error}")]
    ParseBool { error: std::str::ParseBoolError, line: usize, value: String },
    #[error("Invalid document addition format, missing the documents batch index.")]
    InvalidDocumentFormat,
    #[error("Invalid enriched data.")]
    InvalidEnrichedData,
    #[error(transparent)]
    InvalidUtf8(#[from] Utf8Error),
    #[error(transparent)]
    Csv(#[from] csv::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Serialize(serde_json::Error),
    #[error(transparent)]
    Grenad(#[from] grenad::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

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
        let mut file = tempfile::tempfile().unwrap();

        match documents {
            serde_json::Value::Array(vec) => {
                for document in vec {
                    serde_json::to_writer(&mut file, &document).unwrap();
                }
            }
            serde_json::Value::Object(document) => {
                serde_json::to_writer(&mut file, &document).unwrap();
            }
            _ => unimplemented!("The `documents!` macro only support Objects and Array"),
        }
        file.sync_all().unwrap();
        unsafe { memmap2::Mmap::map(&file).unwrap() }
    }};
}

pub fn mmap_from_objects(objects: impl IntoIterator<Item = Object>) -> memmap2::Mmap {
    let mut writer = tempfile::tempfile().map(std::io::BufWriter::new).unwrap();
    for object in objects {
        serde_json::to_writer(&mut writer, &object).unwrap();
    }
    let file = writer.into_inner().unwrap();
    unsafe { memmap2::Mmap::map(&file).unwrap() }
}

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

    use serde_json::json;

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
    fn out_of_order_json_fields() {
        let _documents = documents!([
            {"id": 1,"b": 0},
            {"id": 2,"a": 0,"b": 0},
        ]);
    }
}

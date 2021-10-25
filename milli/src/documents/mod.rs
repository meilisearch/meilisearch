mod builder;
/// The documents module defines an intermediary document format that milli uses for indexation, and
/// provides an API to easily build and read such documents.
///
/// The `DocumentBatchBuilder` interface allows to write batches of documents to a writer, that can
/// later be read by milli using the `DocumentBatchReader` interface.
mod reader;
mod serde;

use std::num::ParseFloatError;
use std::io;
use std::fmt::{self, Debug};

use ::serde::{Deserialize, Serialize};
use bimap::BiHashMap;
pub use builder::DocumentBatchBuilder;
pub use reader::DocumentBatchReader;

use crate::FieldId;

/// A bidirectional map that links field ids to their name in a document batch.
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct DocumentsBatchIndex(pub BiHashMap<FieldId, String>);

impl DocumentsBatchIndex {
    /// Insert the field in the map, or return it's field id if it doesn't already exists.
    pub fn insert(&mut self, field:  &str) -> FieldId {
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

    pub fn iter(&self) -> impl Iterator<Item=(&FieldId, &String)> {
        self.0.iter()
    }

    pub fn name(&self, id: FieldId) -> Option<&String> {
        self.0.get_by_left(&id)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DocumentsMetadata {
    count: usize,
    index: DocumentsBatchIndex,
}

pub struct ByteCounter<W> {
    count: usize,
    writer: W,
}

impl<W> ByteCounter<W> {
    fn new(writer: W) -> Self {
        Self { count: 0, writer }
    }
}

impl<W: io::Write> io::Write for ByteCounter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.writer.write(buf)?;
        self.count += count;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Debug)]
pub enum Error {
    ParseFloat(std::num::ParseFloatError),
    InvalidDocumentFormat,
    Custom(String),
    JsonError(serde_json::Error),
    CsvError(csv::Error),
    Serialize(bincode::Error),
    Io(io::Error),
    DocumentTooLarge,
}

impl From<csv::Error> for Error {
    fn from(e: csv::Error) -> Self {
        Self::CsvError(e)
    }
}

impl From<io::Error> for Error {
    fn from(other: io::Error) -> Self {
        Self::Io(other)
    }
}

impl From<bincode::Error> for Error {
    fn from(other: bincode::Error) -> Self {
        Self::Serialize(other)
    }
}

impl From<serde_json::Error> for Error {
    fn from(other: serde_json::Error) -> Self {
        Self::JsonError(other)
    }
}

impl From<ParseFloatError> for Error {
    fn from(other: ParseFloatError) -> Self {
        Self::ParseFloat(other)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ParseFloat(e) => write!(f, "{}", e),
            Error::Custom(s) => write!(f, "Unexpected serialization error: {}", s),
            Error::InvalidDocumentFormat => f.write_str("Invalid document addition format."),
            Error::JsonError(err) => write!(f, "Couldn't serialize document value: {}", err),
            Error::Io(e) => write!(f, "{}", e),
            Error::DocumentTooLarge => f.write_str("Provided document is too large (>2Gib)"),
            Error::Serialize(e) => write!(f, "{}", e),
            Error::CsvError(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}

/// Macro used to generate documents, with the same syntax as `serde_json::json`
#[cfg(test)]
macro_rules! documents {
    ($data:tt) => {{
        let documents = serde_json::json!($data);
        let mut writer = std::io::Cursor::new(Vec::new());
        let mut builder = crate::documents::DocumentBatchBuilder::new(&mut writer).unwrap();
        let documents = serde_json::to_vec(&documents).unwrap();
        builder.extend_from_json(std::io::Cursor::new(documents)).unwrap();
        builder.finish().unwrap();

        writer.set_position(0);

        crate::documents::DocumentBatchReader::from_reader(writer).unwrap()
    }};
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn create_documents_no_errors() {
        let json = json!({
            "number": 1,
            "string": "this is a field",
            "array": ["an", "array"],
            "object": {
                "key": "value",
            },
            "bool": true
        });

        let json = serde_json::to_vec(&json).unwrap();

        let mut v = Vec::new();
        let mut cursor = io::Cursor::new(&mut v);

        let mut builder = DocumentBatchBuilder::new(&mut cursor).unwrap();

        builder.extend_from_json(Cursor::new(json)).unwrap();

        builder.finish().unwrap();

        let mut documents =
            DocumentBatchReader::from_reader(io::Cursor::new(cursor.into_inner())).unwrap();

        assert_eq!(documents.index().iter().count(), 5);

        let reader = documents.next_document_with_index().unwrap().unwrap();

        assert_eq!(reader.1.iter().count(), 5);
        assert!(documents.next_document_with_index().unwrap().is_none());
    }

    #[test]
    fn test_add_multiple_documents() {
        let doc1 = json!({
            "bool": true,
        });
        let doc2 = json!({
            "toto": false,
        });

        let doc1 = serde_json::to_vec(&doc1).unwrap();
        let doc2 = serde_json::to_vec(&doc2).unwrap();

        let mut v = Vec::new();
        let mut cursor = io::Cursor::new(&mut v);

        let mut builder = DocumentBatchBuilder::new(&mut cursor).unwrap();

        builder.extend_from_json(Cursor::new(doc1)).unwrap();
        builder.extend_from_json(Cursor::new(doc2)).unwrap();

        builder.finish().unwrap();

        let mut documents =
            DocumentBatchReader::from_reader(io::Cursor::new(cursor.into_inner())).unwrap();

        assert_eq!(documents.index().iter().count(), 2);

        let reader = documents.next_document_with_index().unwrap().unwrap();

        assert_eq!(reader.1.iter().count(), 1);
        assert!(documents.next_document_with_index().unwrap().is_some());
        assert!(documents.next_document_with_index().unwrap().is_none());
    }

    #[test]
    fn add_documents_array() {
        let docs = json!([
            { "toto": false },
            { "tata": "hello" },
        ]);

        let docs = serde_json::to_vec(&docs).unwrap();

        let mut v = Vec::new();
        let mut cursor = io::Cursor::new(&mut v);

        let mut builder = DocumentBatchBuilder::new(&mut cursor).unwrap();

        builder.extend_from_json(Cursor::new(docs)).unwrap();

        builder.finish().unwrap();

        let mut documents =
            DocumentBatchReader::from_reader(io::Cursor::new(cursor.into_inner())).unwrap();

        assert_eq!(documents.index().iter().count(), 2);

        let reader = documents.next_document_with_index().unwrap().unwrap();

        assert_eq!(reader.1.iter().count(), 1);
        assert!(documents.next_document_with_index().unwrap().is_some());
        assert!(documents.next_document_with_index().unwrap().is_none());
    }

    #[test]
    fn add_invalid_document_format() {
        let mut v = Vec::new();
        let mut cursor = io::Cursor::new(&mut v);

        let mut builder = DocumentBatchBuilder::new(&mut cursor).unwrap();

        let docs = json!([[
            { "toto": false },
            { "tata": "hello" },
        ]]);

        let docs = serde_json::to_vec(&docs).unwrap();
        assert!(builder.extend_from_json(Cursor::new(docs)).is_err());

        let docs = json!("hello");
        let docs = serde_json::to_vec(&docs).unwrap();

        assert!(builder.extend_from_json(Cursor::new(docs)).is_err());
    }

    #[test]
    fn test_nested() {
        let mut docs = documents!([{
            "hello": {
                "toto": ["hello"]
            }
        }]);

        let (_index, doc) = docs.next_document_with_index().unwrap().unwrap();

        let nested: Value = serde_json::from_slice(doc.get(0).unwrap()).unwrap();
        assert_eq!(nested, json!({ "toto": ["hello"] }));
    }

    #[test]
    fn out_of_order_fields() {
        let _documents = documents!([
            {"id": 1,"b": 0},
            {"id": 2,"a": 0,"b": 0},
        ]);
    }
}

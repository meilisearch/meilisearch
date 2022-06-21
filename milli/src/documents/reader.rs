use std::convert::TryInto;
use std::{error, fmt, io};

use obkv::KvReader;

use super::{DocumentsBatchIndex, Error, DOCUMENTS_BATCH_INDEX_KEY};
use crate::FieldId;

/// The `DocumentsBatchReader` provides a way to iterate over documents that have been created with
/// a `DocumentsBatchWriter`.
///
/// The documents are returned in the form of `obkv::Reader` where each field is identified with a
/// `FieldId`. The mapping between the field ids and the field names is done thanks to the index.
pub struct DocumentsBatchReader<R> {
    cursor: grenad::ReaderCursor<R>,
    fields_index: DocumentsBatchIndex,
}

impl<R: io::Read + io::Seek> DocumentsBatchReader<R> {
    /// Construct a `DocumentsReader` from a reader.
    ///
    /// It first retrieves the index, then moves to the first document. Use the `into_cursor`
    /// method to iterator over the documents, from the first to the last.
    pub fn from_reader(reader: R) -> Result<Self, Error> {
        let reader = grenad::Reader::new(reader)?;
        let mut cursor = reader.into_cursor()?;

        let fields_index = match cursor.move_on_key_equal_to(DOCUMENTS_BATCH_INDEX_KEY)? {
            Some((_, value)) => serde_json::from_slice(value).map_err(Error::Serialize)?,
            None => return Err(Error::InvalidDocumentFormat),
        };

        Ok(DocumentsBatchReader { cursor, fields_index })
    }

    pub fn documents_count(&self) -> u32 {
        self.cursor.len().saturating_sub(1).try_into().expect("Invalid number of documents")
    }

    pub fn is_empty(&self) -> bool {
        self.cursor.len().saturating_sub(1) == 0
    }

    pub fn documents_batch_index(&self) -> &DocumentsBatchIndex {
        &self.fields_index
    }

    /// This method returns a forward cursor over the documents.
    pub fn into_cursor(self) -> DocumentsBatchCursor<R> {
        let DocumentsBatchReader { cursor, fields_index } = self;
        let mut cursor = DocumentsBatchCursor { cursor, fields_index };
        cursor.reset();
        cursor
    }
}

/// A forward cursor over the documents in a `DocumentsBatchReader`.
pub struct DocumentsBatchCursor<R> {
    cursor: grenad::ReaderCursor<R>,
    fields_index: DocumentsBatchIndex,
}

impl<R> DocumentsBatchCursor<R> {
    pub fn into_reader(self) -> DocumentsBatchReader<R> {
        let DocumentsBatchCursor { cursor, fields_index, .. } = self;
        DocumentsBatchReader { cursor, fields_index }
    }

    pub fn documents_batch_index(&self) -> &DocumentsBatchIndex {
        &self.fields_index
    }

    /// Resets the cursor to be able to read from the start again.
    pub fn reset(&mut self) {
        self.cursor.reset();
    }
}

impl<R: io::Read + io::Seek> DocumentsBatchCursor<R> {
    /// Returns the next document, starting from the first one. Subsequent calls to
    /// `next_document` advance the document reader until all the documents have been read.
    pub fn next_document(
        &mut self,
    ) -> Result<Option<KvReader<FieldId>>, DocumentsBatchCursorError> {
        match self.cursor.move_on_next()? {
            Some((key, value)) if key != DOCUMENTS_BATCH_INDEX_KEY => {
                Ok(Some(KvReader::new(value)))
            }
            _otherwise => Ok(None),
        }
    }
}

/// The possible error thrown by the `DocumentsBatchCursor` when iterating on the documents.
#[derive(Debug)]
pub enum DocumentsBatchCursorError {
    Grenad(grenad::Error),
    SerdeJson(serde_json::Error),
}

impl From<grenad::Error> for DocumentsBatchCursorError {
    fn from(error: grenad::Error) -> DocumentsBatchCursorError {
        DocumentsBatchCursorError::Grenad(error)
    }
}

impl From<serde_json::Error> for DocumentsBatchCursorError {
    fn from(error: serde_json::Error) -> DocumentsBatchCursorError {
        DocumentsBatchCursorError::SerdeJson(error)
    }
}

impl error::Error for DocumentsBatchCursorError {}

impl fmt::Display for DocumentsBatchCursorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DocumentsBatchCursorError::Grenad(e) => e.fmt(f),
            DocumentsBatchCursorError::SerdeJson(e) => e.fmt(f),
        }
    }
}

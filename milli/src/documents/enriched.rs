use std::fs::File;
use std::io::{BufRead, BufReader};
use std::{error, fmt, io, str};

use obkv::KvReader;

use super::{DocumentsBatchIndex, DocumentsBatchReader};
use crate::update::DocumentId;
use crate::{FieldId, Object};

/// The `EnrichedDocumentsBatchReader` provides a way to iterate over documents that have
/// been created with a `DocumentsBatchWriter` and, for the enriched data,
/// a simple `grenad::Reader<File>`.
///
/// The documents are returned in the form of `obkv::Reader` where each field is identified with a
/// `FieldId`. The mapping between the field ids and the field names is done thanks to the index.
pub struct EnrichedDocumentsBatchReader<R: io::BufRead> {
    documents: DocumentsBatchReader<R>,
    primary_key: String,
    external_ids: grenad::ReaderCursor<BufReader<File>>,
}

impl<R: io::BufRead> EnrichedDocumentsBatchReader<R> {
    pub fn new(
        documents: DocumentsBatchReader<R>,
        primary_key: String,
        external_ids: grenad::Reader<BufReader<File>>,
    ) -> Result<Self, grenad::Error> {
        Ok(EnrichedDocumentsBatchReader {
            documents,
            primary_key,
            external_ids: external_ids.into_cursor()?,
        })
    }

    pub fn documents_count(&self) -> u64 {
        self.external_ids.len()
    }

    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }

    pub fn is_empty(&self) -> bool {
        self.external_ids.is_empty()
    }

    /// This method returns a forward cursor over the enriched documents.
    pub fn into_cursor(self) -> EnrichedDocumentsBatchCursor<R> {
        let EnrichedDocumentsBatchReader { documents, primary_key, mut external_ids } = self;
        external_ids.reset();
        EnrichedDocumentsBatchCursor { documents, primary_key, external_ids }
    }
}

#[derive(Debug, Clone)]
pub struct EnrichedDocument {
    pub document: Object,
    pub document_id: DocumentId,
}

pub struct EnrichedDocumentsBatchCursor<R: BufRead> {
    documents: DocumentsBatchReader<R>,
    primary_key: String,
    external_ids: grenad::ReaderCursor<BufReader<File>>,
}

impl<R: BufRead> EnrichedDocumentsBatchCursor<R> {
    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }

    // /// Resets the cursor to be able to read from the start again.
    // pub fn reset(&mut self) {
    //     self.documents.reset();
    //     self.external_ids.reset();
    // }
}

impl<R: io::BufRead> EnrichedDocumentsBatchCursor<R> {
    /// Returns the next document, starting from the first one. Subsequent calls to
    /// `next_document` advance the document reader until all the documents have been read.
    pub fn next_enriched_document(
        &mut self,
    ) -> Result<Option<EnrichedDocument>, DocumentsBatchCursorError> {
        let document = self.documents.next().transpose()?;
        let document_id = match self.external_ids.move_on_next()? {
            Some((_, bytes)) => serde_json::from_slice(bytes).map(Some)?,
            None => None,
        };

        match document.zip(document_id) {
            Some((document, document_id)) => Ok(Some(EnrichedDocument { document, document_id })),
            None => Ok(None),
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

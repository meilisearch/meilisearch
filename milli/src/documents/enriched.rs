use std::fs::File;
use std::{io, str};

use obkv::KvReader;

use super::{
    DocumentsBatchCursor, DocumentsBatchCursorError, DocumentsBatchIndex, DocumentsBatchReader,
    Error,
};
use crate::update::DocumentId;
use crate::FieldId;

/// The `EnrichedDocumentsBatchReader` provides a way to iterate over documents that have
/// been created with a `DocumentsBatchWriter` and, for the enriched data,
/// a simple `grenad::Reader<File>`.
///
/// The documents are returned in the form of `obkv::Reader` where each field is identified with a
/// `FieldId`. The mapping between the field ids and the field names is done thanks to the index.
pub struct EnrichedDocumentsBatchReader<R> {
    documents: DocumentsBatchReader<R>,
    primary_key: String,
    external_ids: grenad::ReaderCursor<File>,
}

impl<R: io::Read + io::Seek> EnrichedDocumentsBatchReader<R> {
    pub fn new(
        documents: DocumentsBatchReader<R>,
        primary_key: String,
        external_ids: grenad::Reader<File>,
    ) -> Result<Self, Error> {
        if documents.documents_count() as u64 == external_ids.len() {
            Ok(EnrichedDocumentsBatchReader {
                documents,
                primary_key,
                external_ids: external_ids.into_cursor()?,
            })
        } else {
            Err(Error::InvalidEnrichedData)
        }
    }

    pub fn documents_count(&self) -> u32 {
        self.documents.documents_count()
    }

    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn documents_batch_index(&self) -> &DocumentsBatchIndex {
        self.documents.documents_batch_index()
    }

    /// This method returns a forward cursor over the enriched documents.
    pub fn into_cursor_and_fields_index(
        self,
    ) -> (EnrichedDocumentsBatchCursor<R>, DocumentsBatchIndex) {
        let EnrichedDocumentsBatchReader { documents, primary_key, mut external_ids } = self;
        let (documents, fields_index) = documents.into_cursor_and_fields_index();
        external_ids.reset();
        (EnrichedDocumentsBatchCursor { documents, primary_key, external_ids }, fields_index)
    }
}

#[derive(Debug, Clone)]
pub struct EnrichedDocument<'a> {
    pub document: KvReader<'a, FieldId>,
    pub document_id: DocumentId,
}

pub struct EnrichedDocumentsBatchCursor<R> {
    documents: DocumentsBatchCursor<R>,
    primary_key: String,
    external_ids: grenad::ReaderCursor<File>,
}

impl<R> EnrichedDocumentsBatchCursor<R> {
    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }
    /// Resets the cursor to be able to read from the start again.
    pub fn reset(&mut self) {
        self.documents.reset();
        self.external_ids.reset();
    }
}

impl<R: io::Read + io::Seek> EnrichedDocumentsBatchCursor<R> {
    /// Returns the next document, starting from the first one. Subsequent calls to
    /// `next_document` advance the document reader until all the documents have been read.
    pub fn next_enriched_document(
        &mut self,
    ) -> Result<Option<EnrichedDocument>, DocumentsBatchCursorError> {
        let document = self.documents.next_document()?;
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

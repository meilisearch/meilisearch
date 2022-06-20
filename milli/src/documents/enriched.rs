use std::fs::File;
use std::{io, str};

use obkv::KvReader;

use super::{
    DocumentsBatchCursor, DocumentsBatchCursorError, DocumentsBatchIndex, DocumentsBatchReader,
    Error,
};
use crate::FieldId;

/// The `EnrichedDocumentsBatchReader` provides a way to iterate over documents that have
/// been created with a `DocumentsBatchWriter` and, for the enriched data,
/// a simple `grenad::Reader<File>`.
///
/// The documents are returned in the form of `obkv::Reader` where each field is identified with a
/// `FieldId`. The mapping between the field ids and the field names is done thanks to the index.
pub struct EnrichedDocumentsBatchReader<R> {
    documents: DocumentsBatchReader<R>,
    external_ids: grenad::ReaderCursor<File>,
}

impl<R: io::Read + io::Seek> EnrichedDocumentsBatchReader<R> {
    pub fn new(
        documents: DocumentsBatchReader<R>,
        external_ids: grenad::Reader<File>,
    ) -> Result<Self, Error> {
        if documents.documents_count() as u64 == external_ids.len() {
            Ok(EnrichedDocumentsBatchReader {
                documents,
                external_ids: external_ids.into_cursor()?,
            })
        } else {
            Err(Error::InvalidEnrichedData)
        }
    }

    pub fn documents_count(&self) -> u32 {
        self.documents.documents_count()
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn documents_batch_index(&self) -> &DocumentsBatchIndex {
        self.documents.documents_batch_index()
    }

    /// This method returns a forward cursor over the enriched documents.
    pub fn into_cursor(self) -> EnrichedDocumentsBatchCursor<R> {
        let EnrichedDocumentsBatchReader { documents, mut external_ids } = self;
        external_ids.reset();
        EnrichedDocumentsBatchCursor { documents: documents.into_cursor(), external_ids }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EnrichedDocument<'a> {
    pub document: KvReader<'a, FieldId>,
    pub external_id: &'a str,
}

pub struct EnrichedDocumentsBatchCursor<R> {
    documents: DocumentsBatchCursor<R>,
    external_ids: grenad::ReaderCursor<File>,
}

impl<R> EnrichedDocumentsBatchCursor<R> {
    pub fn into_reader(self) -> EnrichedDocumentsBatchReader<R> {
        let EnrichedDocumentsBatchCursor { documents, external_ids } = self;
        EnrichedDocumentsBatchReader { documents: documents.into_reader(), external_ids }
    }

    pub fn documents_batch_index(&self) -> &DocumentsBatchIndex {
        self.documents.documents_batch_index()
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
        let external_id = match self.external_ids.move_on_next()? {
            Some((_, bytes)) => Some(str::from_utf8(bytes)?),
            None => None,
        };

        match document.zip(external_id) {
            Some((document, external_id)) => Ok(Some(EnrichedDocument { document, external_id })),
            None => Ok(None),
        }
    }
}

use std::io;
use std::io::{BufReader, Read};
use std::mem::size_of;

use byteorder::{BigEndian, ReadBytesExt};
use obkv::KvReader;

use super::{DocumentsBatchIndex, DocumentsMetadata, Error};
use crate::FieldId;

/// The `DocumentsBatchReader` provides a way to iterate over documents that have been created with
/// a `DocumentsBatchWriter`.
///
/// The documents are returned in the form of `obkv::Reader` where each field is identified with a
/// `FieldId`. The mapping between the field ids and the field names is done thanks to the index.
pub struct DocumentBatchReader<R> {
    reader: BufReader<R>,
    metadata: DocumentsMetadata,
    buffer: Vec<u8>,
    seen_documents: usize,
}

impl<R: io::Read + io::Seek> DocumentBatchReader<R> {
    /// Construct a `DocumentsReader` from a reader.
    ///
    /// It first retrieves the index, then moves to the first document. Subsequent calls to
    /// `next_document` advance the document reader until all the documents have been read.
    pub fn from_reader(mut reader: R) -> Result<Self, Error> {
        let mut buffer = Vec::new();

        let meta_offset = reader.read_u64::<BigEndian>()?;
        reader.seek(io::SeekFrom::Start(meta_offset))?;
        reader.read_to_end(&mut buffer)?;
        let metadata: DocumentsMetadata = bincode::deserialize(&buffer)?;

        reader.seek(io::SeekFrom::Start(size_of::<u64>() as u64))?;
        buffer.clear();

        let reader = BufReader::new(reader);

        Ok(Self { reader, metadata, buffer, seen_documents: 0 })
    }

    /// Returns the next document in the reader, and wraps it in an `obkv::KvReader`, along with a
    /// reference to the addition index.
    pub fn next_document_with_index<'a>(
        &'a mut self,
    ) -> io::Result<Option<(&'a DocumentsBatchIndex, KvReader<'a, FieldId>)>> {
        if self.seen_documents < self.metadata.count {
            let doc_len = self.reader.read_u32::<BigEndian>()?;
            self.buffer.resize(doc_len as usize, 0);
            self.reader.read_exact(&mut self.buffer)?;
            self.seen_documents += 1;

            let reader = KvReader::new(&self.buffer);
            Ok(Some((&self.metadata.index, reader)))
        } else {
            Ok(None)
        }
    }

    /// Return the fields index for the documents batch.
    pub fn index(&self) -> &DocumentsBatchIndex {
        &self.metadata.index
    }

    /// Returns the number of documents in the reader.
    pub fn len(&self) -> usize {
        self.metadata.count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

use std::io;

use byteorder::{BigEndian, WriteBytesExt};
use serde::ser::Serialize;

use super::serde::DocumentSerializer;
use super::{ByteCounter, DocumentsBatchIndex, DocumentsMetadata, Error};

/// The `DocumentsBatchBuilder` provides a way to build a documents batch in the intermediary
/// format used by milli.
///
/// The writer used by the DocumentBatchBuilder can be read using a `DocumentBatchReader` to
/// iterate other the documents.
///
/// ## example:
/// ```
/// use milli::documents::DocumentBatchBuilder;
/// use serde_json::json;
/// use std::io::Cursor;
///
/// let mut writer = Cursor::new(Vec::new());
/// let mut builder = DocumentBatchBuilder::new(&mut writer).unwrap();
/// builder.add_documents(json!({"id": 1, "name": "foo"})).unwrap();
/// builder.finish().unwrap();
/// ```
pub struct DocumentBatchBuilder<W> {
    serializer: DocumentSerializer<W>,
}

impl<W: io::Write + io::Seek> DocumentBatchBuilder<W> {
    pub fn new(writer: W) -> Result<Self, Error> {
        let index = DocumentsBatchIndex::new();
        let mut writer = ByteCounter::new(writer);
        // add space to write the offset of the metadata at the end of the writer
        writer.write_u64::<BigEndian>(0)?;

        let serializer =
            DocumentSerializer { writer, buffer: Vec::new(), index, count: 0, allow_seq: true };

        Ok(Self { serializer })
    }

    /// Returns the number of documents that have been written to the builder.
    pub fn len(&self) -> usize {
        self.serializer.count
    }

    /// This method must be called after the document addition is terminated. It will put the
    /// metadata at the end of the file, and write the metadata offset at the beginning on the
    /// file.
    pub fn finish(self) -> Result<(), Error> {
        let DocumentSerializer {
            writer: ByteCounter { mut writer, count: offset },
            index,
            count,
            ..
        } = self.serializer;

        let meta = DocumentsMetadata { count, index };

        bincode::serialize_into(&mut writer, &meta)?;

        writer.seek(io::SeekFrom::Start(0))?;
        writer.write_u64::<BigEndian>(offset as u64)?;

        writer.flush()?;

        Ok(())
    }

    /// Adds documents to the builder.
    ///
    /// The internal index is updated with the fields found
    /// in the documents. Document must either be a map or a sequences of map, anything else will
    /// fail.
    pub fn add_documents<T: Serialize>(&mut self, document: T) -> Result<(), Error> {
        document.serialize(&mut self.serializer)?;
        Ok(())
    }
}

use std::io::{self, Write};

use grenad::{CompressionType, WriterBuilder};
use serde::de::Deserializer;
use serde_json::to_writer;

use super::{DocumentsBatchIndex, Error, DOCUMENTS_BATCH_INDEX_KEY};
use crate::documents::serde_impl::DocumentVisitor;
use crate::Object;

/// The `DocumentsBatchBuilder` provides a way to build a documents batch in the intermediary
/// format used by milli.
///
/// The writer used by the `DocumentsBatchBuilder` can be read using a `DocumentsBatchReader`
/// to iterate over the documents.
///
/// ## example:
/// ```
/// use serde_json::json;
/// use milli::documents::DocumentsBatchBuilder;
///
/// let json = json!({ "id": 1, "name": "foo" });
///
/// let mut builder = DocumentsBatchBuilder::new(Vec::new());
/// builder.append_json_object(json.as_object().unwrap()).unwrap();
/// let _vector = builder.into_inner().unwrap();
/// ```
pub struct DocumentsBatchBuilder<W> {
    /// The inner grenad writer, the last value must always be the `DocumentsBatchIndex`.
    writer: grenad::Writer<W>,
    /// A map that creates the relation between field ids and field names.
    fields_index: DocumentsBatchIndex,
    /// The number of documents that were added to this builder,
    /// it doesn't take the primary key of the documents into account at this point.
    documents_count: u32,

    /// A buffer to store a temporary obkv buffer and avoid reallocating.
    obkv_buffer: Vec<u8>,
    /// A buffer to serialize the values and avoid reallocating,
    /// serialized values are stored in an obkv.
    value_buffer: Vec<u8>,
}

impl<W: Write> DocumentsBatchBuilder<W> {
    pub fn new(writer: W) -> DocumentsBatchBuilder<W> {
        DocumentsBatchBuilder {
            writer: WriterBuilder::new().compression_type(CompressionType::None).build(writer),
            fields_index: DocumentsBatchIndex::default(),
            documents_count: 0,
            obkv_buffer: Vec::new(),
            value_buffer: Vec::new(),
        }
    }

    /// Returns the number of documents inserted into this builder.
    pub fn documents_count(&self) -> u32 {
        self.documents_count
    }

    /// Appends a new JSON object into the batch and updates the `DocumentsBatchIndex` accordingly.
    pub fn append_json_object(&mut self, object: &Object) -> io::Result<()> {
        // Make sure that we insert the fields ids in order as the obkv writer has this requirement.
        let mut fields_ids: Vec<_> = object.keys().map(|k| self.fields_index.insert(k)).collect();
        fields_ids.sort_unstable();

        self.obkv_buffer.clear();
        let mut writer = obkv::KvWriter::new(&mut self.obkv_buffer);
        for field_id in fields_ids {
            let key = self.fields_index.name(field_id).unwrap();
            self.value_buffer.clear();
            to_writer(&mut self.value_buffer, &object[key])?;
            writer.insert(field_id, &self.value_buffer)?;
        }

        let internal_id = self.documents_count.to_be_bytes();
        let document_bytes = writer.into_inner()?;
        self.writer.insert(internal_id, &document_bytes)?;
        self.documents_count += 1;

        Ok(())
    }

    /// Appends a new JSON array of objects into the batch and updates the `DocumentsBatchIndex` accordingly.
    pub fn append_json_array<R: io::Read>(&mut self, reader: R) -> Result<(), Error> {
        let mut de = serde_json::Deserializer::from_reader(reader);
        let mut visitor = DocumentVisitor::new(self);
        de.deserialize_any(&mut visitor)?
    }

    /// Flushes the content on disk and stores the final version of the `DocumentsBatchIndex`.
    pub fn into_inner(mut self) -> io::Result<W> {
        let DocumentsBatchBuilder { mut writer, fields_index, .. } = self;

        // We serialize and insert the `DocumentsBatchIndex` as the last key of the grenad writer.
        self.value_buffer.clear();
        to_writer(&mut self.value_buffer, &fields_index)?;
        writer.insert(DOCUMENTS_BATCH_INDEX_KEY, &self.value_buffer)?;

        writer.into_inner()
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;
    use crate::documents::DocumentsBatchReader;

    #[test]
    fn add_single_documents_json() {
        let json = serde_json::json!({
            "id": 1,
            "field": "hello!",
        });

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_json_object(json.as_object().unwrap()).unwrap();

        let json = serde_json::json!({
            "blabla": false,
            "field": "hello!",
            "id": 1,
        });

        builder.append_json_object(json.as_object().unwrap()).unwrap();

        assert_eq!(builder.documents_count(), 2);
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();
        assert_eq!(index.len(), 3);

        let document = cursor.next_document().unwrap().unwrap();
        assert_eq!(document.iter().count(), 2);

        let document = cursor.next_document().unwrap().unwrap();
        assert_eq!(document.iter().count(), 3);

        assert!(cursor.next_document().unwrap().is_none());
    }
}

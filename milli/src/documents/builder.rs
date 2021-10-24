use std::collections::BTreeMap;
use std::io;
use std::io::Cursor;
use std::io::Write;

use byteorder::{BigEndian, WriteBytesExt};
use serde::Deserializer;
use serde_json::Value;

use crate::FieldId;

use super::serde::DocumentVisitor;
use super::{ByteCounter, DocumentsBatchIndex, DocumentsMetadata, Error};

/// The `DocumentsBatchBuilder` provides a way to build a documents batch in the intermediary
/// format used by milli.
///
/// The writer used by the DocumentBatchBuilder can be read using a `DocumentBatchReader` to
/// iterate other the documents.
pub struct DocumentBatchBuilder<W> {
    inner: ByteCounter<W>,
    index: DocumentsBatchIndex,
    obkv_buffer: Vec<u8>,
    value_buffer: Vec<u8>,
    values: BTreeMap<FieldId, Value>,
    count: usize,
}

impl<W: io::Write + io::Seek> DocumentBatchBuilder<W> {
    pub fn new(writer: W) -> Result<Self, Error> {
        let index = DocumentsBatchIndex::default();
        let mut writer = ByteCounter::new(writer);
        // add space to write the offset of the metadata at the end of the writer
        writer.write_u64::<BigEndian>(0)?;

        let this = Self {
            inner: writer,
            index,
            obkv_buffer: Vec::new(),
            value_buffer: Vec::new(),
            values: BTreeMap::new(),
            count: 0,
        };

        Ok(this)
    }

    /// Returns the number of documents that have been written to the builder.
    pub fn len(&self) -> usize {
        self.count
    }

    /// This method must be called after the document addition is terminated. It will put the
    /// metadata at the end of the file, and write the metadata offset at the beginning on the
    /// file.
    pub fn finish(self) -> Result<(), Error> {
        let Self {
            inner: ByteCounter { mut writer, count: offset },
            index,
            count,
            ..
        } = self;

        let meta = DocumentsMetadata { count, index };

        bincode::serialize_into(&mut writer, &meta)?;

        writer.seek(io::SeekFrom::Start(0))?;
        writer.write_u64::<BigEndian>(offset as u64)?;

        writer.flush()?;

        Ok(())
    }


    /// Extends the builder with json documents from a reader.
    pub fn extend_from_json<R: io::Read>(&mut self, reader: R) -> Result<(), Error> {
        let mut de = serde_json::Deserializer::from_reader(reader);

        let mut visitor = DocumentVisitor {
            inner: &mut self.inner,
            index: &mut self.index,
            obkv_buffer: &mut self.obkv_buffer,
            value_buffer: &mut self.value_buffer,
            values: &mut self.values,
            count: &mut self.count,
        };

        de.deserialize_any(&mut visitor).map_err(Error::JsonError)?
    }

    /// Creates a builder from a reader of CSV documents.
    ///
    /// Since all fields in a csv documents are guaranteed to be ordered, we are able to perform
    /// optimisations, and extending from another CSV is not allowed.
    pub fn from_csv<R: io::Read>(reader: R, writer: W) -> Result<Self, Error> {

        let mut this = Self::new(writer)?;
        // Ensure that this is the first and only addition made with this builder
        debug_assert!(this.index.is_empty());

        let mut records = csv::Reader::from_reader(reader);

        let headers = records
            .headers()?
            .into_iter()
            .map(parse_csv_header)
            .map(|(k, t)| (this.index.insert(&k), t))
            .collect::<BTreeMap<_, _>>();

        let records = records.into_records();

        for record in records {
            match record {
                Ok(record) => {
                    let mut writer = obkv::KvWriter::new(Cursor::new(&mut this.obkv_buffer));
                    for (value, (fid, ty)) in record.into_iter().zip(headers.iter()) {
                        let value = match ty {
                            AllowedType::Number => value.parse::<f64>().map(Value::from)?,
                            AllowedType::String => Value::String(value.to_string()),
                        };

                        serde_json::to_writer(Cursor::new(&mut this.value_buffer), &value)?;
                        writer.insert(*fid, &this.value_buffer)?;
                        this.value_buffer.clear();
                    }

                    this.inner.write_u32::<BigEndian>(this.obkv_buffer.len() as u32)?;
                    this.inner.write_all(&this.obkv_buffer)?;

                    this.obkv_buffer.clear();
                    this.count += 1;
                },
                Err(_) => panic!(),
            }
        }

        Ok(this)
    }
}

#[derive(Debug)]
enum AllowedType {
    String,
    Number,
}

fn parse_csv_header(header: &str) -> (String, AllowedType) {
    // if there are several separators we only split on the last one.
    match header.rsplit_once(':') {
        Some((field_name, field_type)) => match field_type {
            "string" => (field_name.to_string(), AllowedType::String),
            "number" => (field_name.to_string(), AllowedType::Number), // if the pattern isn't reconized, we keep the whole field.
            _otherwise => (header.to_string(), AllowedType::String),
        },
        None => (header.to_string(), AllowedType::String),
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::documents::DocumentBatchReader;

    use super::*;

    #[test]
    fn add_single_documents_json() {
        let mut cursor = Cursor::new(Vec::new());
        let mut builder = DocumentBatchBuilder::new(&mut cursor).unwrap();

        let json = serde_json::json!({
            "id": 1,
            "field": "hello!",
        });

        builder.extend_from_json(Cursor::new(serde_json::to_vec(&json).unwrap())).unwrap();

        let json = serde_json::json!({
            "blabla": false,
            "field": "hello!",
            "id": 1,
        });

        builder.extend_from_json(Cursor::new(serde_json::to_vec(&json).unwrap())).unwrap();

        assert_eq!(builder.len(), 2);

        builder.finish().unwrap();

        cursor.set_position(0);

        let mut reader = DocumentBatchReader::from_reader(cursor).unwrap();

        let (index, document) = reader.next_document_with_index().unwrap().unwrap();
        assert_eq!(index.len(), 3);
        assert_eq!(document.iter().count(), 2);

        let (index, document) = reader.next_document_with_index().unwrap().unwrap();
        assert_eq!(index.len(), 3);
        assert_eq!(document.iter().count(), 3);

        assert!(reader.next_document_with_index().unwrap().is_none());
    }

    #[test]
    fn add_documents_seq_json() {
        let mut cursor = Cursor::new(Vec::new());
        let mut builder = DocumentBatchBuilder::new(&mut cursor).unwrap();

        let json = serde_json::json!([{
            "id": 1,
            "field": "hello!",
        },{
            "blabla": false,
            "field": "hello!",
            "id": 1,
        }
        ]);

        builder.extend_from_json(Cursor::new(serde_json::to_vec(&json).unwrap())).unwrap();

        assert_eq!(builder.len(), 2);

        builder.finish().unwrap();

        cursor.set_position(0);

        let mut reader = DocumentBatchReader::from_reader(cursor).unwrap();

        let (index, document) = reader.next_document_with_index().unwrap().unwrap();
        assert_eq!(index.len(), 3);
        assert_eq!(document.iter().count(), 2);

        let (index, document) = reader.next_document_with_index().unwrap().unwrap();
        assert_eq!(index.len(), 3);
        assert_eq!(document.iter().count(), 3);

        assert!(reader.next_document_with_index().unwrap().is_none());
    }

    #[test]
    fn add_documents_csv() {
        let mut cursor = Cursor::new(Vec::new());

        let csv = "id:number,field:string\n1,hello!\n2,blabla";

        let builder = DocumentBatchBuilder::from_csv(Cursor::new(csv.as_bytes()), &mut cursor).unwrap();
        builder.finish().unwrap();

        cursor.set_position(0);

        let mut reader = DocumentBatchReader::from_reader(cursor).unwrap();

        let (index, document) = reader.next_document_with_index().unwrap().unwrap();
        assert_eq!(index.len(), 2);
        assert_eq!(document.iter().count(), 2);

        let (_index, document) = reader.next_document_with_index().unwrap().unwrap();
        assert_eq!(document.iter().count(), 2);

        assert!(reader.next_document_with_index().unwrap().is_none());
    }
}

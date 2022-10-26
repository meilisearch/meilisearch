use std::io::{self, Write};

use grenad::{CompressionType, WriterBuilder};
use serde::de::Deserializer;
use serde_json::{to_writer, Value};

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

    /// Appends a new CSV file into the batch and updates the `DocumentsBatchIndex` accordingly.
    pub fn append_csv<R: io::Read>(&mut self, mut reader: csv::Reader<R>) -> Result<(), Error> {
        // Make sure that we insert the fields ids in order as the obkv writer has this requirement.
        let mut typed_fields_ids: Vec<_> = reader
            .headers()?
            .into_iter()
            .map(parse_csv_header)
            .map(|(k, t)| (self.fields_index.insert(k), t))
            .enumerate()
            .collect();
        // Make sure that we insert the fields ids in order as the obkv writer has this requirement.
        typed_fields_ids.sort_unstable_by_key(|(_, (fid, _))| *fid);

        let mut record = csv::StringRecord::new();
        let mut line = 0;
        while reader.read_record(&mut record)? {
            // We increment here and not at the end of the while loop to take
            // the header offset into account.
            line += 1;

            self.obkv_buffer.clear();
            let mut writer = obkv::KvWriter::new(&mut self.obkv_buffer);

            for (i, (field_id, type_)) in typed_fields_ids.iter() {
                self.value_buffer.clear();

                let value = &record[*i];
                match type_ {
                    AllowedType::Number => {
                        if value.trim().is_empty() {
                            to_writer(&mut self.value_buffer, &Value::Null)?;
                        } else if let Ok(integer) = value.trim().parse::<i64>() {
                            to_writer(&mut self.value_buffer, &integer)?;
                        } else {
                            match value.trim().parse::<f64>() {
                                Ok(float) => {
                                    to_writer(&mut self.value_buffer, &float)?;
                                }
                                Err(error) => {
                                    return Err(Error::ParseFloat {
                                        error,
                                        line,
                                        value: value.to_string(),
                                    });
                                }
                            }
                        }
                    }
                    AllowedType::String => {
                        if value.is_empty() {
                            to_writer(&mut self.value_buffer, &Value::Null)?;
                        } else {
                            to_writer(&mut self.value_buffer, value)?;
                        }
                    }
                }

                // We insert into the obkv writer the value buffer that has been filled just above.
                writer.insert(*field_id, &self.value_buffer)?;
            }

            let internal_id = self.documents_count.to_be_bytes();
            let document_bytes = writer.into_inner()?;
            self.writer.insert(internal_id, &document_bytes)?;
            self.documents_count += 1;
        }

        Ok(())
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

#[derive(Debug)]
enum AllowedType {
    String,
    Number,
}

fn parse_csv_header(header: &str) -> (&str, AllowedType) {
    // if there are several separators we only split on the last one.
    match header.rsplit_once(':') {
        Some((field_name, field_type)) => match field_type {
            "string" => (field_name, AllowedType::String),
            "number" => (field_name, AllowedType::Number),
            // if the pattern isn't reconized, we keep the whole field.
            _otherwise => (header, AllowedType::String),
        },
        None => (header, AllowedType::String),
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use serde_json::json;

    use super::*;
    use crate::documents::{obkv_to_object, DocumentsBatchReader};

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

    #[test]
    fn add_documents_csv() {
        let csv_content = "id:number,field:string\n1,hello!\n2,blabla";
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        assert_eq!(builder.documents_count(), 2);
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();
        assert_eq!(index.len(), 2);

        let document = cursor.next_document().unwrap().unwrap();
        assert_eq!(document.iter().count(), 2);

        let document = cursor.next_document().unwrap().unwrap();
        assert_eq!(document.iter().count(), 2);

        assert!(cursor.next_document().unwrap().is_none());
    }

    #[test]
    fn simple_csv_document() {
        let csv_content = r#"city,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();
        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );

        assert!(cursor.next_document().unwrap().is_none());
    }

    #[test]
    fn coma_in_field() {
        let csv_content = r#"city,country,pop
"Boston","United, States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                "city": "Boston",
                "country": "United, States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn quote_in_field() {
        let csv_content = r#"city,country,pop
"Boston","United"" States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                "city": "Boston",
                "country": "United\" States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn integer_in_field() {
        let csv_content = r#"city,country,pop:number
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": 4628910,
            })
        );
    }

    #[test]
    fn integer_as_id() {
        let csv_content = r#""id:number","title:string","comment:string"
"1239","Pride and Prejudice","A great book""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                "id": 1239,
                "title": "Pride and Prejudice",
                "comment": "A great book",
            })
        );
    }

    #[test]
    fn float_in_field() {
        let csv_content = r#"city,country,pop:number
"Boston","United States","4628910.01""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": 4628910.01,
            })
        );
    }

    #[test]
    fn several_colon_in_header() {
        let csv_content = r#"city:love:string,country:state,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                "city:love": "Boston",
                "country:state": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn ending_by_colon_in_header() {
        let csv_content = r#"city:,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                "city:": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn starting_by_colon_in_header() {
        let csv_content = r#":city,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                ":city": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[ignore]
    #[test]
    fn starting_by_colon_in_header2() {
        let csv_content = r#":string,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, _) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        assert!(cursor.next_document().is_err());
    }

    #[test]
    fn double_colon_in_header() {
        let csv_content = r#"city::string,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let (mut cursor, index) = DocumentsBatchReader::from_reader(Cursor::new(vector))
            .unwrap()
            .into_cursor_and_fields_index();

        let doc = cursor.next_document().unwrap().unwrap();
        let val = obkv_to_object(&doc, &index).map(Value::from).unwrap();

        assert_eq!(
            val,
            json!({
                "city:": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn bad_type_in_header() {
        let csv_content = r#"city,country:number,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        assert!(builder.append_csv(csv).is_err());
    }

    #[test]
    fn bad_column_count1() {
        let csv_content = r#"city,country,pop
"Boston","United States","4628910", "too much
        let csv = csv::Reader::from_reader(Cursor::new(csv_content"#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        assert!(builder.append_csv(csv).is_err());
    }

    #[test]
    fn bad_column_count2() {
        let csv_content = r#"city,country,pop
"Boston","United States""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        assert!(builder.append_csv(csv).is_err());
    }
}

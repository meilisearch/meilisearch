use std::collections::HashMap;
use std::io::{self, Write};

use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::Value;

use super::Error;
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
pub struct DocumentsBatchBuilder<W: Write> {
    /// The inner grenad writer, the last value must always be the `DocumentsBatchIndex`.
    writer: GzEncoder<W>,
    /// The number of documents that were added to this builder,
    /// it doesn't take the primary key of the documents into account at this point.
    documents_count: u32,
}

impl<W: Write> DocumentsBatchBuilder<W> {
    pub fn new(writer: W) -> DocumentsBatchBuilder<W> {
        DocumentsBatchBuilder {
            writer: GzEncoder::new(writer, Compression::default()),
            documents_count: 0,
        }
    }

    /// Returns the number of documents inserted into this builder.
    pub fn documents_count(&self) -> u32 {
        self.documents_count
    }

    /// Appends a new JSON object into the batch and updates the `DocumentsBatchIndex` accordingly.
    pub fn append_json_object(&mut self, object: &Object) -> io::Result<()> {
        serde_json::to_writer(&mut self.writer, object)?;
        self.documents_count += 1;
        Ok(())
    }

    /// Appends a new CSV file into the batch and updates the `DocumentsBatchIndex` accordingly.
    pub fn append_csv<R: io::Read>(&mut self, mut reader: csv::Reader<R>) -> Result<(), Error> {
        // Extract the name and the type from the header
        let typed_headers: Vec<_> = reader
            .headers()?
            .into_iter()
            .map(parse_csv_header)
            .map(|(s, t)| (s.to_owned(), t))
            .collect();

        let mut record = csv::StringRecord::new();
        let mut line: usize = 0;
        while reader.read_record(&mut record)? {
            // We increment here and not at the end of the while loop to take
            // the header offset into account.
            line += 1;

            let mut document = HashMap::<&str, Value>::default();
            for ((header, allowed_type), value) in typed_headers.iter().zip(record.iter()) {
                let trimmed_value = value.trim();
                match allowed_type {
                    AllowedType::Number => {
                        if trimmed_value.is_empty() {
                            document.insert(header, Value::Null);
                        } else if let Ok(integer) = trimmed_value.parse::<i64>() {
                            document.insert(header, integer.into());
                        } else {
                            match trimmed_value.parse::<f64>() {
                                Ok(float) => {
                                    document.insert(header, float.into());
                                }
                                Err(error) => {
                                    let value = value.to_string();
                                    return Err(Error::ParseFloat { error, line, value });
                                }
                            }
                        }
                    }
                    AllowedType::Boolean => {
                        if trimmed_value.is_empty() {
                            document.insert(header, Value::Null);
                        } else {
                            match trimmed_value.parse::<bool>() {
                                Ok(bool) => {
                                    document.insert(header, bool.into());
                                }
                                Err(error) => {
                                    let value = value.to_string();
                                    return Err(Error::ParseBool { error, line, value });
                                }
                            }
                        }
                    }
                    AllowedType::String => {
                        if value.is_empty() {
                            document.insert(header, Value::Null);
                        } else {
                            document.insert(header, value.into());
                        }
                    }
                }
            }

            // We insert into the JSON lines file the value buffer that has been filled just above
            serde_json::to_writer(&mut self.writer, &document)?;
            self.writer.write_all(&[b'\n'])?;
            self.documents_count += 1;
        }

        Ok(())
    }

    /// Flushes the content on disk and stores the final version of the `DocumentsBatchIndex`.
    pub fn into_inner(self) -> io::Result<W> {
        self.writer.finish()
    }
}

#[derive(Debug)]
enum AllowedType {
    String,
    Boolean,
    Number,
}

fn parse_csv_header(header: &str) -> (&str, AllowedType) {
    // if there are several separators we only split on the last one.
    match header.rsplit_once(':') {
        Some((field_name, field_type)) => match field_type {
            "string" => (field_name, AllowedType::String),
            "boolean" => (field_name, AllowedType::Boolean),
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

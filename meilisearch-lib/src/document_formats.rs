use std::fmt;
use std::io::{self, Read, Result as IoResult, Seek, Write};

use csv::{Reader as CsvReader, StringRecordsIntoIter};
use milli::documents::DocumentBatchBuilder;
use serde_json::{Deserializer, Map, Value};

type Result<T> = std::result::Result<T, DocumentFormatError>;

#[derive(Debug)]
pub enum PayloadType {
    Ndjson,
    Json,
    Csv,
}

impl fmt::Display for PayloadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadType::Ndjson => write!(f, "ndjson"),
            PayloadType::Json => write!(f, "json"),
            PayloadType::Csv => write!(f, "csv"),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DocumentFormatError {
    #[error("Internal error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}. The {1} payload provided is malformed.")]
    MalformedPayload(
        Box<dyn std::error::Error + Send + Sync + 'static>,
        PayloadType,
    ),
}

internal_error!(DocumentFormatError: milli::documents::Error, io::Error);

macro_rules! malformed {
    ($type:path, $e:expr) => {
        $e.map_err(|e| DocumentFormatError::MalformedPayload(Box::new(e), $type))
    };
}

pub fn read_csv(input: impl Read, writer: impl Write + Seek) -> Result<()> {
    let mut builder = DocumentBatchBuilder::new(writer).unwrap();

    let iter = CsvDocumentIter::from_reader(input)?;
    for doc in iter {
        let doc = doc?;
        builder.add_documents(doc).unwrap();
    }
    builder.finish().unwrap();

    Ok(())
}

/// read jsonl from input and write an obkv batch to writer.
pub fn read_ndjson(input: impl Read, writer: impl Write + Seek) -> Result<()> {
    let mut builder = DocumentBatchBuilder::new(writer)?;
    let stream = Deserializer::from_reader(input).into_iter::<Map<String, Value>>();

    for value in stream {
        let value = malformed!(PayloadType::Ndjson, value)?;
        builder.add_documents(&value)?;
    }

    builder.finish()?;

    Ok(())
}

/// read json from input and write an obkv batch to writer.
pub fn read_json(input: impl Read, writer: impl Write + Seek) -> Result<()> {
    let mut builder = DocumentBatchBuilder::new(writer).unwrap();

    let documents: Vec<Map<String, Value>> =
        malformed!(PayloadType::Json, serde_json::from_reader(input))?;
    builder.add_documents(documents).unwrap();
    builder.finish().unwrap();

    Ok(())
}

enum AllowedType {
    String,
    Number,
}

fn parse_csv_header(header: &str) -> (String, AllowedType) {
    // if there are several separators we only split on the last one.
    match header.rsplit_once(':') {
        Some((field_name, field_type)) => match field_type {
            "string" => (field_name.to_string(), AllowedType::String),
            "number" => (field_name.to_string(), AllowedType::Number),
            // if the pattern isn't reconized, we keep the whole field.
            _otherwise => (header.to_string(), AllowedType::String),
        },
        None => (header.to_string(), AllowedType::String),
    }
}

pub struct CsvDocumentIter<R>
where
    R: Read,
{
    documents: StringRecordsIntoIter<R>,
    headers: Vec<(String, AllowedType)>,
}

impl<R: Read> CsvDocumentIter<R> {
    pub fn from_reader(reader: R) -> IoResult<Self> {
        let mut records = CsvReader::from_reader(reader);

        let headers = records
            .headers()?
            .into_iter()
            .map(parse_csv_header)
            .collect();

        Ok(Self {
            documents: records.into_records(),
            headers,
        })
    }
}

impl<R: Read> Iterator for CsvDocumentIter<R> {
    type Item = Result<Map<String, Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        let csv_document = self.documents.next()?;

        match csv_document {
            Ok(csv_document) => {
                let mut document = Map::new();

                for ((field_name, field_type), value) in
                    self.headers.iter().zip(csv_document.into_iter())
                {
                    let parsed_value = match field_type {
                        AllowedType::Number => {
                            malformed!(PayloadType::Csv, value.parse::<f64>().map(Value::from))
                        }
                        AllowedType::String => Ok(Value::String(value.to_string())),
                    };

                    match parsed_value {
                        Ok(value) => drop(document.insert(field_name.to_string(), value)),
                        Err(e) => return Some(Err(e)),
                    }
                }

                Some(Ok(document))
            }
            Err(e) => Some(Err(DocumentFormatError::MalformedPayload(
                Box::new(e),
                PayloadType::Csv,
            ))),
        }
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::*;

    #[test]
    fn simple_csv_document() {
        let documents = r#"city,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn coma_in_field() {
        let documents = r#"city,country,pop
"Boston","United, States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United, States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn quote_in_field() {
        let documents = r#"city,country,pop
"Boston","United"" States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United\" States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn integer_in_field() {
        let documents = r#"city,country,pop:number
"Boston","United States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": 4628910.0,
            })
        );
    }

    #[test]
    fn float_in_field() {
        let documents = r#"city,country,pop:number
"Boston","United States","4628910.01""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": 4628910.01,
            })
        );
    }

    #[test]
    fn several_colon_in_header() {
        let documents = r#"city:love:string,country:state,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city:love": "Boston",
                "country:state": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn ending_by_colon_in_header() {
        let documents = r#"city:,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city:": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn starting_by_colon_in_header() {
        let documents = r#":city,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
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
        let documents = r#":string,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert!(csv_iter.next().unwrap().is_err());
    }

    #[test]
    fn double_colon_in_header() {
        let documents = r#"city::string,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city:": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn bad_type_in_header() {
        let documents = r#"city,country:number,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert!(csv_iter.next().unwrap().is_err());
    }

    #[test]
    fn bad_column_count1() {
        let documents = r#"city,country,pop
"Boston","United States","4628910", "too much""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert!(csv_iter.next().unwrap().is_err());
    }

    #[test]
    fn bad_column_count2() {
        let documents = r#"city,country,pop
"Boston","United States""#;

        let mut csv_iter = CsvDocumentIter::from_reader(documents.as_bytes()).unwrap();

        assert!(csv_iter.next().unwrap().is_err());
    }
}

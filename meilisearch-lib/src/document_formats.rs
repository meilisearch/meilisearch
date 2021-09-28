use std::{fmt, io::{Read, Seek, Write}};

use milli::documents::DocumentBatchBuilder;
use serde_json::{Deserializer, Map, Value};

type Result<T> = std::result::Result<T, DocumentFormatError>;

#[derive(Debug)]
pub enum PayloadType {
    Jsonl,
    Json,
}

impl fmt::Display for PayloadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadType::Jsonl => write!(f, "ndjson"),
            PayloadType::Json => write!(f, "json"),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DocumentFormatError {
    #[error("Internal error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}. The {1} payload provided is malformed.")]
    MalformedPayload(Box<dyn std::error::Error + Send + Sync + 'static>, PayloadType),
}

internal_error!(
    DocumentFormatError: milli::documents::Error
);

macro_rules! malformed {
    ($type:path, $e:expr) => {
        $e.map_err(|e| DocumentFormatError::MalformedPayload(Box::new(e), $type))
    };
}

/// read jsonl from input and write an obkv batch to writer.
pub fn read_jsonl(input: impl Read, writer: impl Write + Seek) -> Result<()> {
    let mut builder = DocumentBatchBuilder::new(writer)?;
    let stream = Deserializer::from_reader(input).into_iter::<Map<String, Value>>();

    for value in stream {
        let value = malformed!(PayloadType::Jsonl, value)?;
        builder.add_documents(&value)?;
    }

    builder.finish()?;

    Ok(())
}

/// read json from input and write an obkv batch to writer.
pub fn read_json(input: impl Read, writer: impl Write + Seek) -> Result<()> {
    let mut builder = DocumentBatchBuilder::new(writer).unwrap();

    let documents: Vec<Map<String, Value>> = malformed!(PayloadType::Json, serde_json::from_reader(input))?;
    builder.add_documents(documents).unwrap();
    builder.finish().unwrap();

    Ok(())
}

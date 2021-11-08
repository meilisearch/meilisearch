use std::fmt;
use std::io::{self, BufRead, BufReader, BufWriter, Cursor, Read, Seek, Write};

use meilisearch_error::{internal_error, Code, ErrorCode};
use milli::documents::DocumentBatchBuilder;

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
    #[error("An internal error has occurred. `{0}`.")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("The `{1}` payload provided is malformed. `{0}`.")]
    MalformedPayload(
        Box<dyn std::error::Error + Send + Sync + 'static>,
        PayloadType,
    ),
    #[error("The `{0}` payload must contain at least one document.")]
    EmptyPayload(PayloadType),
}

impl From<(PayloadType, milli::documents::Error)> for DocumentFormatError {
    fn from((ty, error): (PayloadType, milli::documents::Error)) -> Self {
        match error {
            milli::documents::Error::Io(e) => Self::Internal(Box::new(e)),
            e => Self::MalformedPayload(Box::new(e), ty),
        }
    }
}

impl ErrorCode for DocumentFormatError {
    fn error_code(&self) -> Code {
        match self {
            DocumentFormatError::Internal(_) => Code::Internal,
            DocumentFormatError::MalformedPayload(_, _) => Code::MalformedPayload,
            DocumentFormatError::EmptyPayload(_) => Code::MalformedPayload,
        }
    }
}

internal_error!(DocumentFormatError: io::Error);

/// reads csv from input and write an obkv batch to writer.
pub fn read_csv(input: impl Read, writer: impl Write + Seek) -> Result<usize> {
    let writer = BufWriter::new(writer);
    let builder =
        DocumentBatchBuilder::from_csv(input, writer).map_err(|e| (PayloadType::Csv, e))?;

    if builder.len() == 0 {
        return Err(DocumentFormatError::EmptyPayload(PayloadType::Csv));
    }

    let count = builder.finish().map_err(|e| (PayloadType::Csv, e))?;

    Ok(count)
}

/// reads jsonl from input and write an obkv batch to writer.
pub fn read_ndjson(input: impl Read, writer: impl Write + Seek) -> Result<usize> {
    let mut reader = BufReader::new(input);
    let writer = BufWriter::new(writer);

    let mut builder = DocumentBatchBuilder::new(writer).map_err(|e| (PayloadType::Ndjson, e))?;
    let mut buf = String::new();

    while reader.read_line(&mut buf)? > 0 {
        builder
            .extend_from_json(Cursor::new(&buf.as_bytes()))
            .map_err(|e| (PayloadType::Ndjson, e))?;
        buf.clear();
    }

    if builder.len() == 0 {
        return Err(DocumentFormatError::EmptyPayload(PayloadType::Ndjson));
    }

    let count = builder.finish().map_err(|e| (PayloadType::Ndjson, e))?;

    Ok(count)
}

/// reads json from input and write an obkv batch to writer.
pub fn read_json(input: impl Read, writer: impl Write + Seek) -> Result<usize> {
    let writer = BufWriter::new(writer);
    let mut builder = DocumentBatchBuilder::new(writer).map_err(|e| (PayloadType::Json, e))?;
    builder
        .extend_from_json(input)
        .map_err(|e| (PayloadType::Json, e))?;

    if builder.len() == 0 {
        return Err(DocumentFormatError::EmptyPayload(PayloadType::Json));
    }

    let count = builder.finish().map_err(|e| (PayloadType::Json, e))?;

    Ok(count)
}

use std::borrow::Borrow;
use std::fmt::{self, Debug, Display};
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

#[derive(Debug)]
pub enum DocumentFormatError {
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    MalformedPayload(Box<milli::documents::Error>, PayloadType),
}

impl Display for DocumentFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal(e) => write!(f, "An internal error has occurred: `{}`.", e),
            Self::MalformedPayload(me, b) => match me.borrow() {
                milli::documents::Error::JsonError(se) => {
                    // https://github.com/meilisearch/meilisearch/issues/2107
                    // The user input maybe insanely long. We need to truncate it.
                    let mut serde_msg = se.to_string();
                    let ellipsis = "...";
                    if serde_msg.len() > 100 + ellipsis.len() {
                        serde_msg.replace_range(50..serde_msg.len() - 85, ellipsis);
                    }

                    write!(
                        f,
                        "The `{}` payload provided is malformed. `Couldn't serialize document value: {}`.",
                        b, serde_msg
                )
                }
                _ => write!(f, "The `{}` payload provided is malformed: `{}`.", b, me),
            },
        }
    }
}

impl std::error::Error for DocumentFormatError {}

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
        }
    }
}

internal_error!(DocumentFormatError: io::Error);

/// reads csv from input and write an obkv batch to writer.
pub fn read_csv(input: impl Read, writer: impl Write + Seek) -> Result<usize> {
    let writer = BufWriter::new(writer);
    let builder =
        DocumentBatchBuilder::from_csv(input, writer).map_err(|e| (PayloadType::Csv, e))?;

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
        // skip empty lines
        if buf == "\n" {
            buf.clear();
            continue;
        }
        builder
            .extend_from_json(Cursor::new(&buf.as_bytes()))
            .map_err(|e| (PayloadType::Ndjson, e))?;
        buf.clear();
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

    let count = builder.finish().map_err(|e| (PayloadType::Json, e))?;

    Ok(count)
}

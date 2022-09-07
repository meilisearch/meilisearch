use std::borrow::Borrow;
use std::fmt::{self, Debug, Display};
use std::io::{self, BufReader, Read, Seek, Write};

use either::Either;
use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::internal_error;
use milli::documents::{DocumentsBatchBuilder, Error};
use milli::Object;
use serde::Deserialize;

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
            PayloadType::Ndjson => f.write_str("ndjson"),
            PayloadType::Json => f.write_str("json"),
            PayloadType::Csv => f.write_str("csv"),
        }
    }
}

#[derive(Debug)]
pub enum DocumentFormatError {
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    MalformedPayload(Error, PayloadType),
}

impl Display for DocumentFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal(e) => write!(f, "An internal error has occurred: `{}`.", e),
            Self::MalformedPayload(me, b) => match me.borrow() {
                Error::Json(se) => {
                    // https://github.com/meilisearch/meilisearch/issues/2107
                    // The user input maybe insanely long. We need to truncate it.
                    let mut serde_msg = se.to_string();
                    let ellipsis = "...";
                    let trim_input_prefix_len = 50;
                    let trim_input_suffix_len = 85;

                    if serde_msg.len()
                        > trim_input_prefix_len + trim_input_suffix_len + ellipsis.len()
                    {
                        serde_msg.replace_range(
                            trim_input_prefix_len..serde_msg.len() - trim_input_suffix_len,
                            ellipsis,
                        );
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

impl From<(PayloadType, Error)> for DocumentFormatError {
    fn from((ty, error): (PayloadType, Error)) -> Self {
        match error {
            Error::Io(e) => Self::Internal(Box::new(e)),
            e => Self::MalformedPayload(e, ty),
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

/// Reads CSV from input and write an obkv batch to writer.
pub fn read_csv(input: impl Read, writer: impl Write + Seek) -> Result<usize> {
    let mut builder = DocumentsBatchBuilder::new(writer);

    let csv = csv::Reader::from_reader(input);
    builder.append_csv(csv).map_err(|e| (PayloadType::Csv, e))?;

    let count = builder.documents_count();
    let _ = builder
        .into_inner()
        .map_err(Into::into)
        .map_err(DocumentFormatError::Internal)?;

    Ok(count as usize)
}

/// Reads JSON Lines from input and write an obkv batch to writer.
pub fn read_ndjson(input: impl Read, writer: impl Write + Seek) -> Result<usize> {
    let mut builder = DocumentsBatchBuilder::new(writer);
    let reader = BufReader::new(input);

    for result in serde_json::Deserializer::from_reader(reader).into_iter() {
        let object = result
            .map_err(Error::Json)
            .map_err(|e| (PayloadType::Ndjson, e))?;
        builder
            .append_json_object(&object)
            .map_err(Into::into)
            .map_err(DocumentFormatError::Internal)?;
    }

    let count = builder.documents_count();
    let _ = builder
        .into_inner()
        .map_err(Into::into)
        .map_err(DocumentFormatError::Internal)?;

    Ok(count as usize)
}

/// Reads JSON from input and write an obkv batch to writer.
pub fn read_json(input: impl Read, writer: impl Write + Seek) -> Result<usize> {
    let mut builder = DocumentsBatchBuilder::new(writer);
    let reader = BufReader::new(input);

    #[derive(Deserialize, Debug)]
    #[serde(transparent)]
    struct ArrayOrSingleObject {
        #[serde(with = "either::serde_untagged")]
        inner: Either<Vec<Object>, Object>,
    }

    let content: ArrayOrSingleObject = serde_json::from_reader(reader)
        .map_err(Error::Json)
        .map_err(|e| (PayloadType::Json, e))?;

    for object in content.inner.map_right(|o| vec![o]).into_inner() {
        builder
            .append_json_object(&object)
            .map_err(Into::into)
            .map_err(DocumentFormatError::Internal)?;
    }

    let count = builder.documents_count();
    let _ = builder
        .into_inner()
        .map_err(Into::into)
        .map_err(DocumentFormatError::Internal)?;

    Ok(count as usize)
}

use std::borrow::Borrow;
use std::fmt::{self, Debug, Display};
use std::fs::File;
use std::io::{self, Seek, Write};
use std::marker::PhantomData;
use either::Either;
use log::debug;
use memmap::MmapOptions;
use milli::documents::{DocumentsBatchBuilder, Error};
use milli::Object;
use serde::de::{Visitor, SeqAccess};
use serde::{Deserialize, Deserializer};
use serde_json::error::Category;
use crate::error::{Code, ErrorCode};
use crate::internal_error;

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
                    let mut message = match se.classify() {
                        Category::Data => {
                            "data are neither an object nor a list of objects".to_string()
                        }
                        _ => se.to_string(),
                    };

                    // https://github.com/meilisearch/meilisearch/issues/2107
                    // The user input maybe insanely long. We need to truncate it.
                    let ellipsis = "...";
                    let trim_input_prefix_len = 50;
                    let trim_input_suffix_len = 85;

                    if message.len()
                        > trim_input_prefix_len + trim_input_suffix_len + ellipsis.len()
                    {
                        message.replace_range(
                            trim_input_prefix_len..message.len() - trim_input_suffix_len,
                            ellipsis,
                        );
                    }

                    write!(
                        f,
                        "The `{}` payload provided is malformed. `Couldn't serialize document value: {}`.",
                        b, message
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
pub fn read_csv(file: &File, writer: impl Write + Seek) -> Result<usize> {
    let mut builder = DocumentsBatchBuilder::new(writer);
    let mmap = unsafe { MmapOptions::new().map(file).unwrap()};
    let csv = csv::Reader::from_reader(mmap.as_ref());
    builder.append_csv(csv).map_err(|e| (PayloadType::Csv, e))?;

    let count = builder.documents_count();
    let _ = builder.into_inner().map_err(Into::into).map_err(DocumentFormatError::Internal)?;

    Ok(count as usize)
}

/// Reads JSON from temporary file  and write an obkv batch to writer.
pub fn read_json(file: &File, writer: impl Write + Seek) -> Result<usize> {
    read_json_inner(file, writer, PayloadType::Json)
}

/// Reads JSON from temporary file  and write an obkv batch to writer.
pub fn read_ndjson(file: &File, writer: impl Write + Seek) -> Result<usize> {
    read_json_inner(file, writer, PayloadType::Ndjson)
}

/// Reads JSON from temporary file  and write an obkv batch to writer.
fn read_json_inner(file: &File, writer: impl Write + Seek, payload_type: PayloadType) -> Result<usize> {
    let mut builder = DocumentsBatchBuilder::new(writer);
    let mmap = unsafe { MmapOptions::new().map(file).unwrap()};
    let mut deserializer = serde_json::Deserializer::from_slice(&mmap);

    match array_each(&mut deserializer, |obj: Object | {
        builder
            .append_json_object(&obj)
    }) {
        Ok(Ok(count)) => debug!("serde json array size: {}", count),
        Ok(Err(e)) => return Err(DocumentFormatError::Internal(Box::new(e))),
        Err(_e) => {
            debug!("deserialize single json");
            #[derive(Deserialize, Debug)]
            #[serde(transparent)]
            struct ArrayOrSingleObject {
                #[serde(with = "either::serde_untagged")]
                inner: Either<Vec<Object>, Object>,
            }

            let content: ArrayOrSingleObject =
            serde_json::from_reader(file).map_err(Error::Json).map_err(|e| (payload_type, e))?;

            for object in content.inner.map_right(|o| vec![o]).into_inner() {
                builder
                    .append_json_object(&object)
                    .map_err(Into::into)
                    .map_err(DocumentFormatError::Internal)?;
            }
        } 
    }

    let count = builder.documents_count();
    let _ = builder.into_inner().map_err(Into::into).map_err(DocumentFormatError::Internal)?;

    Ok(count as usize)
}

/**
 * https://serde.rs/stream-array.html
 * https://github.com/serde-rs/json/issues/160
 */
fn array_each<'de, D, T, F>(deserializer: D, f: F) -> std::result::Result<io::Result<u64>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
    F: FnMut(T) -> io::Result<()>,
{
    struct SeqVisitor<T, F>(F, PhantomData<T>);

    impl<'de, T, F> Visitor<'de> for SeqVisitor<T, F>
    where
        T: Deserialize<'de>,
        F: FnMut(T) -> io::Result<()>,
    {
        type Value = io::Result<u64>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a nonempty sequence")
        }

        fn visit_seq<A>(mut self, mut seq: A) -> std::result::Result<io::Result<u64>, <A as SeqAccess<'de>>::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut max: u64 = 0;
            while let Some(value) = seq.next_element::<T>()? {
                match self.0(value) {
                    Ok(()) =>  max = max + 1,
                    Err(e) => return Ok(Err(e)),
                };
            }
            Ok(Ok(max))
        }
    }
    let visitor = SeqVisitor(f, PhantomData);
    deserializer.deserialize_seq(visitor)
}
use std::fmt::{self, Debug, Display};
use std::fs::File;
use std::io::{self, BufWriter};
use std::marker::PhantomData;

use bumpalo::Bump;
use bumparaw_collections::RawMap;
use memmap2::Mmap;
use milli::documents::Error;
use milli::Object;
use rustc_hash::FxBuildHasher;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::error::Category;
use serde_json::value::RawValue;
use serde_json::{to_writer, Map, Value};

use crate::error::{Code, ErrorCode};

type Result<T> = std::result::Result<T, DocumentFormatError>;

#[derive(Debug, Clone, Copy)]
pub enum PayloadType {
    Ndjson,
    Json,
    Csv { delimiter: u8 },
}

impl fmt::Display for PayloadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadType::Ndjson => f.write_str("ndjson"),
            PayloadType::Json => f.write_str("json"),
            PayloadType::Csv { .. } => f.write_str("csv"),
        }
    }
}

#[derive(Debug)]
pub enum DocumentFormatError {
    Io(io::Error),
    MalformedPayload(Error, PayloadType),
}

impl Display for DocumentFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::MalformedPayload(me, b) => match me {
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
            Error::Io(e) => Self::Io(e),
            e => Self::MalformedPayload(e, ty),
        }
    }
}

impl From<(PayloadType, serde_json::Error)> for DocumentFormatError {
    fn from((ty, error): (PayloadType, serde_json::Error)) -> Self {
        if error.classify() == Category::Data {
            Self::Io(error.into())
        } else {
            Self::MalformedPayload(Error::Json(error), ty)
        }
    }
}

impl From<(PayloadType, csv::Error)> for DocumentFormatError {
    fn from((ty, error): (PayloadType, csv::Error)) -> Self {
        if error.is_io_error() {
            Self::Io(error.into())
        } else {
            Self::MalformedPayload(Error::Csv(error), ty)
        }
    }
}

impl From<io::Error> for DocumentFormatError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl ErrorCode for DocumentFormatError {
    fn error_code(&self) -> Code {
        match self {
            DocumentFormatError::Io(e) => e.error_code(),
            DocumentFormatError::MalformedPayload(_, _) => Code::MalformedPayload,
        }
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
            // if the pattern isn't recognized, we keep the whole field.
            _otherwise => (header, AllowedType::String),
        },
        None => (header, AllowedType::String),
    }
}

/// Reads CSV from file and write it in NDJSON in a file checking it along the way.
pub fn read_csv(input: &File, output: impl io::Write, delimiter: u8) -> Result<u64> {
    let ptype = PayloadType::Csv { delimiter };
    let mut output = BufWriter::new(output);
    let mut reader = csv::ReaderBuilder::new().delimiter(delimiter).from_reader(input);

    let headers = reader.headers().map_err(|e| DocumentFormatError::from((ptype, e)))?.clone();
    let typed_fields: Vec<_> = headers.iter().map(parse_csv_header).collect();
    let mut object: Map<_, _> =
        typed_fields.iter().map(|(k, _)| (k.to_string(), Value::Null)).collect();

    let mut line = 0;
    let mut record = csv::StringRecord::new();
    while reader.read_record(&mut record).map_err(|e| DocumentFormatError::from((ptype, e)))? {
        // We increment here and not at the end of the loop
        // to take the header offset into account.
        line += 1;

        // Reset the document values
        object.iter_mut().for_each(|(_, v)| *v = Value::Null);

        for (i, (name, atype)) in typed_fields.iter().enumerate() {
            let value = &record[i];
            let trimmed_value = value.trim();
            let value = match atype {
                AllowedType::Number if trimmed_value.is_empty() => Value::Null,
                AllowedType::Number => match trimmed_value.parse::<i64>() {
                    Ok(integer) => Value::from(integer),
                    Err(_) => match trimmed_value.parse::<f64>() {
                        Ok(float) => Value::from(float),
                        Err(error) => {
                            return Err(DocumentFormatError::MalformedPayload(
                                Error::ParseFloat { error, line, value: value.to_string() },
                                ptype,
                            ))
                        }
                    },
                },
                AllowedType::Boolean if trimmed_value.is_empty() => Value::Null,
                AllowedType::Boolean => match trimmed_value.parse::<bool>() {
                    Ok(bool) => Value::from(bool),
                    Err(error) => {
                        return Err(DocumentFormatError::MalformedPayload(
                            Error::ParseBool { error, line, value: value.to_string() },
                            ptype,
                        ))
                    }
                },
                AllowedType::String if value.is_empty() => Value::Null,
                AllowedType::String => Value::from(value),
            };

            *object.get_mut(*name).expect("encountered an unknown field") = value;
        }

        to_writer(&mut output, &object).map_err(|e| DocumentFormatError::from((ptype, e)))?;
    }

    Ok(line as u64)
}

/// Reads JSON from file and write it in NDJSON in a file checking it along the way.
pub fn read_json(input: &File, output: impl io::Write) -> Result<u64> {
    // We memory map to be able to deserialize into a RawMap that
    // does not allocate when possible and only materialize the first/top level.
    let input = unsafe { Mmap::map(input).map_err(DocumentFormatError::Io)? };
    let mut doc_alloc = Bump::with_capacity(1024 * 1024); // 1MiB

    let mut out = BufWriter::new(output);
    let mut deserializer = serde_json::Deserializer::from_slice(&input);
    let res = array_each(&mut deserializer, |obj: &RawValue| {
        doc_alloc.reset();
        let map = RawMap::from_raw_value_and_hasher(obj, FxBuildHasher, &doc_alloc)?;
        to_writer(&mut out, &map)
    });
    let count = match res {
        // The json data has been deserialized and does not need to be processed again.
        // The data has been transferred to the writer during the deserialization process.
        Ok(Ok(count)) => count,
        Ok(Err(e)) => return Err(DocumentFormatError::from((PayloadType::Json, e))),
        Err(e) => {
            // Attempt to deserialize a single json string when the cause of the exception is not Category.data
            // Other types of deserialisation exceptions are returned directly to the front-end
            if e.classify() != Category::Data {
                return Err(DocumentFormatError::from((PayloadType::Json, e)));
            }

            let content: Object = serde_json::from_slice(&input)
                .map_err(Error::Json)
                .map_err(|e| (PayloadType::Json, e))?;
            to_writer(&mut out, &content)
                .map(|_| 1)
                .map_err(|e| DocumentFormatError::from((PayloadType::Json, e)))?
        }
    };

    match out.into_inner() {
        Ok(_) => Ok(count),
        Err(ie) => Err(DocumentFormatError::Io(ie.into_error())),
    }
}

/// Reads NDJSON from file and checks it.
pub fn read_ndjson(input: &File) -> Result<u64> {
    // We memory map to be able to deserialize into a RawMap that
    // does not allocate when possible and only materialize the first/top level.
    let input = unsafe { Mmap::map(input).map_err(DocumentFormatError::Io)? };
    let mut bump = Bump::with_capacity(1024 * 1024);

    let mut count = 0;
    for result in serde_json::Deserializer::from_slice(&input).into_iter() {
        bump.reset();
        match result {
            Ok(raw) => {
                // try to deserialize as a map
                RawMap::from_raw_value_and_hasher(raw, FxBuildHasher, &bump)
                    .map_err(|e| DocumentFormatError::from((PayloadType::Ndjson, e)))?;
                count += 1;
            }
            Err(e) => return Err(DocumentFormatError::from((PayloadType::Ndjson, e))),
        }
    }

    Ok(count)
}

/// The actual handling of the deserialization process in serde
/// avoids storing the deserialized object in memory.
///
/// ## References
/// <https://serde.rs/stream-array.html>
/// <https://github.com/serde-rs/json/issues/160>
fn array_each<'de, D, T, F>(
    deserializer: D,
    f: F,
) -> std::result::Result<serde_json::Result<u64>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
    F: FnMut(T) -> serde_json::Result<()>,
{
    struct SeqVisitor<T, F>(F, PhantomData<T>);

    impl<'de, T, F> Visitor<'de> for SeqVisitor<T, F>
    where
        T: Deserialize<'de>,
        F: FnMut(T) -> serde_json::Result<()>,
    {
        type Value = serde_json::Result<u64>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a nonempty sequence")
        }

        fn visit_seq<A>(
            mut self,
            mut seq: A,
        ) -> std::result::Result<serde_json::Result<u64>, <A as SeqAccess<'de>>::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut max: u64 = 0;
            while let Some(value) = seq.next_element::<T>()? {
                match self.0(value) {
                    Ok(()) => max += 1,
                    Err(e) => return Ok(Err(e)),
                }
            }
            Ok(Ok(max))
        }
    }
    let visitor = SeqVisitor(f, PhantomData);
    deserializer.deserialize_seq(visitor)
}

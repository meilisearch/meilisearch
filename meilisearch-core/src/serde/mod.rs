mod deserializer;

pub use self::deserializer::{Deserializer, DeserializerError};

use std::{error::Error, fmt};

use serde::ser;
use serde_json::Error as SerdeJsonError;
use meilisearch_schema::Error as SchemaError;

use crate::ParseNumberError;

#[derive(Debug)]
pub enum SerializerError {
    DocumentIdNotFound,
    InvalidDocumentIdFormat,
    Zlmdb(heed::Error),
    SerdeJson(SerdeJsonError),
    ParseNumber(ParseNumberError),
    Schema(SchemaError),
    UnserializableType { type_name: &'static str },
    UnindexableType { type_name: &'static str },
    UnrankableType { type_name: &'static str },
    Custom(String),
}

impl ser::Error for SerializerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SerializerError::Custom(msg.to_string())
    }
}

impl fmt::Display for SerializerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SerializerError::DocumentIdNotFound => {
                f.write_str("Primary key is missing.")
            }
            SerializerError::InvalidDocumentIdFormat => {
                f.write_str("a document primary key can be of type integer or string only composed of alphanumeric characters, hyphens (-) and underscores (_).")
            }
            SerializerError::Zlmdb(e) => write!(f, "heed related error: {}", e),
            SerializerError::SerdeJson(e) => write!(f, "serde json error: {}", e),
            SerializerError::ParseNumber(e) => {
                write!(f, "error while trying to parse a number: {}", e)
            }
            SerializerError::Schema(e) => write!(f, "impossible to update schema: {}", e),
            SerializerError::UnserializableType { type_name } => {
                write!(f, "{} is not a serializable type", type_name)
            }
            SerializerError::UnindexableType { type_name } => {
                write!(f, "{} is not an indexable type", type_name)
            }
            SerializerError::UnrankableType { type_name } => {
                write!(f, "{} types can not be used for ranking", type_name)
            }
            SerializerError::Custom(s) => f.write_str(s),
        }
    }
}

impl Error for SerializerError {}

impl From<String> for SerializerError {
    fn from(value: String) -> SerializerError {
        SerializerError::Custom(value)
    }
}

impl From<SerdeJsonError> for SerializerError {
    fn from(error: SerdeJsonError) -> SerializerError {
        SerializerError::SerdeJson(error)
    }
}

impl From<heed::Error> for SerializerError {
    fn from(error: heed::Error) -> SerializerError {
        SerializerError::Zlmdb(error)
    }
}

impl From<ParseNumberError> for SerializerError {
    fn from(error: ParseNumberError) -> SerializerError {
        SerializerError::ParseNumber(error)
    }
}

impl From<SchemaError> for SerializerError {
    fn from(error: SchemaError) -> SerializerError {
        SerializerError::Schema(error)
   }
}

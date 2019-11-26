macro_rules! forward_to_unserializable_type {
    ($($ty:ident => $se_method:ident,)*) => {
        $(
            fn $se_method(self, _v: $ty) -> Result<Self::Ok, Self::Error> {
                Err(SerializerError::UnserializableType { type_name: "$ty" })
            }
        )*
    }
}

mod convert_to_number;
mod convert_to_string;
mod deserializer;
mod extract_document_id;
mod indexer;
mod serializer;

pub use self::convert_to_number::ConvertToNumber;
pub use self::convert_to_string::ConvertToString;
pub use self::deserializer::{Deserializer, DeserializerError};
pub use self::extract_document_id::{compute_document_id, extract_document_id, value_to_string};
pub use self::indexer::Indexer;
pub use self::serializer::{serialize_value, Serializer};

use std::{error::Error, fmt};

use serde::ser;
use serde_json::Error as SerdeJsonError;

use crate::ParseNumberError;

#[derive(Debug)]
pub enum SerializerError {
    DocumentIdNotFound,
    InvalidDocumentIdType,
    Zlmdb(heed::Error),
    SerdeJson(SerdeJsonError),
    ParseNumber(ParseNumberError),
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
                f.write_str("serialized document does not have an id according to the schema")
            }
            SerializerError::InvalidDocumentIdType => {
                f.write_str("document identifier can only be of type string or number")
            }
            SerializerError::Zlmdb(e) => write!(f, "heed related error: {}", e),
            SerializerError::SerdeJson(e) => write!(f, "serde json error: {}", e),
            SerializerError::ParseNumber(e) => {
                write!(f, "error while trying to parse a number: {}", e)
            }
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

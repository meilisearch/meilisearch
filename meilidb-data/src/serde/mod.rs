macro_rules! forward_to_unserializable_type {
    ($($ty:ident => $se_method:ident,)*) => {
        $(
            fn $se_method(self, _v: $ty) -> Result<Self::Ok, Self::Error> {
                Err(SerializerError::UnserializableType { type_name: "$ty" })
            }
        )*
    }
}

mod deserializer;
mod extract_document_id;
mod convert_to_string;
mod indexer;
mod serializer;

pub use self::deserializer::Deserializer;
pub use self::extract_document_id::extract_document_id;
pub use self::convert_to_string::ConvertToString;
pub use self::indexer::Indexer;
pub use self::serializer::Serializer;

use std::{fmt, error::Error};
use rmp_serde::encode::Error as RmpError;
use serde::ser;

#[derive(Debug)]
pub enum SerializerError {
    DocumentIdNotFound,
    RmpError(RmpError),
    SledError(sled::Error),
    UnserializableType { type_name: &'static str },
    UnindexableType { type_name: &'static str },
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
                write!(f, "serialized document does not have an id according to the schema")
            }
            SerializerError::RmpError(e) => write!(f, "rmp serde related error: {}", e),
            SerializerError::SledError(e) => write!(f, "sled related error: {}", e),
            SerializerError::UnserializableType { type_name } => {
                write!(f, "{} are not a serializable type", type_name)
            },
            SerializerError::UnindexableType { type_name } => {
                write!(f, "{} are not an indexable type", type_name)
            },
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

impl From<RmpError> for SerializerError {
    fn from(error: RmpError) -> SerializerError {
        SerializerError::RmpError(error)
    }
}

impl From<sled::Error> for SerializerError {
    fn from(error: sled::Error) -> SerializerError {
        SerializerError::SledError(error)
    }
}

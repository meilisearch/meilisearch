macro_rules! forward_to_unserializable_type {
    ($($ty:ident => $se_method:ident,)*) => {
        $(
            fn $se_method(self, _v: $ty) -> Result<Self::Ok, Self::Error> {
                Err(SerializerError::UnserializableType { name: "$ty" })
            }
        )*
    }
}

mod deserializer;
mod serializer;
mod extract_string;

pub use self::deserializer::Deserializer;
pub use self::serializer::Serializer;
pub use self::extract_string::ExtractString;

use std::{fmt, error::Error};
use rmp_serde::encode::Error as RmpError;
use serde::ser;

#[derive(Debug)]
pub enum SerializerError {
    DocumentIdNotFound,
    RmpError(RmpError),
    SledError(sled::Error),
    UnserializableType { name: &'static str },
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
            SerializerError::UnserializableType { name } => {
                write!(f, "Only struct and map types are considered valid documents and
                           can be serialized, not {} types directly.", name)
            },
            SerializerError::Custom(s) => f.write_str(&s),
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

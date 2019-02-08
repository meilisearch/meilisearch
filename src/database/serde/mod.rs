use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::error::Error;
use std::fmt;

use serde::ser;

macro_rules! forward_to_unserializable_type {
    ($($ty:ident => $se_method:ident,)*) => {
        $(
            fn $se_method(self, _v: $ty) -> Result<Self::Ok, Self::Error> {
                Err(SerializerError::UnserializableType { name: "$ty" })
            }
        )*
    }
}

pub mod find_id;
pub mod key_to_string;
pub mod value_to_i64;
pub mod serializer;
pub mod indexer_serializer;
pub mod deserializer;

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[derive(Debug)]
pub enum SerializerError {
    DocumentIdNotFound,
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

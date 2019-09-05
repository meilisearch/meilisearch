use std::collections::HashSet;
use std::io::Cursor;
use std::{fmt, error::Error};

use meilidb_core::DocumentId;
use meilidb_schema::SchemaAttr;
use rmp_serde::decode::{Deserializer as RmpDeserializer, ReadReader};
use rmp_serde::decode::{Error as RmpError};
use serde::{de, forward_to_deserialize_any};

use crate::database::Index;

#[derive(Debug)]
pub enum DeserializerError {
    RmpError(RmpError),
    RocksDbError(rocksdb::Error),
    Custom(String),
}

impl de::Error for DeserializerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        DeserializerError::Custom(msg.to_string())
    }
}

impl fmt::Display for DeserializerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeserializerError::RmpError(e) => write!(f, "rmp serde related error: {}", e),
            DeserializerError::RocksDbError(e) => write!(f, "RocksDB related error: {}", e),
            DeserializerError::Custom(s) => f.write_str(s),
        }
    }
}

impl Error for DeserializerError {}

impl From<RmpError> for DeserializerError {
    fn from(error: RmpError) -> DeserializerError {
        DeserializerError::RmpError(error)
    }
}

impl From<rocksdb::Error> for DeserializerError {
    fn from(error: rocksdb::Error) -> DeserializerError {
        DeserializerError::RocksDbError(error)
    }
}

pub struct Deserializer<'a> {
    pub document_id: DocumentId,
    pub index: &'a Index,
    pub fields: Option<&'a HashSet<SchemaAttr>>,
}

impl<'de, 'a, 'b> de::Deserializer<'de> for &'b mut Deserializer<'a>
{
    type Error = DeserializerError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: de::Visitor<'de>
    {
        self.deserialize_map(visitor)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct struct enum identifier ignored_any
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: de::Visitor<'de>
    {
        let schema = self.index.schema();
        let documents = self.index.as_ref().documents_index;

        let iter = documents
            .document_fields(self.document_id)?
            .filter_map(|(attr, value)| {
                let is_displayed = schema.props(attr).is_displayed();
                if is_displayed && self.fields.map_or(true, |f| f.contains(&attr)) {
                    let attribute_name = schema.attribute_name(attr);
                    Some((attribute_name, Value::new(value)))
                } else {
                    None
                }
            });

        let map_deserializer = de::value::MapDeserializer::new(iter);
        let result = visitor.visit_map(map_deserializer).map_err(DeserializerError::from);

        result
    }
}

struct Value<A>(RmpDeserializer<ReadReader<Cursor<A>>>) where A: AsRef<[u8]>;

impl<A> Value<A> where A: AsRef<[u8]>
{
    fn new(value: A) -> Value<A> {
        Value(RmpDeserializer::new(Cursor::new(value)))
    }
}

impl<'de, A> de::IntoDeserializer<'de, RmpError> for Value<A>
where A: AsRef<[u8]>,
{
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

impl<'de, 'a, A> de::Deserializer<'de> for Value<A>
where A: AsRef<[u8]>,
{
    type Error = RmpError;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where V: de::Visitor<'de>
    {
        self.0.deserialize_any(visitor)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

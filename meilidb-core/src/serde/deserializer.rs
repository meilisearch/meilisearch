use std::collections::HashSet;
use std::io::Cursor;
use std::{fmt, error::Error};

use meilidb_schema::{Schema, SchemaAttr};
use rmp_serde::decode::{Deserializer as RmpDeserializer, ReadReader};
use rmp_serde::decode::{Error as RmpError};
use serde::{de, forward_to_deserialize_any};

use crate::store::DocumentsFields;
use crate::DocumentId;

#[derive(Debug)]
pub enum DeserializerError {
    RmpError(RmpError),
    RkvError(rkv::StoreError),
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
            DeserializerError::RkvError(e) => write!(f, "rkv related error: {}", e),
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

impl From<rkv::StoreError> for DeserializerError {
    fn from(error: rkv::StoreError) -> DeserializerError {
        DeserializerError::RkvError(error)
    }
}

pub struct Deserializer<'a, R> {
    pub document_id: DocumentId,
    pub reader: &'a R,
    pub documents_fields: DocumentsFields,
    pub schema: &'a Schema,
    pub attributes: Option<&'a HashSet<SchemaAttr>>,
}

impl<'de, 'a, 'b, R: 'a> de::Deserializer<'de> for &'b mut Deserializer<'a, R>
where R: rkv::Readable,
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
        let mut error = None;

        let iter = self.documents_fields
            .document_fields(self.reader, self.document_id)?
            .filter_map(|result| {
                let (attr, value) = match result {
                    Ok(value) => value,
                    Err(e) => { error = Some(e); return None },
                };

                let is_displayed = self.schema.props(attr).is_displayed();
                if is_displayed && self.attributes.map_or(true, |f| f.contains(&attr)) {
                    let attribute_name = self.schema.attribute_name(attr);
                    Some((attribute_name, Value::new(value)))
                } else {
                    None
                }
            });

        let map_deserializer = de::value::MapDeserializer::new(iter);
        let result = visitor.visit_map(map_deserializer).map_err(DeserializerError::from);

        match error.take() {
            Some(error) => Err(error.into()),
            None => result,
        }
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

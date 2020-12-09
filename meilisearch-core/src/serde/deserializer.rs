use std::collections::HashSet;
use std::io::Cursor;
use std::{error::Error, fmt};

use meilisearch_schema::{Schema, FieldId};
use serde::{de, forward_to_deserialize_any};
use serde_json::de::IoRead as SerdeJsonIoRead;
use serde_json::Deserializer as SerdeJsonDeserializer;
use serde_json::Error as SerdeJsonError;

use crate::database::MainT;
use crate::store::DocumentsFields;
use crate::DocumentId;

#[derive(Debug)]
pub enum DeserializerError {
    SerdeJson(SerdeJsonError),
    Zlmdb(heed::Error),
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
            DeserializerError::SerdeJson(e) => write!(f, "serde json related error: {}", e),
            DeserializerError::Zlmdb(e) => write!(f, "heed related error: {}", e),
            DeserializerError::Custom(s) => f.write_str(s),
        }
    }
}

impl Error for DeserializerError {}

impl From<SerdeJsonError> for DeserializerError {
    fn from(error: SerdeJsonError) -> DeserializerError {
        DeserializerError::SerdeJson(error)
    }
}

impl From<heed::Error> for DeserializerError {
    fn from(error: heed::Error) -> DeserializerError {
        DeserializerError::Zlmdb(error)
    }
}

pub struct Deserializer<'a> {
    pub document_id: DocumentId,
    pub reader: &'a heed::RoTxn<'a, MainT>,
    pub documents_fields: DocumentsFields,
    pub schema: &'a Schema,
    pub fields: Option<&'a HashSet<FieldId>>,
}

impl<'de, 'a, 'b> de::Deserializer<'de> for &'b mut Deserializer<'a> {
    type Error = DeserializerError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_option(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        let mut error = None;

        let iter = self
            .documents_fields
            .document_fields(self.reader, self.document_id)?
            .filter_map(|result| {
                let (attr, value) = match result {
                    Ok(value) => value,
                    Err(e) => {
                        error = Some(e);
                        return None;
                    }
                };

                let is_displayed = self.schema.is_displayed(attr);
                if is_displayed && self.fields.map_or(true, |f| f.contains(&attr)) {
                    if let Some(attribute_name) = self.schema.name(attr) {
                        let cursor = Cursor::new(value.to_owned());
                        let ioread = SerdeJsonIoRead::new(cursor);
                        let value = Value(SerdeJsonDeserializer::new(ioread));

                        Some((attribute_name, value))
                    } else {
                        None
                    }
                } else {
                    None
                }
            });

        let mut iter = iter.peekable();

        let result = match iter.peek() {
            Some(_) => {
                let map_deserializer = de::value::MapDeserializer::new(iter);
                visitor
                    .visit_some(map_deserializer)
                    .map_err(DeserializerError::from)
            }
            None => visitor.visit_none(),
        };

        match error.take() {
            Some(error) => Err(error.into()),
            None => result,
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct seq tuple
        tuple_struct struct enum identifier ignored_any
    }
}

struct Value(SerdeJsonDeserializer<SerdeJsonIoRead<Cursor<Vec<u8>>>>);

impl<'de> de::IntoDeserializer<'de, SerdeJsonError> for Value {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

impl<'de> de::Deserializer<'de> for Value {
    type Error = SerdeJsonError;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0.deserialize_any(visitor)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

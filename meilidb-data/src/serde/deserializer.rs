use std::collections::HashSet;
use std::io::Cursor;

use meilidb_core::DocumentId;
use rmp_serde::decode::{Deserializer as RmpDeserializer, ReadReader};
use rmp_serde::decode::{Error as RmpError};
use serde::{de, forward_to_deserialize_any};

use crate::database::RawIndex;
use crate::SchemaAttr;

pub struct Deserializer<'a> {
    pub document_id: DocumentId,
    pub raw_index: &'a RawIndex,
    pub fields: Option<&'a HashSet<SchemaAttr>>,
}

impl<'de, 'a, 'b> de::Deserializer<'de> for &'b mut Deserializer<'a>
{
    type Error = RmpError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: de::Visitor<'de>
    {
        self.deserialize_map(visitor)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit seq
        bytes byte_buf unit_struct tuple_struct
        identifier tuple ignored_any option newtype_struct enum struct
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: de::Visitor<'de>
    {
        let document_attributes = self.raw_index.get_document_fields(self.document_id);
        let document_attributes = document_attributes.filter_map(|result| {
            match result {
                Ok(value) => Some(value),
                Err(e) => {
                    // TODO: must log the error
                    // error!("sled iter error; {}", e);
                    None
                },
            }
        });
        let iter = document_attributes.filter_map(|(_, attr, value)| {
            if self.fields.map_or(true, |f| f.contains(&attr)) {
                let attribute_name = self.raw_index.schema().attribute_name(attr);
                Some((attribute_name, Value::new(value)))
            } else {
                None
            }
        });

        let map_deserializer = de::value::MapDeserializer::new(iter);
        visitor.visit_map(map_deserializer)
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

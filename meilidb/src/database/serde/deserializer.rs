use std::error::Error;
use std::ops::Deref;
use std::fmt;

use rocksdb::rocksdb::{DB, Snapshot, SeekKey};
use rocksdb::rocksdb_options::ReadOptions;
use serde::forward_to_deserialize_any;
use serde::de::value::MapDeserializer;
use serde::de::{self, Visitor, IntoDeserializer};

use crate::database::document_key::{DocumentKey, DocumentKeyAttr};
use crate::database::schema::Schema;
use meilidb_core::DocumentId;

pub struct Deserializer<'a, D>
where D: Deref<Target=DB>
{
    snapshot: &'a Snapshot<D>,
    schema: &'a Schema,
    document_id: DocumentId,
}

impl<'a, D> Deserializer<'a, D>
where D: Deref<Target=DB>
{
    pub fn new(snapshot: &'a Snapshot<D>, schema: &'a Schema, doc: DocumentId) -> Self {
        Deserializer { snapshot, schema, document_id: doc }
    }
}

impl<'de, 'a, 'b, D> de::Deserializer<'de> for &'b mut Deserializer<'a, D>
where D: Deref<Target=DB>
{
    type Error = DeserializerError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        self.deserialize_map(visitor)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit seq
        bytes byte_buf unit_struct tuple_struct
        identifier tuple ignored_any option newtype_struct enum struct
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        let mut options = ReadOptions::new();
        let lower = DocumentKey::new(self.document_id);
        let upper = lower.with_attribute_max();
        options.set_iterate_lower_bound(lower.as_ref());
        options.set_iterate_upper_bound(upper.as_ref());

        let mut iter = self.snapshot.iter_opt(options);
        iter.seek(SeekKey::Start);

        if iter.kv().is_none() {
            // FIXME return an error
        }

        let iter = iter.map(|(key, value)| {
            // retrieve the schema attribute name
            // from the schema attribute number
            let document_key_attr = DocumentKeyAttr::from_bytes(&key);
            let schema_attr = document_key_attr.attribute();
            let attribute_name = self.schema.attribute_name(schema_attr);
            (attribute_name, Value(value))
        });

        let map_deserializer = MapDeserializer::new(iter);
        visitor.visit_map(map_deserializer)
    }
}

struct Value(Vec<u8>);

impl<'de> IntoDeserializer<'de, DeserializerError> for Value {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

macro_rules! forward_to_bincode_values {
    ($($ty:ident => $de_method:ident,)*) => {
        $(
            fn $de_method<V>(self, visitor: V) -> Result<V::Value, Self::Error>
                where V: de::Visitor<'de>
            {
                match bincode::deserialize::<$ty>(&self.0) {
                    Ok(val) => val.into_deserializer().$de_method(visitor),
                    Err(e) => Err(de::Error::custom(e)),
                }
            }
        )*
    }
}

impl<'de, 'a> de::Deserializer<'de> for Value {
    type Error = DeserializerError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        self.0.into_deserializer().deserialize_any(visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        match bincode::deserialize::<String>(&self.0) {
            Ok(val) => val.into_deserializer().deserialize_string(visitor),
            Err(e) => Err(de::Error::custom(e)),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        match bincode::deserialize::<Vec<u8>>(&self.0) {
            Ok(val) => val.into_deserializer().deserialize_byte_buf(visitor),
            Err(e) => Err(de::Error::custom(e)),
        }
    }

    forward_to_bincode_values! {
        char => deserialize_char,
        bool => deserialize_bool,

        u8  => deserialize_u8,
        u16 => deserialize_u16,
        u32 => deserialize_u32,
        u64 => deserialize_u64,

        i8  => deserialize_i8,
        i16 => deserialize_i16,
        i32 => deserialize_i32,
        i64 => deserialize_i64,

        f32 => deserialize_f32,
        f64 => deserialize_f64,
    }

    forward_to_deserialize_any! {
        unit seq map
        unit_struct tuple_struct
        identifier tuple ignored_any option newtype_struct enum struct
    }
}

#[derive(Debug)]
pub enum DeserializerError {
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
            DeserializerError::Custom(s) => f.write_str(&s),
        }
    }
}

impl Error for DeserializerError {}

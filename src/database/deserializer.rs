use std::error::Error;
use std::fmt;

use rocksdb::rocksdb::{DB, Snapshot};
use rocksdb::rocksdb_options::ReadOptions;
use serde::de::value::MapDeserializer;
use serde::forward_to_deserialize_any;
use serde::de::Visitor;

use crate::database::document_key::{DocumentKey, DocumentKeyAttr};
use crate::index::schema::Schema;
use crate::DocumentId;

pub struct Deserializer<'a> {
    snapshot: &'a Snapshot<&'a DB>,
    schema: &'a Schema,
    document_id: DocumentId,
}

impl<'a> Deserializer<'a> {
    pub fn new(snapshot: &'a Snapshot<&DB>, schema: &'a Schema, doc: DocumentId) -> Self {
        Deserializer { snapshot, schema, document_id: doc }
    }
}

impl<'de, 'a, 'b> serde::de::Deserializer<'de> for &'b mut Deserializer<'a> {
    type Error = DeserializerError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        self.deserialize_map(visitor)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit seq
        bytes byte_buf unit_struct tuple_struct
        identifier tuple ignored_any option newtype_struct enum
        struct
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        let mut options = ReadOptions::new();
        let lower = DocumentKey::new(self.document_id);
        let upper = DocumentKey::new(self.document_id + 1);
        options.set_iterate_lower_bound(lower.as_ref());
        options.set_iterate_upper_bound(upper.as_ref());

        let mut db_iter = self.snapshot.iter_opt(options);
        let iter = db_iter.map(|(key, value)| {
            // retrieve the schema attribute name
            // from the schema attribute number
            let document_key_attr = DocumentKeyAttr::from_bytes(&key);
            let schema_attr = document_key_attr.attribute();
            let attribute_name = self.schema.attribute_name(schema_attr);
            (attribute_name, value)
        });

        let map_deserializer = MapDeserializer::new(iter);
        visitor.visit_map(map_deserializer)
    }
}

#[derive(Debug)]
pub enum DeserializerError {
    Custom(String),
}

impl serde::de::Error for DeserializerError {
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

use std::collections::BTreeMap;
use std::io::Cursor;
use std::io::Write;
use std::fmt;

use byteorder::WriteBytesExt;
use serde::Deserialize;
use serde::de::DeserializeSeed;
use serde::de::MapAccess;
use serde::de::SeqAccess;
use serde::de::Visitor;
use serde_json::Value;

use super::{ByteCounter, DocumentsBatchIndex};
use crate::FieldId;

struct FieldIdResolver<'a>(&'a mut DocumentsBatchIndex);

impl<'a, 'de> DeserializeSeed<'de> for FieldIdResolver<'a> {
    type Value = FieldId;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de> {
            deserializer.deserialize_str(self)
    }
}

impl<'a, 'de> Visitor<'de> for FieldIdResolver<'a> {
    type Value = FieldId;

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
            Ok(self.0.insert(v))
    }

    fn expecting(&self, _formatter: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

struct ValueDeserializer;

impl<'de> DeserializeSeed<'de> for ValueDeserializer {
    type Value = serde_json::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de> {
            serde_json::Value::deserialize(deserializer)
    }
}

pub struct DocumentVisitor<'a, W> {
    pub inner: &'a mut ByteCounter<W>,
    pub index: &'a mut DocumentsBatchIndex,
    pub obkv_buffer: &'a mut Vec<u8>,
    pub value_buffer: &'a mut Vec<u8>,
    pub values: &'a mut BTreeMap<FieldId, Value>,
    pub count: &'a mut usize,
}

impl<'a, 'de, W: Write> Visitor<'de> for &mut DocumentVisitor<'a, W> {
    /// This Visitor value is nothing, since it write the value to a file.
    type Value = ();

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(_) = seq.next_element_seed(&mut *self)? { }

        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some((key, value)) = map.next_entry_seed(FieldIdResolver(&mut *self.index), ValueDeserializer).unwrap() {
            self.values.insert(key, value);
        }

        self.obkv_buffer.clear();
        let mut obkv = obkv::KvWriter::new(Cursor::new(&mut *self.obkv_buffer));
        for (key, value) in self.values.iter() {
            self.value_buffer.clear();
            // This is guaranteed to work
            serde_json::to_writer(Cursor::new(&mut *self.value_buffer), value).unwrap();
            obkv.insert(*key, &self.value_buffer).unwrap();
        }

        let reader = obkv.into_inner().unwrap().into_inner();

        self.inner.write_u32::<byteorder::BigEndian>(reader.len() as u32).unwrap();
        self.inner.write_all(reader).unwrap();

        *self.count += 1;

        Ok(())
    }

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a documents, or a sequence of documents.")
    }
}

impl<'a, 'de, W> DeserializeSeed<'de> for &mut DocumentVisitor<'a, W>
where W: Write,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de> {
            deserializer.deserialize_map(self)
    }
}

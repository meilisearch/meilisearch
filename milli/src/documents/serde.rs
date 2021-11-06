use std::collections::BTreeMap;
use std::fmt;
use std::io::{Cursor, Write};

use byteorder::WriteBytesExt;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::Value;

use super::{ByteCounter, DocumentsBatchIndex, Error};
use crate::FieldId;

macro_rules! tri {
    ($e:expr) => {
        match $e {
            Ok(r) => r,
            Err(e) => return Ok(Err(e.into())),
        }
    };
}

struct FieldIdResolver<'a>(&'a mut DocumentsBatchIndex);

impl<'a, 'de> DeserializeSeed<'de> for FieldIdResolver<'a> {
    type Value = FieldId;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
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

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a string")
    }
}

struct ValueDeserializer;

impl<'de> DeserializeSeed<'de> for ValueDeserializer {
    type Value = serde_json::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
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
    type Value = Result<(), Error>;

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(v) = seq.next_element_seed(&mut *self)? {
            tri!(v)
        }

        Ok(Ok(()))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some((key, value)) =
            map.next_entry_seed(FieldIdResolver(&mut *self.index), ValueDeserializer)?
        {
            self.values.insert(key, value);
        }

        self.obkv_buffer.clear();
        let mut obkv = obkv::KvWriter::new(Cursor::new(&mut *self.obkv_buffer));
        for (key, value) in self.values.iter() {
            self.value_buffer.clear();
            // This is guaranteed to work
            tri!(serde_json::to_writer(Cursor::new(&mut *self.value_buffer), value));
            tri!(obkv.insert(*key, &self.value_buffer));
        }

        let reader = tri!(obkv.into_inner()).into_inner();

        tri!(self.inner.write_u32::<byteorder::BigEndian>(reader.len() as u32));
        tri!(self.inner.write_all(reader));

        *self.count += 1;
        self.values.clear();

        Ok(Ok(()))
    }

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a documents, or a sequence of documents.")
    }
}

impl<'a, 'de, W> DeserializeSeed<'de> for &mut DocumentVisitor<'a, W>
where
    W: Write,
{
    type Value = Result<(), Error>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

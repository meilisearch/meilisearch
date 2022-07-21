use std::fmt;
use std::io::Write;

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

use super::Error;
use crate::documents::DocumentsBatchBuilder;
use crate::Object;

macro_rules! tri {
    ($e:expr) => {
        match $e {
            Ok(r) => r,
            Err(e) => return Ok(Err(e.into())),
        }
    };
}

pub struct DocumentVisitor<'a, W> {
    inner: &'a mut DocumentsBatchBuilder<W>,
    object: Object,
}

impl<'a, W> DocumentVisitor<'a, W> {
    pub fn new(inner: &'a mut DocumentsBatchBuilder<W>) -> Self {
        DocumentVisitor { inner, object: Object::new() }
    }
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
        self.object.clear();
        while let Some((key, value)) = map.next_entry()? {
            self.object.insert(key, value);
        }

        tri!(self.inner.append_json_object(&self.object));

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

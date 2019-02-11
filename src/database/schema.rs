use std::collections::{HashMap, BTreeMap};
use std::io::{Read, Write};
use std::error::Error;
use std::{fmt, u16};
use std::ops::BitOr;
use std::sync::Arc;

use serde_derive::{Serialize, Deserialize};
use linked_hash_map::LinkedHashMap;
use serde::Serialize;

use crate::database::serde::find_id::FindDocumentIdSerializer;
use crate::database::serde::SerializerError;
use crate::DocumentId;

pub const STORED: SchemaProps  = SchemaProps { stored: true,  indexed: false, ranked: false };
pub const INDEXED: SchemaProps = SchemaProps { stored: false, indexed: true,  ranked: false };
pub const RANKED: SchemaProps  = SchemaProps { stored: false, indexed: false, ranked: true  };

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaProps {
    #[serde(default)]
    stored: bool,

    #[serde(default)]
    indexed: bool,

    #[serde(default)]
    ranked: bool,
}

impl SchemaProps {
    pub fn is_stored(self) -> bool {
        self.stored
    }

    pub fn is_indexed(self) -> bool {
        self.indexed
    }

    pub fn is_ranked(self) -> bool {
        self.ranked
    }
}

impl BitOr for SchemaProps {
    type Output = Self;

    fn bitor(self, other: Self) -> Self::Output {
        SchemaProps {
            stored: self.stored | other.stored,
            indexed: self.indexed | other.indexed,
            ranked: self.ranked | other.ranked,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SchemaBuilder {
    identifier: String,
    attributes: LinkedHashMap<String, SchemaProps>,
}

impl SchemaBuilder {
    pub fn with_identifier<S: Into<String>>(name: S) -> SchemaBuilder {
        SchemaBuilder {
            identifier: name.into(),
            attributes: LinkedHashMap::new(),
        }
    }

    pub fn new_attribute<S: Into<String>>(&mut self, name: S, props: SchemaProps) -> SchemaAttr {
        let len = self.attributes.len();
        if self.attributes.insert(name.into(), props).is_some() {
            panic!("Field already inserted.")
        }
        SchemaAttr(len as u16)
    }

    pub fn build(self) -> Schema {
        let mut attrs = HashMap::new();
        let mut props = Vec::new();

        for (i, (name, prop)) in self.attributes.into_iter().enumerate() {
            attrs.insert(name.clone(), SchemaAttr(i as u16));
            props.push((name, prop));
        }

        let identifier = self.identifier;
        Schema { inner: Arc::new(InnerSchema { identifier, attrs, props }) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    inner: Arc<InnerSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InnerSchema {
    identifier: String,
    attrs: HashMap<String, SchemaAttr>,
    props: Vec<(String, SchemaProps)>,
}

impl Schema {
    pub fn from_toml<R: Read>(mut reader: R) -> Result<Schema, Box<Error>> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        let builder: SchemaBuilder = toml::from_slice(&buffer)?;
        Ok(builder.build())
    }

    pub fn to_toml<W: Write>(&self, mut writer: W) -> Result<(), Box<Error>> {
        let identifier = self.inner.identifier.clone();
        let attributes = self.attributes_ordered();
        let builder = SchemaBuilder { identifier, attributes };

        let string = toml::to_string_pretty(&builder)?;
        writer.write_all(string.as_bytes())?;

        Ok(())
    }

    pub fn from_json<R: Read>(mut reader: R) -> Result<Schema, Box<Error>> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        let builder: SchemaBuilder = serde_json::from_slice(&buffer)?;
        Ok(builder.build())
    }

    pub fn to_json<W: Write>(&self, mut writer: W) -> Result<(), Box<Error>> {
        let identifier = self.inner.identifier.clone();
        let attributes = self.attributes_ordered();
        let builder = SchemaBuilder { identifier, attributes };
        let string = serde_json::to_string_pretty(&builder)?;
        writer.write_all(string.as_bytes())?;

        Ok(())
    }

    pub(crate) fn read_from_bin<R: Read>(reader: R) -> bincode::Result<Schema> {
        let builder: SchemaBuilder = bincode::deserialize_from(reader)?;
        Ok(builder.build())
    }

    pub(crate) fn write_to_bin<W: Write>(&self, writer: W) -> bincode::Result<()> {
        let identifier = self.inner.identifier.clone();
        let attributes = self.attributes_ordered();
        let builder = SchemaBuilder { identifier, attributes };

        bincode::serialize_into(writer, &builder)
    }

    fn attributes_ordered(&self) -> LinkedHashMap<String, SchemaProps> {
        let mut ordered = BTreeMap::new();
        for (name, attr) in &self.inner.attrs {
            let (_, props) = self.inner.props[attr.0 as usize];
            ordered.insert(attr.0, (name, props));
        }

        let mut attributes = LinkedHashMap::with_capacity(ordered.len());
        for (_, (name, props)) in ordered {
            attributes.insert(name.clone(), props);
        }

        attributes
    }

    pub fn document_id<T>(&self, document: T) -> Result<DocumentId, SerializerError>
    where T: Serialize,
    {
        let id_attribute_name = &self.inner.identifier;
        let serializer = FindDocumentIdSerializer { id_attribute_name };
        document.serialize(serializer)
    }

    pub fn props(&self, attr: SchemaAttr) -> SchemaProps {
        let (_, props) = self.inner.props[attr.0 as usize];
        props
    }

    pub fn identifier_name(&self) -> &str {
        &self.inner.identifier
    }

    pub fn attribute<S: AsRef<str>>(&self, name: S) -> Option<SchemaAttr> {
        self.inner.attrs.get(name.as_ref()).cloned()
    }

    pub fn attribute_name(&self, attr: SchemaAttr) -> &str {
        let (name, _) = &self.inner.props[attr.0 as usize];
        name
    }
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct SchemaAttr(pub(crate) u16);

impl SchemaAttr {
    pub fn new(value: u16) -> SchemaAttr {
        SchemaAttr(value)
    }

    pub fn min() -> SchemaAttr {
        SchemaAttr(0)
    }

    pub fn next(self) -> Option<SchemaAttr> {
        self.0.checked_add(1).map(SchemaAttr)
    }

    pub fn prev(self) -> Option<SchemaAttr> {
        self.0.checked_sub(1).map(SchemaAttr)
    }

    pub fn max() -> SchemaAttr {
        SchemaAttr(u16::MAX)
    }
}

impl fmt::Display for SchemaAttr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn serialize_deserialize() -> bincode::Result<()> {
        let mut builder = SchemaBuilder::with_identifier("id");
        builder.new_attribute("alpha", STORED);
        builder.new_attribute("beta", STORED | INDEXED);
        builder.new_attribute("gamma", INDEXED);
        let schema = builder.build();

        let mut buffer = Vec::new();

        schema.write_to_bin(&mut buffer)?;
        let schema2 = Schema::read_from_bin(buffer.as_slice())?;

        assert_eq!(schema, schema2);

        Ok(())
    }

    #[test]
    fn serialize_deserialize_toml() -> Result<(), Box<Error>> {
        let mut builder = SchemaBuilder::with_identifier("id");
        builder.new_attribute("alpha", STORED);
        builder.new_attribute("beta", STORED | INDEXED);
        builder.new_attribute("gamma", INDEXED);
        let schema = builder.build();

        let mut buffer = Vec::new();
        schema.to_toml(&mut buffer)?;

        let schema2 = Schema::from_toml(buffer.as_slice())?;
        assert_eq!(schema, schema2);

        let data = r#"
            identifier = "id"

            [attributes."alpha"]
            stored = true

            [attributes."beta"]
            stored = true
            indexed = true

            [attributes."gamma"]
            indexed = true
        "#;
        let schema2 = Schema::from_toml(data.as_bytes())?;
        assert_eq!(schema, schema2);

        Ok(())
    }

    #[test]
    fn serialize_deserialize_json() -> Result<(), Box<Error>> {
        let mut builder = SchemaBuilder::with_identifier("id");
        builder.new_attribute("alpha", STORED);
        builder.new_attribute("beta", STORED | INDEXED);
        builder.new_attribute("gamma", INDEXED);
        let schema = builder.build();

        let mut buffer = Vec::new();
        schema.to_json(&mut buffer)?;

        let schema2 = Schema::from_json(buffer.as_slice())?;
        assert_eq!(schema, schema2);

        let data = r#"
            {
                "identifier": "id",
                "attributes": {
                    "alpha": {
                        "stored": true
                    },
                    "beta": {
                        "stored": true,
                        "indexed": true
                    },
                    "gamma": {
                        "indexed": true
                    }
                }
            }"#;
        let schema2 = Schema::from_json(data.as_bytes())?;
        assert_eq!(schema, schema2);

        Ok(())
    }
}

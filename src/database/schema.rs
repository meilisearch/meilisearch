use crate::database::update::SerializerError;
use std::collections::{HashMap, BTreeMap};
use crate::database::calculate_hash;
use std::io::{Read, Write};
use std::error::Error;
use std::{fmt, u16};
use std::ops::BitOr;
use std::sync::Arc;

use serde_derive::{Serialize, Deserialize};
use serde::ser::{self, Serialize};
use linked_hash_map::LinkedHashMap;

use crate::DocumentId;

pub const STORED: SchemaProps = SchemaProps { stored: true, indexed: false };
pub const INDEXED: SchemaProps = SchemaProps { stored: false, indexed: true };

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaProps {
    #[serde(default)]
    stored: bool,

    #[serde(default)]
    indexed: bool,
}

impl SchemaProps {
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    pub fn is_indexed(&self) -> bool {
        self.indexed
    }
}

impl BitOr for SchemaProps {
    type Output = Self;

    fn bitor(self, other: Self) -> Self::Output {
        SchemaProps {
            stored: self.stored | other.stored,
            indexed: self.indexed | other.indexed,
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

    pub fn document_id<T>(&self, document: &T) -> Result<DocumentId, SerializerError>
    where T: Serialize,
    {
        let find_document_id = FindDocumentIdSerializer {
            id_attribute_name: self.identifier_name(),
        };
        document.serialize(find_document_id)
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

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct SchemaAttr(pub(crate) u16);

impl SchemaAttr {
    pub fn new(value: u16) -> SchemaAttr {
        SchemaAttr(value)
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

struct FindDocumentIdSerializer<'a> {
    id_attribute_name: &'a str,
}

impl<'a> ser::Serializer for FindDocumentIdSerializer<'a> {
    type Ok = DocumentId;
    type Error = SerializerError;
    type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = FindDocumentIdStructSerializer<'a>;
    type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;

    forward_to_unserializable_type! {
        bool => serialize_bool,
        char => serialize_char,

        i8  => serialize_i8,
        i16 => serialize_i16,
        i32 => serialize_i32,
        i64 => serialize_i64,

        u8  => serialize_u8,
        u16 => serialize_u16,
        u32 => serialize_u32,
        u64 => serialize_u64,

        f32 => serialize_f32,
        f64 => serialize_f64,
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "str" })
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "&[u8]" })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "Option" })
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        Err(SerializerError::UnserializableType { name: "Option" })
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "()" })
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "unit struct" })
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str
    ) -> Result<Self::Ok, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "unit variant" })
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T
    ) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T
    ) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        Err(SerializerError::UnserializableType { name: "newtype variant" })
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(SerializerError::UnserializableType { name: "sequence" })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(SerializerError::UnserializableType { name: "tuple" })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleStruct, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "tuple struct" })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "tuple variant" })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        // Ok(MapSerializer {
        //     schema: self.schema,
        //     document_id: self.document_id,
        //     new_states: self.new_states,
        // })
        Err(SerializerError::UnserializableType { name: "map" })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStruct, Self::Error>
    {
        Ok(FindDocumentIdStructSerializer {
            id_attribute_name: self.id_attribute_name,
            document_id: None,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStructVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "struct variant" })
    }
}

struct FindDocumentIdStructSerializer<'a> {
    id_attribute_name: &'a str,
    document_id: Option<DocumentId>,
}

impl<'a> ser::SerializeStruct for FindDocumentIdStructSerializer<'a> {
    type Ok = DocumentId;
    type Error = SerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T
    ) -> Result<(), Self::Error>
    where T: Serialize,
    {
        if self.id_attribute_name == key {
            // TODO can it be possible to have multiple ids?
            let id = bincode::serialize(value).unwrap();
            let hash = calculate_hash(&id);
            self.document_id = Some(DocumentId(hash));
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self.document_id {
            Some(document_id) => Ok(document_id),
            None => Err(SerializerError::DocumentIdNotFound)
        }
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
}

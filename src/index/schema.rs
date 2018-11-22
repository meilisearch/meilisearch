use std::collections::{HashMap, BTreeMap};
use std::io::{Read, Write};
use std::path::Path;
use std::ops::BitOr;
use std::fs::File;
use std::fmt;

use linked_hash_map::LinkedHashMap;

pub const STORED: SchemaProps = SchemaProps { stored: true, indexed: false };
pub const INDEXED: SchemaProps = SchemaProps { stored: false, indexed: true };

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaProps {
    stored: bool,
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

pub struct SchemaBuilder {
    attrs: LinkedHashMap<String, SchemaProps>,
}

impl SchemaBuilder {
    pub fn new() -> SchemaBuilder {
        SchemaBuilder { attrs: LinkedHashMap::new() }
    }

    pub fn new_attribute<S: Into<String>>(&mut self, name: S, props: SchemaProps) -> SchemaAttr {
        let len = self.attrs.len();
        if self.attrs.insert(name.into(), props).is_some() {
            panic!("Field already inserted.")
        }
        SchemaAttr(len as u32)
    }

    pub fn build(self) -> Schema {
        let mut attrs = HashMap::new();
        let mut props = Vec::new();

        for (i, (name, prop)) in self.attrs.into_iter().enumerate() {
            attrs.insert(name, SchemaAttr(i as u32));
            props.push(prop);
        }

        Schema { attrs, props }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    attrs: HashMap<String, SchemaAttr>,
    props: Vec<SchemaProps>,
}

impl Schema {
    pub fn open<P: AsRef<Path>>(path: P) -> bincode::Result<Schema> {
        let file = File::open(path)?;
        Schema::read_from(file)
    }

    pub fn read_from<R: Read>(reader: R) -> bincode::Result<Schema> {
        let attrs = bincode::deserialize_from(reader)?;
        let builder = SchemaBuilder { attrs };
        Ok(builder.build())
    }

    pub fn write_to<W: Write>(&self, writer: W) -> bincode::Result<()> {
        let mut ordered = BTreeMap::new();
        for (name, field) in &self.attrs {
            let index = field.as_u32();
            let props = self.props[index as usize];
            ordered.insert(index, (name, props));
        }

        let mut attrs = LinkedHashMap::with_capacity(ordered.len());
        for (_, (name, props)) in ordered {
            attrs.insert(name, props);
        }

        bincode::serialize_into(writer, &attrs)
    }

    pub fn props(&self, attr: SchemaAttr) -> SchemaProps {
        self.props[attr.as_u32() as usize]
    }

    pub fn attribute<S: AsRef<str>>(&self, name: S) -> Option<SchemaAttr> {
        self.attrs.get(name.as_ref()).cloned()
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct SchemaAttr(u32);

impl SchemaAttr {
    pub fn as_u32(&self) -> u32 {
        self.0
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

    #[test]
    fn serialize_deserialize() -> bincode::Result<()> {
        let mut builder = SchemaBuilder::new();
        builder.new_attribute("alphabet", STORED);
        builder.new_attribute("beta", STORED | INDEXED);
        builder.new_attribute("gamma", INDEXED);
        let schema = builder.build();

        let mut buffer = Vec::new();

        schema.write_to(&mut buffer)?;
        let schema2 = Schema::read_from(buffer.as_slice())?;

        assert_eq!(schema, schema2);

        Ok(())
    }
}

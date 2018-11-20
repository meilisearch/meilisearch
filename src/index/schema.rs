use std::io::{Read, Write};
use std::error::Error;
use std::path::Path;
use std::ops::BitOr;
use std::fs::File;
use std::fmt;

pub const STORED: SchemaProps = SchemaProps { stored: true, indexed: false };
pub const INDEXED: SchemaProps = SchemaProps { stored: false, indexed: true };

#[derive(Copy, Clone)]
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
    fields: Vec<(String, SchemaProps)>,
}

impl SchemaBuilder {
    pub fn new() -> SchemaBuilder {
        SchemaBuilder { fields: Vec::new() }
    }

    pub fn field<N>(&mut self, name: N, props: SchemaProps) -> SchemaField
    where N: Into<String>,
    {
        let len = self.fields.len();
        let name = name.into();
        self.fields.push((name, props));

        SchemaField(len as u32)
    }

    pub fn build(self) -> Schema {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct Schema;

impl Schema {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Schema, Box<Error>> {
        let file = File::open(path)?;
        Schema::read_from(file)
    }

    pub fn read_from<R: Read>(reader: R) -> Result<Schema, Box<Error>> {
        unimplemented!()
    }

    pub fn write_to<W: Write>(writer: W) -> Result<(), Box<Error>> {
        unimplemented!()
    }

    pub fn props(&self, field: SchemaField) -> SchemaProps {
        unimplemented!()
    }

    pub fn field(&self, name: &str) -> Option<SchemaField> {
        unimplemented!()
    }
}

#[derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct SchemaField(u32);

impl SchemaField {
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for SchemaField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

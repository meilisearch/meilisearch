use std::error::Error;
use std::path::Path;
use std::ops::BitOr;
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

pub struct SchemaBuilder;

impl SchemaBuilder {
    pub fn new() -> SchemaBuilder {
        unimplemented!()
    }

    pub fn field(&mut self, name: &str, props: SchemaProps) -> SchemaField {
        unimplemented!()
    }

    pub fn build(self) -> Schema {
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

#[derive(Clone)]
pub struct Schema;

impl Schema {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Schema, Box<Error>> {
        unimplemented!()
    }

    pub fn props(&self, field: SchemaField) -> SchemaProps {
        unimplemented!()
    }

    pub fn field(&self, name: &str) -> Option<SchemaField> {
        unimplemented!()
    }
}

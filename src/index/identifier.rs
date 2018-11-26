use std::io::Write;

use byteorder::{NetworkEndian, WriteBytesExt};

use crate::index::schema::SchemaAttr;
use crate::DocumentId;

pub struct Identifier {
    inner: Vec<u8>,
}

impl Identifier {
    pub fn data() -> Data {
        let mut inner = Vec::new();
        let _ = inner.write(b"data");
        Data { inner }
    }

    pub fn document(id: DocumentId) -> Document {
        let mut inner = Vec::new();
        let _ = inner.write(b"docu");
        let _ = inner.write(b"-");
        let _ = inner.write_u64::<NetworkEndian>(id);
        Document { inner }
    }
}

pub struct Data {
    inner: Vec<u8>,
}

impl Data {
    pub fn index(mut self) -> Self {
        let _ = self.inner.write(b"-");
        let _ = self.inner.write(b"index");
        self
    }

    pub fn schema(mut self) -> Self {
        let _ = self.inner.write(b"-");
        let _ = self.inner.write(b"schema");
        self
    }

    pub fn build(self) -> Vec<u8> {
        self.inner
    }
}

pub struct Document {
    inner: Vec<u8>,
}

impl Document {
    pub fn attribute(mut self, attr: SchemaAttr) -> Self {
        let _ = self.inner.write(b"-");
        let _ = self.inner.write_u32::<NetworkEndian>(attr.as_u32());
        self
    }

    pub fn build(self) -> Vec<u8> {
        self.inner
    }
}

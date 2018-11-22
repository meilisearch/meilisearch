use std::io::Write;

use byteorder::{NetworkEndian, WriteBytesExt};

use crate::index::schema::SchemaAttr;
use crate::blob::BlobName;
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

    pub fn blob(name: BlobName) -> Blob {
        let mut inner = Vec::new();
        let _ = inner.write(b"blob");
        let _ = inner.write(name.as_bytes());
        Blob { inner }
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
    pub fn blobs_order(mut self) -> Self {
        let _ = self.inner.write(b"-");
        let _ = self.inner.write(b"blobs-order");
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

pub struct Blob {
    inner: Vec<u8>,
}

impl Blob {
    pub fn document_indexes(mut self) -> Self {
        let _ = self.inner.write(b"-");
        let _ = self.inner.write(b"doc-idx");
        self
    }

    pub fn document_ids(mut self) -> Self {
        let _ = self.inner.write(b"-");
        let _ = self.inner.write(b"doc-ids");
        self
    }

    pub fn fst_map(mut self) -> Self {
        let _ = self.inner.write(b"-");
        let _ = self.inner.write(b"fst");
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

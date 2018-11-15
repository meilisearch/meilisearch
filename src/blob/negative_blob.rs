use std::error::Error;
use std::path::Path;
use std::io::Write;

use crate::DocumentId;
use crate::data::{DocIds, DocIdsBuilder};

pub struct NegativeBlob {
    doc_ids: DocIds,
}

impl NegativeBlob {
    pub unsafe fn from_path<P>(doc_ids: P) -> Result<Self, Box<Error>>
    where P: AsRef<Path>,
    {
        let doc_ids = DocIds::from_path(doc_ids)?;
        Ok(NegativeBlob { doc_ids })
    }

    pub fn from_bytes(doc_ids: Vec<u8>) -> Result<Self, Box<Error>> {
        let doc_ids = DocIds::from_bytes(doc_ids)?;
        Ok(NegativeBlob { doc_ids })
    }

    pub fn as_ids(&self) -> &DocIds {
        &self.doc_ids
    }

    pub fn into_doc_ids(self) -> DocIds {
        self.doc_ids
    }
}

pub struct NegativeBlobBuilder<W> {
    doc_ids: DocIdsBuilder<W>,
}

impl<W: Write> NegativeBlobBuilder<W> {
    pub fn new(wrt: W) -> Self {
        Self { doc_ids: DocIdsBuilder::new(wrt) }
    }

    pub fn insert(&mut self, doc: DocumentId) -> bool {
        self.doc_ids.insert(doc)
    }

    pub fn finish(self) -> Result<(), Box<Error>> {
        self.into_inner().map(|_| ())
    }

    pub fn into_inner(self) -> Result<W, Box<Error>> {
        // FIXME insert a magic number that indicates if the endianess
        //       of the input is the same as the machine that is reading it.
        Ok(self.doc_ids.into_inner()?)
    }
}

impl NegativeBlobBuilder<Vec<u8>> {
    pub fn build(self) -> Result<NegativeBlob, Box<Error>> {
        self.into_inner().and_then(|ids| NegativeBlob::from_bytes(ids))
    }
}

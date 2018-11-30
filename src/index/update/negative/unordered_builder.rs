use std::collections::BTreeSet;
use std::io;

use byteorder::{NativeEndian, WriteBytesExt};

use crate::DocumentId;

pub struct UnorderedNegativeBlobBuilder<W> {
    doc_ids: BTreeSet<DocumentId>, // TODO: prefer a linked-list
    wrt: W,
}

impl UnorderedNegativeBlobBuilder<Vec<u8>> {
    pub fn memory() -> Self {
        UnorderedNegativeBlobBuilder::new(Vec::new())
    }
}

impl<W: io::Write> UnorderedNegativeBlobBuilder<W> {
    pub fn new(wrt: W) -> Self {
        Self {
            doc_ids: BTreeSet::new(),
            wrt: wrt,
        }
    }

    pub fn insert(&mut self, doc: DocumentId) -> bool {
        self.doc_ids.insert(doc)
    }

    pub fn into_inner(mut self) -> io::Result<W> {
        for id in self.doc_ids {
            self.wrt.write_u64::<NativeEndian>(id)?;
        }
        Ok(self.wrt)
    }
}

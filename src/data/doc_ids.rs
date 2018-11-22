use std::collections::BTreeSet;
use std::slice::from_raw_parts;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::{io, mem};

use byteorder::{NativeEndian, WriteBytesExt};
use fst::raw::MmapReadOnly;

use crate::DocumentId;
use crate::data::Data;

#[derive(Clone)]
pub struct DocIds {
    doc_ids: Data,
}

impl DocIds {
    pub unsafe fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mmap = MmapReadOnly::open_path(path)?;
        let doc_ids = Data::Mmap(mmap);
        Ok(DocIds { doc_ids })
    }

    pub fn from_bytes(vec: Vec<u8>) -> Result<Self, Box<Error>> {
        // FIXME check if modulo DocumentId
        let len = vec.len();
        let doc_ids = Data::Shared {
            vec: Arc::new(vec),
            offset: 0,
            len: len
        };
        Ok(DocIds { doc_ids })
    }

    pub fn contains(&self, doc: DocumentId) -> bool {
        // FIXME prefer using the sdset::exponential_search function
        self.doc_ids().binary_search(&doc).is_ok()
    }

    pub fn doc_ids(&self) -> &[DocumentId] {
        let slice = &self.doc_ids;
        let ptr = slice.as_ptr() as *const DocumentId;
        let len = slice.len() / mem::size_of::<DocumentId>();
        unsafe { from_raw_parts(ptr, len) }
    }
}

pub struct DocIdsBuilder<W> {
    doc_ids: BTreeSet<DocumentId>, // TODO: prefer a linked-list
    wrt: W,
}

impl<W: io::Write> DocIdsBuilder<W> {
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

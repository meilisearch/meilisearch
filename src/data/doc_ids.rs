use std::slice::from_raw_parts;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::{io, mem};

use sdset::Set;
use fst::raw::MmapReadOnly;
use serde::ser::{Serialize, Serializer};

use crate::DocumentId;
use crate::data::Data;

#[derive(Default, Clone)]
pub struct DocIds {
    data: Data,
}

impl DocIds {
    pub unsafe fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mmap = MmapReadOnly::open_path(path)?;
        let data = Data::Mmap(mmap);
        Ok(DocIds { data })
    }

    pub fn from_bytes(vec: Vec<u8>) -> Result<Self, Box<Error>> {
        // FIXME check if modulo DocumentId
        let len = vec.len();
        let data = Data::Shared {
            bytes: Arc::new(vec),
            offset: 0,
            len: len
        };
        Ok(DocIds { data })
    }

    pub fn from_document_ids(vec: Vec<DocumentId>) -> Self {
        DocIds::from_bytes(unsafe { mem::transmute(vec) }).unwrap()
    }

    pub fn contains(&self, doc: DocumentId) -> bool {
        // FIXME prefer using the sdset::exponential_search function
        self.doc_ids().binary_search(&doc).is_ok()
    }

    pub fn doc_ids(&self) -> &Set<DocumentId> {
        let slice = &self.data;
        let ptr = slice.as_ptr() as *const DocumentId;
        let len = slice.len() / mem::size_of::<DocumentId>();
        let slice = unsafe { from_raw_parts(ptr, len) };
        Set::new_unchecked(slice)
    }
}

impl Serialize for DocIds {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.as_ref().serialize(serializer)
    }
}

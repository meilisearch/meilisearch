use std::slice::from_raw_parts;
use std::sync::Arc;
use std::{io, mem};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use sdset::Set;

use crate::DocumentId;
use crate::data::SharedData;

#[derive(Default, Clone)]
pub struct DocIds {
    data: SharedData,
}

impl DocIds {
    pub fn empty() -> Self {
        DocIds { data: SharedData::empty() }
    }

    pub fn from_bytes(vec: Vec<u8>) -> io::Result<Self> {
        let len = vec.len();
        DocIds::from_shared_bytes(Arc::new(vec), 0, len)
    }

    pub fn from_shared_bytes(bytes: Arc<Vec<u8>>, offset: usize, len: usize) -> io::Result<Self> {
        let data = SharedData { bytes, offset, len };
        DocIds::from_data(data)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    fn from_data(data: SharedData) -> io::Result<Self> {
        let len = data.as_ref().read_u64::<LittleEndian>()?;
        let data = data.range(mem::size_of::<u64>(), len as usize);
        Ok(DocIds { data })
    }

    pub fn from_raw(vec: Vec<DocumentId>) -> Self {
        DocIds::from_bytes(unsafe { mem::transmute(vec) }).unwrap()
    }

    pub fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        let len = self.data.len() as u64;
        bytes.write_u64::<LittleEndian>(len).unwrap();
        bytes.extend_from_slice(&self.data);
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

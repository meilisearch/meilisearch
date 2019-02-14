mod doc_ids;
mod doc_indexes;

use std::slice::from_raw_parts;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

pub use self::doc_ids::DocIds;
pub use self::doc_indexes::{DocIndexes, DocIndexesBuilder};

#[derive(Clone, Default)]
pub struct SharedData {
    pub bytes: Arc<Vec<u8>>,
    pub offset: usize,
    pub len: usize,
}

impl SharedData {
    pub fn from_bytes(vec: Vec<u8>) -> SharedData {
        let len = vec.len();
        let bytes = Arc::from(vec);
        SharedData::new(bytes, 0, len)
    }

    pub fn new(bytes: Arc<Vec<u8>>, offset: usize, len: usize) -> SharedData {
        SharedData { bytes, offset, len }
    }

    pub fn range(&self, offset: usize, len: usize) -> SharedData {
        assert!(offset + len <= self.len);
        SharedData {
            bytes: self.bytes.clone(),
            offset: self.offset + offset,
            len: len,
        }
    }
}

impl Deref for SharedData {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<[u8]> for SharedData {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[self.offset..self.offset + self.len]
    }
}

unsafe fn into_u8_slice<T: Sized>(slice: &[T]) -> &[u8] {
    let ptr = slice.as_ptr() as *const u8;
    let len = slice.len() * size_of::<T>();
    from_raw_parts(ptr, len)
}

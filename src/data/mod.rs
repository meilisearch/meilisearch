mod doc_ids;
mod doc_indexes;

use std::ops::Deref;
use std::sync::Arc;

pub use self::doc_ids::DocIds;
pub use self::doc_indexes::{DocIndexes, DocIndexesBuilder};

#[derive(Clone)]
struct SharedData {
    bytes: Arc<Vec<u8>>,
    offset: usize,
    len: usize,
}

impl SharedData {
    pub fn range(&self, offset: usize, len: usize) -> SharedData {
        assert!(offset + len <= self.len);
        SharedData {
            bytes: self.bytes.clone(),
            offset: self.offset + offset,
            len: len,
        }
    }
}

impl Default for SharedData {
    fn default() -> SharedData {
        SharedData {
            bytes: Arc::default(),
            offset: 0,
            len: 0,
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

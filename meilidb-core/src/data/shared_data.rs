use std::sync::Arc;
use std::ops::Deref;

#[derive(Clone)]
pub struct SharedData {
    pub bytes: Arc<[u8]>,
    pub offset: usize,
    pub len: usize,
}

impl SharedData {
    pub fn from_bytes(vec: Vec<u8>) -> SharedData {
        let len = vec.len();
        let bytes = Arc::from(vec);
        SharedData::new(bytes, 0, len)
    }

    pub fn new(bytes: Arc<[u8]>, offset: usize, len: usize) -> SharedData {
        SharedData { bytes, offset, len }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[self.offset..self.offset + self.len]
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

impl Default for SharedData {
    fn default() -> SharedData {
        SharedData {
            bytes: Arc::from(Vec::new()),
            offset: 0,
            len: 0,
        }
    }
}

impl Deref for SharedData {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u8]> for SharedData {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

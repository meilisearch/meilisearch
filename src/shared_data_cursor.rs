use std::io::{self, Read, Cursor, BufRead};
use std::sync::Arc;
use crate::data::SharedData;

pub struct SharedDataCursor(Cursor<SharedData>);

impl SharedDataCursor {
    pub fn from_bytes(bytes: Vec<u8>) -> SharedDataCursor {
        let len = bytes.len();
        let bytes = Arc::new(bytes);

        SharedDataCursor::from_shared_bytes(bytes, 0, len)
    }

    pub fn from_shared_bytes(bytes: Arc<Vec<u8>>, offset: usize, len: usize) -> SharedDataCursor {
        let data = SharedData::new(bytes, offset, len);
        let cursor = Cursor::new(data);

        SharedDataCursor(cursor)
    }

    pub fn extract(&mut self, amt: usize) -> SharedData {
        let offset = self.0.position() as usize;
        let extracted = self.0.get_ref().range(offset, amt);
        self.0.consume(amt);

        extracted
    }
}

impl Read for SharedDataCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl BufRead for SharedDataCursor {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.0.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.0.consume(amt)
    }
}

pub trait FromSharedDataCursor: Sized {
    type Error;

    fn from_shared_data_cursor(cursor: &mut SharedDataCursor) -> Result<Self, Self::Error>;

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let mut cursor = SharedDataCursor::from_bytes(bytes);
        Self::from_shared_data_cursor(&mut cursor)
    }
}

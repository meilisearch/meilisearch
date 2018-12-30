use std::io::{Cursor, BufRead};
use std::error::Error;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

use sdset::Set;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::data::DocIds;
use crate::DocumentId;

#[derive(Default)]
pub struct Negative {
    pub doc_ids: DocIds,
}

impl Negative {
    pub fn from_shared_bytes(
        bytes: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    ) -> Result<(Negative, usize), Box<Error>>
    {
        let mut cursor = Cursor::new(&bytes[..len]);
        cursor.consume(offset);

        let len = cursor.read_u64::<LittleEndian>()? as usize;
        let offset = cursor.position() as usize;
        let doc_ids = DocIds::from_shared_bytes(bytes, offset, len)?;

        Ok((Negative { doc_ids }, offset + len))
    }

    pub fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        let slice = self.doc_ids.as_bytes();
        let len = slice.len() as u64;
        let _ = bytes.write_u64::<LittleEndian>(len);
        bytes.extend_from_slice(slice);
    }

    pub fn is_empty(&self) -> bool {
        self.doc_ids.doc_ids().is_empty()
    }
}

impl Deref for Negative {
    type Target = Set<DocumentId>;

    fn deref(&self) -> &Self::Target {
        self.doc_ids.doc_ids()
    }
}

use std::error::Error;
use std::io::Cursor;
use std::ops::Deref;

use sdset::Set;
use byteorder::{LittleEndian, WriteBytesExt};

use crate::data::SharedData;
use crate::data::DocIds;
use crate::DocumentId;

#[derive(Default)]
pub struct Negative(DocIds);

impl Negative {
    pub fn new(doc_ids: DocIds) -> Negative {
        Negative(doc_ids)
    }

    pub fn from_cursor(cursor: &mut Cursor<SharedData>) -> Result<Negative, Box<Error>> {
        let doc_ids = DocIds::from_cursor(cursor)?;
        Ok(Negative(doc_ids))
    }

    pub fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        let slice = self.0.as_bytes();
        let len = slice.len() as u64;
        let _ = bytes.write_u64::<LittleEndian>(len);
        bytes.extend_from_slice(slice);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Deref for Negative {
    type Target = Set<DocumentId>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

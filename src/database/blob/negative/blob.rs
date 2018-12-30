use std::io::{Cursor, BufRead};
use std::error::Error;
use std::sync::Arc;
use std::fmt;

use sdset::Set;
use byteorder::{LittleEndian, ReadBytesExt};

use crate::data::DocIds;
use crate::DocumentId;

#[derive(Default)]
pub struct NegativeBlob {
    doc_ids: DocIds,
}

impl NegativeBlob {
    pub fn from_bytes(doc_ids: Vec<u8>) -> Result<Self, Box<Error>> {
        let doc_ids = DocIds::from_bytes(doc_ids)?;
        Ok(NegativeBlob { doc_ids })
    }

    pub fn from_shared_bytes(bytes: Arc<Vec<u8>>, offset: usize, len: usize) -> Result<Self, Box<Error>> {
        let mut cursor = Cursor::new(&bytes.as_slice()[..len]);
        cursor.consume(offset);

        let len = cursor.read_u64::<LittleEndian>()? as usize;
        let offset = cursor.position() as usize;
        let doc_ids = DocIds::from_shared_bytes(bytes, offset, len)?;

        Ok(NegativeBlob::from_raw(doc_ids))
    }

    pub fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        self.doc_ids.write_to_bytes(bytes)
    }

    pub fn from_raw(doc_ids: DocIds) -> Self {
        NegativeBlob { doc_ids }
    }

    pub fn as_ids(&self) -> &DocIds {
        &self.doc_ids
    }

    pub fn into_doc_ids(self) -> DocIds {
        self.doc_ids
    }
}

impl AsRef<Set<DocumentId>> for NegativeBlob {
    fn as_ref(&self) -> &Set<DocumentId> {
        self.as_ids().doc_ids()
    }
}

impl fmt::Debug for NegativeBlob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NegativeBlob(")?;
        f.debug_list().entries(self.as_ref().as_slice()).finish()?;
        write!(f, ")")
    }
}

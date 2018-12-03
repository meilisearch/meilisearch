use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::error::Error;

use byteorder::{NetworkEndian, WriteBytesExt};

use crate::index::schema::SchemaAttr;
use crate::DocumentId;

mod negative;
mod positive;

pub use self::positive::{PositiveUpdateBuilder, NewState};
pub use self::negative::NegativeUpdateBuilder;

const DOC_KEY_LEN:      usize = 4 + std::mem::size_of::<u64>();
const DOC_KEY_ATTR_LEN: usize = DOC_KEY_LEN + 1 + std::mem::size_of::<u32>();

pub struct Update {
    path: PathBuf,
    can_be_moved: bool,
}

impl Update {
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Update, Box<Error>> {
        Ok(Update { path: path.into(), can_be_moved: false })
    }

    pub fn open_and_move<P: Into<PathBuf>>(path: P) -> Result<Update, Box<Error>> {
        Ok(Update { path: path.into(), can_be_moved: true })
    }

    pub fn can_be_moved(&self) -> bool {
        self.can_be_moved
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.path
    }
}

// "doc-{ID_8_BYTES}"
fn raw_document_key(id: DocumentId) -> [u8; DOC_KEY_LEN] {
    let mut key = [0; DOC_KEY_LEN];

    let mut wtr = Cursor::new(&mut key[..]);
    wtr.write_all(b"doc-").unwrap();
    wtr.write_u64::<NetworkEndian>(id).unwrap();

    key
}

// "doc-{ID_8_BYTES}-{ATTR_4_BYTES}"
fn raw_document_key_attr(id: DocumentId, attr: SchemaAttr) -> [u8; DOC_KEY_ATTR_LEN] {
    let mut key = [0; DOC_KEY_ATTR_LEN];
    let raw_key = raw_document_key(id);

    let mut wtr = Cursor::new(&mut key[..]);
    wtr.write_all(&raw_key).unwrap();
    wtr.write_all(b"-").unwrap();
    wtr.write_u32::<NetworkEndian>(attr.as_u32()).unwrap();

    key
}

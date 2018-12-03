use std::io::{Cursor, Read, Write};
use std::mem::size_of;
use std::fmt;

use byteorder::{NativeEndian, WriteBytesExt, ReadBytesExt};

use crate::index::schema::SchemaAttr;
use crate::DocumentId;

const DOC_KEY_LEN:      usize = 4 + size_of::<u64>();
const DOC_KEY_ATTR_LEN: usize = DOC_KEY_LEN + 1 + size_of::<u32>();

#[derive(Copy, Clone)]
pub struct DocumentKey([u8; DOC_KEY_LEN]);

impl DocumentKey {
    pub fn new(id: DocumentId) -> DocumentKey {
        let mut buffer = [0; DOC_KEY_LEN];

        let mut wtr = Cursor::new(&mut buffer[..]);
        wtr.write_all(b"doc-").unwrap();
        wtr.write_u64::<NativeEndian>(id).unwrap();

        DocumentKey(buffer)
    }

    pub fn from_bytes(mut bytes: &[u8]) -> DocumentKey {
        assert!(bytes.len() >= DOC_KEY_LEN);
        assert_eq!(&bytes[..4], b"doc-");

        let mut buffer = [0; DOC_KEY_LEN];
        bytes.read_exact(&mut buffer).unwrap();

        DocumentKey(buffer)
    }

    pub fn with_attribute(&self, attr: SchemaAttr) -> DocumentKeyAttr {
        DocumentKeyAttr::new(self.document_id(), attr)
    }

    pub fn document_id(&self) -> DocumentId {
        (&self.0[4..]).read_u64::<NativeEndian>().unwrap()
    }
}

impl AsRef<[u8]> for DocumentKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for DocumentKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DocumentKey")
            .field("document_id", &self.document_id())
            .finish()
    }
}

#[derive(Copy, Clone)]
pub struct DocumentKeyAttr([u8; DOC_KEY_ATTR_LEN]);

impl DocumentKeyAttr {
    pub fn new(id: DocumentId, attr: SchemaAttr) -> DocumentKeyAttr {
        let mut buffer = [0; DOC_KEY_ATTR_LEN];
        let DocumentKey(raw_key) = DocumentKey::new(id);

        let mut wtr = Cursor::new(&mut buffer[..]);
        wtr.write_all(&raw_key).unwrap();
        wtr.write_all(b"-").unwrap();
        wtr.write_u32::<NativeEndian>(attr.as_u32()).unwrap();

        DocumentKeyAttr(buffer)
    }

    pub fn from_bytes(mut bytes: &[u8]) -> DocumentKeyAttr {
        assert!(bytes.len() >= DOC_KEY_ATTR_LEN);
        assert_eq!(&bytes[..4], b"doc-");

        let mut buffer = [0; DOC_KEY_ATTR_LEN];
        bytes.read_exact(&mut buffer).unwrap();

        DocumentKeyAttr(buffer)
    }

    pub fn document_id(&self) -> DocumentId {
        (&self.0[4..]).read_u64::<NativeEndian>().unwrap()
    }

    pub fn attribute(&self) -> SchemaAttr {
        let offset = 4 + size_of::<u64>() + 1;
        let value = (&self.0[offset..]).read_u32::<NativeEndian>().unwrap();
        SchemaAttr::new(value)
    }

    pub fn into_document_key(self) -> DocumentKey {
        DocumentKey::new(self.document_id())
    }
}

impl AsRef<[u8]> for DocumentKeyAttr {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for DocumentKeyAttr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DocumentKeyAttr")
            .field("document_id", &self.document_id())
            .field("attribute", &self.attribute().as_u32())
            .finish()
    }
}

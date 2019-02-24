use std::io::{Cursor, Read, Write};
use std::mem::size_of;
use std::fmt;

use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

use crate::database::schema::SchemaAttr;
use meilidb_core::DocumentId;

const DOC_KEY_LEN:      usize = 4 + size_of::<u64>();
const DOC_KEY_ATTR_LEN: usize = DOC_KEY_LEN + 1 + size_of::<u16>();

#[derive(Copy, Clone)]
pub struct DocumentKey([u8; DOC_KEY_LEN]);

impl DocumentKey {
    pub fn new(id: DocumentId) -> DocumentKey {
        let mut buffer = [0; DOC_KEY_LEN];

        let mut wtr = Cursor::new(&mut buffer[..]);
        wtr.write_all(b"doc-").unwrap();
        wtr.write_u64::<BigEndian>(id.0).unwrap();

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

    pub fn with_attribute_min(&self) -> DocumentKeyAttr {
        DocumentKeyAttr::new(self.document_id(), SchemaAttr::min())
    }

    pub fn with_attribute_max(&self) -> DocumentKeyAttr {
        DocumentKeyAttr::new(self.document_id(), SchemaAttr::max())
    }

    pub fn document_id(&self) -> DocumentId {
        let id = (&self.0[4..]).read_u64::<BigEndian>().unwrap();
        DocumentId(id)
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

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DocumentKeyAttr([u8; DOC_KEY_ATTR_LEN]);

impl DocumentKeyAttr {
    pub fn new(id: DocumentId, attr: SchemaAttr) -> DocumentKeyAttr {
        let mut buffer = [0; DOC_KEY_ATTR_LEN];
        let DocumentKey(raw_key) = DocumentKey::new(id);

        let mut wtr = Cursor::new(&mut buffer[..]);
        wtr.write_all(&raw_key).unwrap();
        wtr.write_all(b"-").unwrap();
        wtr.write_u16::<BigEndian>(attr.0).unwrap();

        DocumentKeyAttr(buffer)
    }

    pub fn with_attribute_min(id: DocumentId) -> DocumentKeyAttr {
        DocumentKeyAttr::new(id, SchemaAttr::min())
    }

    pub fn with_attribute_max(id: DocumentId) -> DocumentKeyAttr {
        DocumentKeyAttr::new(id, SchemaAttr::max())
    }

    pub fn from_bytes(mut bytes: &[u8]) -> DocumentKeyAttr {
        assert!(bytes.len() >= DOC_KEY_ATTR_LEN);
        assert_eq!(&bytes[..4], b"doc-");

        let mut buffer = [0; DOC_KEY_ATTR_LEN];
        bytes.read_exact(&mut buffer).unwrap();

        DocumentKeyAttr(buffer)
    }

    pub fn document_id(&self) -> DocumentId {
        let id = (&self.0[4..]).read_u64::<BigEndian>().unwrap();
        DocumentId(id)
    }

    pub fn attribute(&self) -> SchemaAttr {
        let offset = 4 + size_of::<u64>() + 1;
        let value = (&self.0[offset..]).read_u16::<BigEndian>().unwrap();
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
            .field("attribute", &self.attribute().0)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_as_ref_order() {
        for (a, b) in (0..).zip(1..).take(u16::max_value() as usize - 1) {
            let id = DocumentId(0);
            let a = DocumentKeyAttr::new(id, SchemaAttr(a));
            let b = DocumentKeyAttr::new(id, SchemaAttr(b));

            assert!(a < b);
            assert!(a.as_ref() < b.as_ref());
        }
    }
}

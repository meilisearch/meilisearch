use meilidb_core::DocumentId;
use crate::schema::SchemaAttr;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocumentAttrKey {
    pub document_id: DocumentId,
    pub attribute: SchemaAttr,
}

impl DocumentAttrKey {
    pub fn new(document_id: DocumentId, attribute: SchemaAttr) -> DocumentAttrKey {
        DocumentAttrKey { document_id, attribute }
    }

    pub fn to_be_bytes(self) -> [u8; 10] {
        let mut output = [0u8; 10];

        let document_id = self.document_id.0.to_be_bytes();
        let attribute = self.attribute.0.to_be_bytes();

        unsafe {
            use std::{mem::size_of, ptr::copy_nonoverlapping};

            let output = output.as_mut_ptr();
            copy_nonoverlapping(document_id.as_ptr(), output, size_of::<u64>());

            let output = output.add(size_of::<u64>());
            copy_nonoverlapping(attribute.as_ptr(), output, size_of::<u16>());
        }

        output
    }

    pub fn from_be_bytes(bytes: [u8; 10]) -> DocumentAttrKey {
        let document_id;
        let attribute;

        unsafe {
            use std::ptr::read_unaligned;

            let pointer = bytes.as_ptr() as *const _;
            let document_id_bytes = read_unaligned(pointer);
            document_id = u64::from_be_bytes(document_id_bytes);

            let pointer = pointer.add(1) as *const _;
            let attribute_bytes = read_unaligned(pointer);
            attribute = u16::from_be_bytes(attribute_bytes);
        }

        DocumentAttrKey {
            document_id: DocumentId(document_id),
            attribute: SchemaAttr(attribute),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_from_be_bytes() {
        let document_id = DocumentId(67578308);
        let schema_attr = SchemaAttr(3456);
        let x = DocumentAttrKey::new(document_id, schema_attr);

        assert_eq!(x, DocumentAttrKey::from_be_bytes(x.to_be_bytes()));
    }
}

use std::sync::Arc;
use std::convert::TryInto;

use meilidb_core::DocumentId;
use sled::IVec;

use crate::document_attr_key::DocumentAttrKey;
use crate::schema::SchemaAttr;

#[derive(Clone)]
pub struct DocumentsIndex(pub(crate) Arc<sled::Tree>);

impl DocumentsIndex {
    pub fn document_field(&self, id: DocumentId, attr: SchemaAttr) -> sled::Result<Option<IVec>> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.get(key)
    }

    pub fn set_document_field(&self, id: DocumentId, attr: SchemaAttr, value: Vec<u8>) -> sled::Result<()> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.set(key, value)?;
        Ok(())
    }

    pub fn del_document_field(&self, id: DocumentId, attr: SchemaAttr) -> sled::Result<()> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.del(key)?;
        Ok(())
    }

    pub fn del_all_document_fields(&self, id: DocumentId) -> sled::Result<()> {
        let start = DocumentAttrKey::new(id, SchemaAttr::min()).to_be_bytes();
        let end = DocumentAttrKey::new(id, SchemaAttr::max()).to_be_bytes();
        let document_attrs = self.0.range(start..=end).keys();

        for key in document_attrs {
            self.0.del(key?)?;
        }

        Ok(())
    }

    pub fn document_fields(&self, id: DocumentId) -> DocumentFieldsIter {
        let start = DocumentAttrKey::new(id, SchemaAttr::min());
        let start = start.to_be_bytes();

        let end = DocumentAttrKey::new(id, SchemaAttr::max());
        let end = end.to_be_bytes();

        DocumentFieldsIter(self.0.range(start..=end))
    }
}

pub struct DocumentFieldsIter<'a>(sled::Iter<'a>);

impl<'a> Iterator for DocumentFieldsIter<'a> {
    type Item = sled::Result<(SchemaAttr, IVec)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Ok((key, value))) => {
                let slice: &[u8] = key.as_ref();
                let array = slice.try_into().unwrap();
                let key = DocumentAttrKey::from_be_bytes(array);
                Some(Ok((key.attribute, value)))
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

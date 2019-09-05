use std::sync::Arc;
use std::convert::TryInto;
use std::ops::Bound;

use meilidb_core::DocumentId;
use meilidb_schema::SchemaAttr;

use crate::document_attr_key::DocumentAttrKey;

fn document_fields_range(id: DocumentId) -> (Bound<[u8; 10]>, Bound<[u8; 10]>) {
    let start = DocumentAttrKey::new(id, SchemaAttr::min()).to_be_bytes();
    let end   = DocumentAttrKey::new(id, SchemaAttr::max()).to_be_bytes();

    (Bound::Included(start), Bound::Included(end))
}

#[derive(Clone)]
pub struct DocumentsIndex(pub(crate) Arc<sled::Tree>);

impl DocumentsIndex {
    pub fn document_field(&self, id: DocumentId, attr: SchemaAttr) -> sled::Result<Option<sled::IVec>> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.get(key)
    }

    pub fn set_document_field(&self, id: DocumentId, attr: SchemaAttr, value: Vec<u8>) -> sled::Result<()> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.insert(key, value)?;
        Ok(())
    }

    pub fn del_document_field(&self, id: DocumentId, attr: SchemaAttr) -> sled::Result<()> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.remove(key)?;
        Ok(())
    }

    pub fn del_all_document_fields(&self, id: DocumentId) -> sled::Result<()> {
        let range = document_fields_range(id);

        for result in self.0.range(range) {
            let (key, _) = result?;
            self.0.remove(key)?;
        }

        Ok(())
    }

    pub fn document_fields(&self, id: DocumentId) -> DocumentFieldsIter {
        let range = document_fields_range(id);

        let iter = self.0.range(range);
        DocumentFieldsIter(iter)
    }

    pub fn len(&self) -> sled::Result<usize> {
        let mut last_document_id = None;
        let mut count = 0;

        for result in self.0.iter() {
            let (key, _) = result?;
            let array = key.as_ref().try_into().unwrap();
            let document_id = DocumentAttrKey::from_be_bytes(array).document_id;

            if Some(document_id) != last_document_id {
                last_document_id = Some(document_id);
                count += 1;
            }
        }

        Ok(count)
    }
}

pub struct DocumentFieldsIter<'a>(sled::Iter<'a>);

impl Iterator for DocumentFieldsIter<'_> {
    type Item = sled::Result<(SchemaAttr, sled::IVec)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Ok((key, value))) => {
                let array = key.as_ref().try_into().unwrap();
                let key = DocumentAttrKey::from_be_bytes(array);
                Some(Ok((key.attribute, value)))
            },
            Some(Err(e)) => return Some(Err(e)),
            None => None,
        }
    }
}

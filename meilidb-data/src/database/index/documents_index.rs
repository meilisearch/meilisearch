use std::convert::TryInto;

use meilidb_core::DocumentId;
use meilidb_schema::SchemaAttr;
use rocksdb::DBVector;

use crate::document_attr_key::DocumentAttrKey;
use crate::RocksDbResult;

fn document_fields_range(id: DocumentId) -> ([u8; 10], [u8; 10]) {
    let start = DocumentAttrKey::new(id, SchemaAttr::min()).to_be_bytes();
    let end   = DocumentAttrKey::new(id, SchemaAttr::max()).to_be_bytes();

    (start, end)
}

#[derive(Clone)]
pub struct DocumentsIndex(pub(crate) crate::CfTree);

impl DocumentsIndex {
    pub fn document_field(&self, id: DocumentId, attr: SchemaAttr) -> RocksDbResult<Option<DBVector>> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.get(key)
    }

    pub fn set_document_field(&self, id: DocumentId, attr: SchemaAttr, value: Vec<u8>) -> RocksDbResult<()> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.insert(key, value)?;
        Ok(())
    }

    pub fn del_document_field(&self, id: DocumentId, attr: SchemaAttr) -> RocksDbResult<()> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.remove(key)?;
        Ok(())
    }

    pub fn del_all_document_fields(&self, id: DocumentId) -> RocksDbResult<usize> {
        let (start, end) = document_fields_range(id);

        let mut count = 0;
        for (key, _) in self.0.range(start, end)? {
            self.0.remove(key)?;
            count += 1;
        }

        Ok(count)
    }

    pub fn document_fields(&self, id: DocumentId) -> RocksDbResult<DocumentFieldsIter> {
        let (start, end) = document_fields_range(id);

        let iter = self.0.range(start, end)?;
        Ok(DocumentFieldsIter(iter))
    }

    pub fn len(&self) -> RocksDbResult<u64> {
        let mut last_document_id = None;
        let mut count = 0;

        for (key, _) in self.0.iter()? {
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

pub struct DocumentFieldsIter<'a>(crate::CfIter<'a>);

impl Iterator for DocumentFieldsIter<'_> {
    type Item = (SchemaAttr, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some((key, value)) => {
                let array = key.as_ref().try_into().unwrap();
                let key = DocumentAttrKey::from_be_bytes(array);
                Some((key.attribute, value))
            },
            None => None,
        }
    }
}

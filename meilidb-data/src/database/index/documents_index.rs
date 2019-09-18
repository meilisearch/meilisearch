use std::convert::TryInto;
use std::collections::HashMap;

use meilidb_core::DocumentId;
use meilidb_schema::{Schema, SchemaAttr};
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

    pub fn documents_ids(&self) -> RocksDbResult<DocumentsIdsIter> {
        let iter = DocumentsKeysIter(self.0.iter()?);
        Ok(DocumentsIdsIter { inner: iter, last: None })
    }

    pub fn documents_fields_repartition(&self, schema: Schema) -> RocksDbResult<HashMap<String, u64>> {
        let iter = self.0.iter()?;
        let mut repartition_attributes_id = HashMap::new();
        for key in DocumentsKeysIter(iter) {
            let counter = repartition_attributes_id.entry(key.attribute).or_insert(0);
            *counter += 1u64;
        }
        let mut repartition_with_attribute_name = HashMap::new();
        for (key, val) in repartition_attributes_id {
            repartition_with_attribute_name.insert(schema.attribute_name(key).to_owned(), val);
        }
        Ok(repartition_with_attribute_name)
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

pub struct DocumentsKeysIter<'a>(crate::CfIter<'a>);

impl Iterator for DocumentsKeysIter<'_> {
    type Item = DocumentAttrKey;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some((key, _)) => {
                let array = key.as_ref().try_into().unwrap();
                let key = DocumentAttrKey::from_be_bytes(array);
                Some(key)
            },
            None => None,
        }
    }
}

pub struct DocumentsIdsIter<'a> {
    inner: DocumentsKeysIter<'a>,
    last: Option<DocumentId>,
}

impl Iterator for DocumentsIdsIter<'_> {
    type Item = DocumentId;

    fn next(&mut self) -> Option<Self::Item> {
        for DocumentAttrKey { document_id, .. } in &mut self.inner {
            if self.last != Some(document_id) {
                self.last = Some(document_id);
                return Some(document_id)
            }
        }
        None
    }
}

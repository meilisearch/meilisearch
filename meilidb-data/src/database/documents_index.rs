use std::convert::TryInto;

use meilidb_core::DocumentId;
use rocksdb::DBVector;

use crate::database::raw_index::InnerRawIndex;
use crate::document_attr_key::DocumentAttrKey;
use crate::schema::SchemaAttr;

#[derive(Clone)]
pub struct DocumentsIndex(pub(crate) InnerRawIndex);

impl DocumentsIndex {
    pub fn document_field(&self, id: DocumentId, attr: SchemaAttr) -> Result<Option<DBVector>, rocksdb::Error> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.get(key)
    }

    pub fn set_document_field(&self, id: DocumentId, attr: SchemaAttr, value: Vec<u8>) -> Result<(), rocksdb::Error> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.set(key, value)?;
        Ok(())
    }

    pub fn del_document_field(&self, id: DocumentId, attr: SchemaAttr) -> Result<(), rocksdb::Error> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.delete(key)?;
        Ok(())
    }

    pub fn del_all_document_fields(&self, id: DocumentId) -> Result<(), rocksdb::Error> {
        let start = DocumentAttrKey::new(id, SchemaAttr::min()).to_be_bytes();
        let end = DocumentAttrKey::new(id, SchemaAttr::max()).to_be_bytes();
        self.0.delete_range(start, end)?;
        Ok(())
    }

    pub fn document_fields(&self, id: DocumentId) -> DocumentFieldsIter {
        let start = DocumentAttrKey::new(id, SchemaAttr::min()).to_be_bytes();
        let end = DocumentAttrKey::new(id, SchemaAttr::max()).to_be_bytes();

        let from = rocksdb::IteratorMode::From(&start[..], rocksdb::Direction::Forward);
        let iter = self.0.iterator(from).unwrap();

        DocumentFieldsIter(iter, end.to_vec())
    }
}

pub struct DocumentFieldsIter<'a>(rocksdb::DBIterator<'a>, Vec<u8>);

impl<'a> Iterator for DocumentFieldsIter<'a> {
    type Item = Result<(SchemaAttr, Box<[u8]>), rocksdb::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some((key, value)) => {

                if key.as_ref() > self.1.as_ref() {
                    return None;
                }

                let slice: &[u8] = key.as_ref();
                let array = slice.try_into().unwrap();
                let key = DocumentAttrKey::from_be_bytes(array);
                Some(Ok((key.attribute, value)))
            },
            None => None,
        }
    }
}

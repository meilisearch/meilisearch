use std::convert::TryFrom;
use meilidb_schema::SchemaAttr;
use crate::DocumentId;
use super::{document_attribute_into_key, document_attribute_from_key};

#[derive(Copy, Clone)]
pub struct DocumentsFieldsCounts {
    pub(crate) documents_fields_counts: rkv::SingleStore,
}

impl DocumentsFieldsCounts {
    pub fn put_document_field_count(
        &self,
        writer: &mut rkv::Writer,
        document_id: DocumentId,
        attribute: SchemaAttr,
        value: u64,
    ) -> Result<(), rkv::StoreError>
    {
        let key = document_attribute_into_key(document_id, attribute);
        self.documents_fields_counts.put(writer, key, &rkv::Value::U64(value))
    }

    pub fn del_all_document_fields_counts(
        &self,
        writer: &mut rkv::Writer,
        document_id: DocumentId,
    ) -> Result<usize, rkv::StoreError>
    {
        let mut keys_to_delete = Vec::new();

        // WARN we can not delete the keys using the iterator
        //      so we store them and delete them just after
        for result in self.document_fields_counts(writer, document_id)? {
            let (attribute, _) = result?;
            let key = document_attribute_into_key(document_id, attribute);
            keys_to_delete.push(key);
        }

        let count = keys_to_delete.len();
        for key in keys_to_delete {
            self.documents_fields_counts.delete(writer, key)?;
        }

        Ok(count)
    }

    pub fn document_field_count(
        &self,
        reader: &impl rkv::Readable,
        document_id: DocumentId,
        attribute: SchemaAttr,
    ) -> Result<Option<u64>, rkv::StoreError>
    {
        let key = document_attribute_into_key(document_id, attribute);

        match self.documents_fields_counts.get(reader, key)? {
            Some(rkv::Value::U64(count)) => Ok(Some(count)),
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(None),
        }
    }

    pub fn document_fields_counts<'r, T: rkv::Readable>(
        &self,
        reader: &'r T,
        document_id: DocumentId,
    ) -> Result<DocumentFieldsCountsIter<'r>, rkv::StoreError>
    {
        let document_id_bytes = document_id.0.to_be_bytes();
        let iter = self.documents_fields_counts.iter_from(reader, document_id_bytes)?;
        Ok(DocumentFieldsCountsIter { document_id, iter })
    }

    pub fn documents_ids<'r, T: rkv::Readable>(
        &self,
        reader: &'r T,
    ) -> Result<DocumentsIdsIter<'r>, rkv::StoreError>
    {
        let iter = self.documents_fields_counts.iter_start(reader)?;
        Ok(DocumentsIdsIter { last_seen_id: None, iter })
    }

    pub fn all_documents_fields_counts<'r, T: rkv::Readable>(
        &self,
        reader: &'r T,
    ) -> Result<AllDocumentsFieldsCountsIter<'r>, rkv::StoreError>
    {
        let iter = self.documents_fields_counts.iter_start(reader)?;
        Ok(AllDocumentsFieldsCountsIter { iter })
    }
}

pub struct DocumentFieldsCountsIter<'r> {
    document_id: DocumentId,
    iter: rkv::store::single::Iter<'r>,
}

impl Iterator for DocumentFieldsCountsIter<'_> {
    type Item = Result<(SchemaAttr, u64), rkv::StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, Some(rkv::Value::U64(count))))) => {
                let array = TryFrom::try_from(key).unwrap();
                let (current_document_id, attr) = document_attribute_from_key(array);
                if current_document_id != self.document_id { return None; }

                Some(Ok((attr, count)))
            },
            Some(Ok((key, data))) => panic!("{:?}, {:?}", key, data),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

pub struct DocumentsIdsIter<'r> {
    last_seen_id: Option<DocumentId>,
    iter: rkv::store::single::Iter<'r>,
}

impl Iterator for DocumentsIdsIter<'_> {
    type Item = Result<DocumentId, rkv::StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        for result in &mut self.iter {
            match result {
                Ok((key, _)) => {
                    let array = TryFrom::try_from(key).unwrap();
                    let (document_id, _) = document_attribute_from_key(array);
                    if Some(document_id) != self.last_seen_id {
                        self.last_seen_id = Some(document_id);
                        return Some(Ok(document_id))
                    }
                },
                Err(e) => return Some(Err(e)),
            }
        }

        None
    }
}

pub struct AllDocumentsFieldsCountsIter<'r> {
    iter: rkv::store::single::Iter<'r>,
}

impl<'r> Iterator for AllDocumentsFieldsCountsIter<'r> {
    type Item = Result<(DocumentId, SchemaAttr, u64), rkv::StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, Some(rkv::Value::U64(count))))) => {
                let array = TryFrom::try_from(key).unwrap();
                let (document_id, attr) = document_attribute_from_key(array);
                Some(Ok((document_id, attr, count)))
            },
            Some(Ok((key, data))) => panic!("{:?}, {:?}", key, data),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

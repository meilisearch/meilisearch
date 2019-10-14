use std::convert::TryFrom;
use meilidb_schema::SchemaAttr;
use crate::DocumentId;
use super::{document_attribute_into_key, document_attribute_from_key};

#[derive(Copy, Clone)]
pub struct DocumentsFields {
    pub(crate) documents_fields: rkv::SingleStore,
}

impl DocumentsFields {
    pub fn put_document_field(
        &self,
        writer: &mut rkv::Writer,
        document_id: DocumentId,
        attribute: SchemaAttr,
        value: &[u8],
    ) -> Result<(), rkv::StoreError>
    {
        let key = document_attribute_into_key(document_id, attribute);
        self.documents_fields.put(writer, key, &rkv::Value::Blob(value))
    }

    pub fn del_all_document_fields(
        &self,
        writer: &mut rkv::Writer,
        document_id: DocumentId,
    ) -> Result<usize, rkv::StoreError>
    {
        let document_id_bytes = document_id.0.to_be_bytes();
        let mut keys_to_delete = Vec::new();

        // WARN we can not delete the keys using the iterator
        //      so we store them and delete them just after
        let iter = self.documents_fields.iter_from(writer, document_id_bytes)?;
        for result in iter {
            let (key, _) = result?;
            let array = TryFrom::try_from(key).unwrap();
            let (current_document_id, _) = document_attribute_from_key(array);
            if current_document_id != document_id { break }

            keys_to_delete.push(key.to_owned());
        }

        let count = keys_to_delete.len();
        for key in keys_to_delete {
            self.documents_fields.delete(writer, key)?;
        }

        Ok(count)
    }

    pub fn document_attribute<'a>(
        &self,
        reader: &'a impl rkv::Readable,
        document_id: DocumentId,
        attribute: SchemaAttr,
    ) -> Result<Option<&'a [u8]>, rkv::StoreError>
    {
        let key = document_attribute_into_key(document_id, attribute);

        match self.documents_fields.get(reader, key)? {
            Some(rkv::Value::Blob(bytes)) => Ok(Some(bytes)),
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(None),
        }
    }

    pub fn document_fields<'r, T: rkv::Readable>(
        &self,
        reader: &'r T,
        document_id: DocumentId,
    ) -> Result<DocumentFieldsIter<'r>, rkv::StoreError>
    {
        let document_id_bytes = document_id.0.to_be_bytes();
        let iter = self.documents_fields.iter_from(reader, document_id_bytes)?;
        Ok(DocumentFieldsIter { document_id, iter })
    }
}

pub struct DocumentFieldsIter<'r> {
    document_id: DocumentId,
    iter: rkv::store::single::Iter<'r>,
}

impl<'r> Iterator for DocumentFieldsIter<'r> {
    type Item = Result<(SchemaAttr, &'r [u8]), rkv::StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, Some(rkv::Value::Blob(bytes))))) => {
                let array = TryFrom::try_from(key).unwrap();
                let (current_document_id, attr) = document_attribute_from_key(array);
                if current_document_id != self.document_id { return None; }

                Some(Ok((attr, bytes)))
            },
            Some(Ok((key, data))) => panic!("{:?}, {:?}", key, data),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

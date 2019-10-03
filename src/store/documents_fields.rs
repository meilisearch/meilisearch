use std::convert::TryFrom;
use meilidb_schema::SchemaAttr;
use crate::DocumentId;

#[derive(Copy, Clone)]
pub struct DocumentsFields {
    pub(crate) documents_fields: rkv::SingleStore,
}

fn document_attribute_into_key(document_id: DocumentId, attribute: SchemaAttr) -> [u8; 10] {
    let document_id_bytes = document_id.0.to_be_bytes();
    let attr_bytes = attribute.0.to_be_bytes();

    let mut key = [0u8; 10];
    key[0..8].copy_from_slice(&document_id_bytes);
    key[8..10].copy_from_slice(&attr_bytes);

    key
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
            let current_document_id = {
                let bytes = key.get(0..8).unwrap();
                let array = TryFrom::try_from(bytes).unwrap();
                DocumentId(u64::from_be_bytes(array))
            };

            if current_document_id != document_id { break }
            keys_to_delete.push(key.to_owned());
        }

        let count = keys_to_delete.len();
        for key in keys_to_delete {
            self.documents_fields.delete(writer, key)?;
        }

        Ok(count)
    }

    pub fn document_field<'a, T: rkv::Readable>(
        &self,
        reader: &'a T,
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
    ) -> Result<DocumentFieldsIter<'r, T>, rkv::StoreError>
    {
        let document_id_bytes = document_id.0.to_be_bytes();
        let iter = self.documents_fields.iter_from(reader, document_id_bytes)?;
        Ok(DocumentFieldsIter { reader, document_id, iter })
    }
}

pub struct DocumentFieldsIter<'r, T> {
    reader: &'r T,
    document_id: DocumentId,
    iter: rkv::store::single::Iter<'r>,
}

impl<'r, T: rkv::Readable + 'r> Iterator for DocumentFieldsIter<'r, T> {
    type Item = Result<(SchemaAttr, &'r [u8]), rkv::StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, Some(rkv::Value::Blob(bytes))))) => {
                let key_bytes = key.get(8..8+2).unwrap();
                let array = TryFrom::try_from(key_bytes).unwrap();
                let attr = u16::from_be_bytes(array);
                let attr = SchemaAttr::new(attr);
                Some(Ok((attr, bytes)))
            },
            Some(Ok((key, data))) => panic!("{:?}, {:?}", key, data),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

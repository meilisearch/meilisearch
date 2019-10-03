use std::convert::TryFrom;
use meilidb_schema::SchemaAttr;
use crate::DocumentId;

pub struct DocumentsFields {
    pub(crate) documents_fields: rkv::SingleStore,
}

impl DocumentsFields {
    pub fn del_all_document_fields(
        &mut self,
        writer: &mut rkv::Writer,
        document_id: DocumentId,
    ) -> Result<(), rkv::StoreError>
    {
        unimplemented!()
    }

    pub fn document_field<T: rkv::Readable>(
        &self,
        reader: &T,
        document_id: DocumentId,
        attribute: SchemaAttr,
    ) -> Result<Option<&[u8]>, rkv::StoreError>
    {
        unimplemented!()
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
                let bytes = key.get(8..8+2).unwrap();
                let array = <[u8; 2]>::try_from(bytes).unwrap();
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

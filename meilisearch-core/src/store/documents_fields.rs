use heed::types::{ByteSlice, OwnedType};
use crate::database::MainT;
use heed::Result as ZResult;
use meilisearch_schema::FieldId;

use super::DocumentFieldStoredKey;
use crate::DocumentId;

#[derive(Copy, Clone)]
pub struct DocumentsFields {
    pub(crate) documents_fields: heed::Database<OwnedType<DocumentFieldStoredKey>, ByteSlice>,
}

impl DocumentsFields {
    pub fn put_document_field(
        self,
        writer: &mut heed::RwTxn<MainT>,
        document_id: DocumentId,
        field: FieldId,
        value: &[u8],
    ) -> ZResult<()> {
        let key = DocumentFieldStoredKey::new(document_id, field);
        self.documents_fields.put(writer, &key, value)
    }

    pub fn del_all_document_fields(
        self,
        writer: &mut heed::RwTxn<MainT>,
        document_id: DocumentId,
    ) -> ZResult<usize> {
        let start = DocumentFieldStoredKey::new(document_id, FieldId::min());
        let end = DocumentFieldStoredKey::new(document_id, FieldId::max());
        self.documents_fields.delete_range(writer, &(start..=end))
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.documents_fields.clear(writer)
    }

    pub fn document_attribute<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        document_id: DocumentId,
        field: FieldId,
    ) -> ZResult<Option<&'txn [u8]>> {
        let key = DocumentFieldStoredKey::new(document_id, field);
        self.documents_fields.get(reader, &key)
    }

    pub fn document_fields<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        document_id: DocumentId,
    ) -> ZResult<DocumentFieldsIter<'txn>> {
        let start = DocumentFieldStoredKey::new(document_id, FieldId::min());
        let end = DocumentFieldStoredKey::new(document_id, FieldId::max());
        let iter = self.documents_fields.range(reader, &(start..=end))?;
        Ok(DocumentFieldsIter { iter })
    }
}

pub struct DocumentFieldsIter<'txn> {
    iter: heed::RoRange<'txn, OwnedType<DocumentFieldStoredKey>, ByteSlice>,
}

impl<'txn> Iterator for DocumentFieldsIter<'txn> {
    type Item = ZResult<(FieldId, &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, bytes))) => {
                let field_id = FieldId(key.field_id.get());
                Some(Ok((field_id, bytes)))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

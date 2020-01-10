use super::DocumentAttrKey;
use crate::database::MainT;
use crate::DocumentId;
use heed::types::OwnedType;
use heed::Result as ZResult;
use meilisearch_schema::FieldId;

#[derive(Copy, Clone)]
pub struct DocumentsFieldsCounts {
    pub(crate) documents_fields_counts: heed::Database<OwnedType<DocumentAttrKey>, OwnedType<u16>>,
}

impl DocumentsFieldsCounts {
    pub fn put_document_field_count(
        self,
        writer: &mut heed::RwTxn<MainT>,
        document_id: DocumentId,
        attribute: FieldId,
        value: u16,
    ) -> ZResult<()> {
        let key = DocumentAttrKey::new(document_id, attribute);
        self.documents_fields_counts.put(writer, &key, &value)
    }

    pub fn del_all_document_fields_counts(
        self,
        writer: &mut heed::RwTxn<MainT>,
        document_id: DocumentId,
    ) -> ZResult<usize> {
        let start = DocumentAttrKey::new(document_id, FieldId::min());
        let end = DocumentAttrKey::new(document_id, FieldId::max());
        self.documents_fields_counts
            .delete_range(writer, &(start..=end))
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.documents_fields_counts.clear(writer)
    }

    pub fn document_field_count(
        self,
        reader: &heed::RoTxn<MainT>,
        document_id: DocumentId,
        attribute: FieldId,
    ) -> ZResult<Option<u16>> {
        let key = DocumentAttrKey::new(document_id, attribute);
        match self.documents_fields_counts.get(reader, &key)? {
            Some(count) => Ok(Some(count)),
            None => Ok(None),
        }
    }

    pub fn document_fields_counts<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        document_id: DocumentId,
    ) -> ZResult<DocumentFieldsCountsIter<'txn>> {
        let start = DocumentAttrKey::new(document_id, FieldId::min());
        let end = DocumentAttrKey::new(document_id, FieldId::max());
        let iter = self.documents_fields_counts.range(reader, &(start..=end))?;
        Ok(DocumentFieldsCountsIter { iter })
    }

    pub fn documents_ids<'txn>(self, reader: &'txn heed::RoTxn<MainT>) -> ZResult<DocumentsIdsIter<'txn>> {
        let iter = self.documents_fields_counts.iter(reader)?;
        Ok(DocumentsIdsIter {
            last_seen_id: None,
            iter,
        })
    }

    pub fn all_documents_fields_counts<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
    ) -> ZResult<AllDocumentsFieldsCountsIter<'txn>> {
        let iter = self.documents_fields_counts.iter(reader)?;
        Ok(AllDocumentsFieldsCountsIter { iter })
    }
}

pub struct DocumentFieldsCountsIter<'txn> {
    iter: heed::RoRange<'txn, OwnedType<DocumentAttrKey>, OwnedType<u16>>,
}

impl Iterator for DocumentFieldsCountsIter<'_> {
    type Item = ZResult<(FieldId, u16)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, count))) => {
                let attr = FieldId(key.attr.get());
                Some(Ok((attr, count)))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

pub struct DocumentsIdsIter<'txn> {
    last_seen_id: Option<DocumentId>,
    iter: heed::RoIter<'txn, OwnedType<DocumentAttrKey>, OwnedType<u16>>,
}

impl Iterator for DocumentsIdsIter<'_> {
    type Item = ZResult<DocumentId>;

    fn next(&mut self) -> Option<Self::Item> {
        for result in &mut self.iter {
            match result {
                Ok((key, _)) => {
                    let document_id = DocumentId(key.docid.get());
                    if Some(document_id) != self.last_seen_id {
                        self.last_seen_id = Some(document_id);
                        return Some(Ok(document_id));
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

pub struct AllDocumentsFieldsCountsIter<'txn> {
    iter: heed::RoIter<'txn, OwnedType<DocumentAttrKey>, OwnedType<u16>>,
}

impl Iterator for AllDocumentsFieldsCountsIter<'_> {
    type Item = ZResult<(DocumentId, FieldId, u16)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, count))) => {
                let docid = DocumentId(key.docid.get());
                let attr = FieldId(key.attr.get());
                Some(Ok((docid, attr, count)))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

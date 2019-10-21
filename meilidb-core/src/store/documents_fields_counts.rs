use super::DocumentAttrKey;
use crate::DocumentId;
use heed::types::OwnedType;
use heed::Result as ZResult;
use meilidb_schema::SchemaAttr;

#[derive(Copy, Clone)]
pub struct DocumentsFieldsCounts {
    pub(crate) documents_fields_counts: heed::Database<OwnedType<DocumentAttrKey>, OwnedType<u64>>,
}

impl DocumentsFieldsCounts {
    pub fn put_document_field_count(
        self,
        writer: &mut heed::RwTxn,
        document_id: DocumentId,
        attribute: SchemaAttr,
        value: u64,
    ) -> ZResult<()> {
        let key = DocumentAttrKey::new(document_id, attribute);
        self.documents_fields_counts.put(writer, &key, &value)
    }

    pub fn del_all_document_fields_counts(
        self,
        writer: &mut heed::RwTxn,
        document_id: DocumentId,
    ) -> ZResult<usize> {
        let start = DocumentAttrKey::new(document_id, SchemaAttr::min());
        let end = DocumentAttrKey::new(document_id, SchemaAttr::max());
        self.documents_fields_counts
            .delete_range(writer, start..=end)
    }

    pub fn document_field_count(
        self,
        reader: &heed::RoTxn,
        document_id: DocumentId,
        attribute: SchemaAttr,
    ) -> ZResult<Option<u64>> {
        let key = DocumentAttrKey::new(document_id, attribute);
        match self.documents_fields_counts.get(reader, &key)? {
            Some(count) => Ok(Some(count)),
            None => Ok(None),
        }
    }

    pub fn document_fields_counts<'txn>(
        self,
        reader: &'txn heed::RoTxn,
        document_id: DocumentId,
    ) -> ZResult<DocumentFieldsCountsIter<'txn>> {
        let start = DocumentAttrKey::new(document_id, SchemaAttr::min());
        let end = DocumentAttrKey::new(document_id, SchemaAttr::max());
        let iter = self.documents_fields_counts.range(reader, start..=end)?;
        Ok(DocumentFieldsCountsIter { iter })
    }

    pub fn documents_ids<'txn>(self, reader: &'txn heed::RoTxn) -> ZResult<DocumentsIdsIter<'txn>> {
        let iter = self.documents_fields_counts.iter(reader)?;
        Ok(DocumentsIdsIter {
            last_seen_id: None,
            iter,
        })
    }

    pub fn all_documents_fields_counts<'txn>(
        self,
        reader: &'txn heed::RoTxn,
    ) -> ZResult<AllDocumentsFieldsCountsIter<'txn>> {
        let iter = self.documents_fields_counts.iter(reader)?;
        Ok(AllDocumentsFieldsCountsIter { iter })
    }
}

pub struct DocumentFieldsCountsIter<'txn> {
    iter: heed::RoRange<'txn, OwnedType<DocumentAttrKey>, OwnedType<u64>>,
}

impl Iterator for DocumentFieldsCountsIter<'_> {
    type Item = ZResult<(SchemaAttr, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, count))) => {
                let attr = SchemaAttr(key.attr.get());
                Some(Ok((attr, count)))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

pub struct DocumentsIdsIter<'txn> {
    last_seen_id: Option<DocumentId>,
    iter: heed::RoIter<'txn, OwnedType<DocumentAttrKey>, OwnedType<u64>>,
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
    iter: heed::RoIter<'txn, OwnedType<DocumentAttrKey>, OwnedType<u64>>,
}

impl<'r> Iterator for AllDocumentsFieldsCountsIter<'r> {
    type Item = ZResult<(DocumentId, SchemaAttr, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, count))) => {
                let docid = DocumentId(key.docid.get());
                let attr = SchemaAttr(key.attr.get());
                Some(Ok((docid, attr, count)))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

use super::BEU64;
use crate::DocumentId;
use heed::types::{ByteSlice, OwnedType};
use heed::Result as ZResult;
use std::sync::Arc;

#[derive(Copy, Clone)]
pub struct DocsWords {
    pub(crate) docs_words: heed::Database<OwnedType<BEU64>, ByteSlice>,
}

impl DocsWords {
    pub fn put_doc_words(
        self,
        writer: &mut heed::RwTxn,
        document_id: DocumentId,
        words: &fst::Set,
    ) -> ZResult<()> {
        let document_id = BEU64::new(document_id.0);
        let bytes = words.as_fst().as_bytes();
        self.docs_words.put(writer, &document_id, bytes)
    }

    pub fn del_doc_words(self, writer: &mut heed::RwTxn, document_id: DocumentId) -> ZResult<bool> {
        let document_id = BEU64::new(document_id.0);
        self.docs_words.delete(writer, &document_id)
    }

    pub fn clear(self, writer: &mut heed::RwTxn) -> ZResult<()> {
        self.docs_words.clear(writer)
    }

    pub fn doc_words(
        self,
        reader: &heed::RoTxn,
        document_id: DocumentId,
    ) -> ZResult<Option<fst::Set>> {
        let document_id = BEU64::new(document_id.0);
        match self.docs_words.get(reader, &document_id)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::new(bytes.to_owned());
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(Some(fst::Set::from(fst)))
            }
            None => Ok(None),
        }
    }
}

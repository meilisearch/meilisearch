use std::borrow::Cow;

use heed::Result as ZResult;
use heed::types::{ByteSlice, OwnedType};

use crate::database::MainT;
use crate::{DocumentId, FstSetCow};
use super::BEU32;

#[derive(Copy, Clone)]
pub struct DocsWords {
    pub(crate) docs_words: heed::Database<OwnedType<BEU32>, ByteSlice>,
}

impl DocsWords {
    pub fn put_doc_words(
        self,
        writer: &mut heed::RwTxn<MainT>,
        document_id: DocumentId,
        words: &FstSetCow,
    ) -> ZResult<()> {
        let document_id = BEU32::new(document_id.0);
        let bytes = words.as_fst().as_bytes();
        self.docs_words.put(writer, &document_id, bytes)
    }

    pub fn del_doc_words(self, writer: &mut heed::RwTxn<MainT>, document_id: DocumentId) -> ZResult<bool> {
        let document_id = BEU32::new(document_id.0);
        self.docs_words.delete(writer, &document_id)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.docs_words.clear(writer)
    }

    pub fn doc_words<'a>(self, reader: &'a heed::RoTxn<'a, MainT>, document_id: DocumentId) -> ZResult<FstSetCow> {
        let document_id = BEU32::new(document_id.0);
        match self.docs_words.get(reader, &document_id)? {
            Some(bytes) => Ok(fst::Set::new(bytes).unwrap().map_data(Cow::Borrowed).unwrap()),
            None => Ok(fst::Set::default().map_data(Cow::Owned).unwrap()),
        }
    }
}

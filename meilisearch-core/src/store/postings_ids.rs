use std::borrow::Cow;

use heed::Result as ZResult;
use heed::types::ByteSlice;
use sdset::Set;

use crate::database::MainT;
use crate::DocumentId;
use crate::heed_types::CowSet;

#[derive(Copy, Clone)]
pub struct PostingsIds {
    pub(crate) postings_ids: heed::Database<ByteSlice, CowSet<DocumentId>>,
}

impl PostingsIds {
    pub fn put_postings_ids(
        self,
        writer: &mut heed::RwTxn<MainT>,
        word: &[u8],
        postings: &Set<DocumentId>,
    ) -> ZResult<()>
    {
        self.postings_ids.put(writer, word, &postings)
    }

    pub fn del_postings_ids(self, writer: &mut heed::RwTxn<MainT>, word: &[u8]) -> ZResult<bool> {
        self.postings_ids.delete(writer, word)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.postings_ids.clear(writer)
    }

    pub fn postings_ids<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        word: &[u8],
    ) -> ZResult<Option<Cow<'txn, Set<DocumentId>>>>
    {
        self.postings_ids.get(reader, word)
    }
}

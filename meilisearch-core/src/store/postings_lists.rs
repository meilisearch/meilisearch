use crate::DocIndex;
use heed::types::{ByteSlice, CowSlice};
use heed::Result as ZResult;
use sdset::{Set, SetBuf};
use std::borrow::Cow;

#[derive(Copy, Clone)]
pub struct PostingsLists {
    pub(crate) postings_lists: heed::Database<ByteSlice, CowSlice<DocIndex>>,
}

impl PostingsLists {
    pub fn put_postings_list(
        self,
        writer: &mut heed::RwTxn,
        word: &[u8],
        words_indexes: &Set<DocIndex>,
    ) -> ZResult<()> {
        self.postings_lists.put(writer, word, words_indexes)
    }

    pub fn del_postings_list(self, writer: &mut heed::RwTxn, word: &[u8]) -> ZResult<bool> {
        self.postings_lists.delete(writer, word)
    }

    pub fn clear(self, writer: &mut heed::RwTxn) -> ZResult<()> {
        self.postings_lists.clear(writer)
    }

    pub fn postings_list<'txn>(
        self,
        reader: &'txn heed::RoTxn,
        word: &[u8],
    ) -> ZResult<Option<Cow<'txn, Set<DocIndex>>>> {
        match self.postings_lists.get(reader, word)? {
            Some(Cow::Borrowed(slice)) => Ok(Some(Cow::Borrowed(Set::new_unchecked(slice)))),
            Some(Cow::Owned(vec)) => Ok(Some(Cow::Owned(SetBuf::new_unchecked(vec)))),
            None => Ok(None),
        }
    }
}

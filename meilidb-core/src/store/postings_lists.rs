use std::borrow::Cow;
use sdset::{Set, SetBuf};
use zlmdb::types::{ByteSlice, CowSlice};
use zlmdb::Result as ZResult;
use crate::DocIndex;

#[derive(Copy, Clone)]
pub struct PostingsLists {
    pub(crate) postings_lists: zlmdb::Database<ByteSlice, CowSlice<DocIndex>>,
}

impl PostingsLists {
    pub fn put_postings_list(
        &self,
        writer: &mut zlmdb::RwTxn,
        word: &[u8],
        words_indexes: &Set<DocIndex>,
    ) -> ZResult<()>
    {
        self.postings_lists.put(writer, word, words_indexes)
    }

    pub fn del_postings_list(&self, writer: &mut zlmdb::RwTxn, word: &[u8]) -> ZResult<bool> {
        self.postings_lists.delete(writer, word)
    }

    pub fn postings_list<'txn>(
        &self,
        reader: &'txn zlmdb::RoTxn,
        word: &[u8],
    ) -> ZResult<Option<Cow<'txn, Set<DocIndex>>>>
    {
        match self.postings_lists.get(reader, word)? {
            Some(Cow::Borrowed(slice)) => Ok(Some(Cow::Borrowed(Set::new_unchecked(slice)))),
            Some(Cow::Owned(vec)) => Ok(Some(Cow::Owned(SetBuf::new_unchecked(vec)))),
            None => Ok(None),
        }
    }
}

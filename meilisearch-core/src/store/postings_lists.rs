use std::borrow::Cow;

use heed::Result as ZResult;
use heed::types::ByteSlice;
use sdset::{Set, SetBuf};
use slice_group_by::GroupBy;

use crate::database::MainT;
use crate::DocIndex;
use crate::store::{Postings, PostingsCodec};

#[derive(Copy, Clone)]
pub struct PostingsLists {
    pub(crate) postings_lists: heed::Database<ByteSlice, PostingsCodec>,
}

impl PostingsLists {
    pub fn put_postings_list(
        self,
        writer: &mut heed::RwTxn<MainT>,
        word: &[u8],
        matches: &Set<DocIndex>,
    ) -> ZResult<()> {
        let docids = matches.linear_group_by_key(|m| m.document_id).map(|g| g[0].document_id).collect();
        let docids = Cow::Owned(SetBuf::new_unchecked(docids));
        let matches = Cow::Borrowed(matches);
        let postings = Postings { docids, matches };

        self.postings_lists.put(writer, word, &postings)
    }

    pub fn del_postings_list(self, writer: &mut heed::RwTxn<MainT>, word: &[u8]) -> ZResult<bool> {
        self.postings_lists.delete(writer, word)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.postings_lists.clear(writer)
    }

    pub fn postings_list<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        word: &[u8],
    ) -> ZResult<Option<Postings<'txn>>> {
        self.postings_lists.get(reader, word)
    }
}

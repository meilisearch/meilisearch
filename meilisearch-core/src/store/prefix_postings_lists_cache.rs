use std::borrow::Cow;

use heed::Result as ZResult;
use heed::types::{OwnedType, CowSlice};
use sdset::{Set, SetBuf};

use crate::DocIndex;
use crate::database::MainT;

#[derive(Copy, Clone)]
pub struct PrefixPostingsListsCache {
    pub(crate) prefix_postings_lists_cache: heed::Database<OwnedType<[u8; 4]>, CowSlice<DocIndex>>,
}

impl PrefixPostingsListsCache {
    pub fn put_prefix_postings_list(
        self,
        writer: &mut heed::RwTxn<MainT>,
        prefix: [u8; 4],
        postings_list: &Set<DocIndex>,
    ) -> ZResult<()>
    {
        self.prefix_postings_lists_cache.put(writer, &prefix, postings_list)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.prefix_postings_lists_cache.clear(writer)
    }

    pub fn prefix_postings_list<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        prefix: [u8; 4],
    ) -> ZResult<Option<Cow<'txn, Set<DocIndex>>>>
    {
        match self.prefix_postings_lists_cache.get(reader, &prefix)? {
            Some(Cow::Owned(vec)) => Ok(Some(Cow::Owned(SetBuf::new_unchecked(vec)))),
            Some(Cow::Borrowed(slice)) => Ok(Some(Cow::Borrowed(Set::new_unchecked(slice)))),
            None => Ok(None),
        }
    }
}

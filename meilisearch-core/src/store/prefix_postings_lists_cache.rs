use std::borrow::Cow;

use heed::Result as ZResult;
use heed::types::OwnedType;
use sdset::{Set, SetBuf};
use slice_group_by::GroupBy;

use crate::database::MainT;
use crate::DocIndex;
use crate::store::{PostingsCodec, Postings};

#[derive(Copy, Clone)]
pub struct PrefixPostingsListsCache {
    pub(crate) prefix_postings_lists_cache: heed::Database<OwnedType<[u8; 4]>, PostingsCodec>,
}

impl PrefixPostingsListsCache {
    pub fn put_prefix_postings_list(
        self,
        writer: &mut heed::RwTxn<MainT>,
        prefix: [u8; 4],
        matches: &Set<DocIndex>,
    ) -> ZResult<()>
    {
        let docids = matches.linear_group_by_key(|m| m.document_id).map(|g| g[0].document_id).collect();
        let docids = Cow::Owned(SetBuf::new_unchecked(docids));
        let matches = Cow::Borrowed(matches);
        let postings = Postings { docids, matches };

        self.prefix_postings_lists_cache.put(writer, &prefix, &postings)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.prefix_postings_lists_cache.clear(writer)
    }

    pub fn prefix_postings_list<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        prefix: [u8; 4],
    ) -> ZResult<Option<Postings<'txn>>>
    {
        self.prefix_postings_lists_cache.get(reader, &prefix)
    }
}

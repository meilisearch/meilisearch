use std::borrow::Cow;

use heed::types::{OwnedType, CowSlice};
use heed::Result as ZResult;
use zerocopy::{AsBytes, FromBytes};

use super::{BEU64, BEU32};
use crate::{DocumentId, Highlight};
use crate::database::MainT;

#[derive(Debug, Copy, Clone, AsBytes, FromBytes)]
#[repr(C)]
pub struct PrefixKey {
    prefix: [u8; 4],
    index: BEU64,
    docid: BEU32,
}

impl PrefixKey {
    pub fn new(prefix: [u8; 4], index: u64, docid: u32) -> PrefixKey {
        PrefixKey {
            prefix,
            index: BEU64::new(index),
            docid: BEU32::new(docid),
        }
    }
}

#[derive(Copy, Clone)]
pub struct PrefixDocumentsCache {
    pub(crate) prefix_documents_cache: heed::Database<OwnedType<PrefixKey>, CowSlice<Highlight>>,
}

impl PrefixDocumentsCache {
    pub fn put_prefix_document(
        self,
        writer: &mut heed::RwTxn<MainT>,
        prefix: [u8; 4],
        index: usize,
        docid: DocumentId,
        highlights: &[Highlight],
    ) -> ZResult<()> {
        let key = PrefixKey::new(prefix, index as u64, docid.0);
        self.prefix_documents_cache.put(writer, &key, highlights)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.prefix_documents_cache.clear(writer)
    }

    pub fn prefix_documents<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        prefix: [u8; 4],
    ) -> ZResult<PrefixDocumentsIter<'txn>> {
        let start = PrefixKey::new(prefix, 0, 0);
        let end = PrefixKey::new(prefix, u64::max_value(), u32::max_value());
        let iter = self.prefix_documents_cache.range(reader, &(start..=end))?;
        Ok(PrefixDocumentsIter { iter })
    }
}

pub struct PrefixDocumentsIter<'txn> {
    iter: heed::RoRange<'txn, OwnedType<PrefixKey>, CowSlice<Highlight>>,
}

impl<'txn> Iterator for PrefixDocumentsIter<'txn> {
    type Item = ZResult<(DocumentId, Cow<'txn, [Highlight]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((key, highlights))) => {
                let docid = DocumentId(key.docid.get());
                Some(Ok((docid, highlights)))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

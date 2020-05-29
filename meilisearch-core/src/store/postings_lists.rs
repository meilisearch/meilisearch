use std::borrow::Cow;

use heed::Result as ZResult;
use heed::types::ByteSlice;
use sdset::{Set, SetBuf};

use crate::database::MainT;
use crate::{DocIndex, DocumentId};
use crate::heed_types::{CowSet, Postings, BitPackerSorted, BitPackerUnsorted};

/// The postings lists are composed of 1 + 3 lists:
///   - The postings documents ids which are used to do fast lookups
///     and used to do set operations as fast as possible.
///
///   - The postings attributes which are composed of 3 lists:
///      - The documents ids which are used as indexes for the 2 other lists.
///      - The postings positions which correspond to the position where the word matched.
///        (`n / 1000` is the position and `n % 1000` is the attribute).
///      - The postings highlights which correspond to the position in bytes where the word matched.
///        (`n / 1000` is the position and `n % 1000` is the attribute).
///
#[derive(Copy, Clone)]
pub struct PostingsLists {
    /// The sorted, deduplicated and non-encoded documents ids
    pub(crate) postings_ids: heed::Database<ByteSlice, CowSet<DocumentId>>,

    /// The postings ids sorted but non-deduplicated and encoded.
    pub(crate) postings_ids_indexes: heed::Database<ByteSlice, BitPackerSorted>,
    /// The postings positions non-sorted, non-deduplicated and encoded.
    pub(crate) postings_positions: heed::Database<ByteSlice, BitPackerUnsorted>,
    /// The postings highlights non-sorted, non-deduplicated and encoded.
    pub(crate) postings_highlights: heed::Database<ByteSlice, BitPackerUnsorted>,
}

impl PostingsLists {
    pub fn put_postings_list(
        self,
        writer: &mut heed::RwTxn<MainT>,
        word: &[u8],
        matches: &Set<DocIndex>,
    ) -> ZResult<()>
    {
        let mut docids: Vec<_> = matches.iter().map(|m| m.document_id).collect();
        docids.dedup();
        let docids = SetBuf::new_unchecked(docids);

        self.postings_ids.put(writer, word, &docids)?;
        self.postings_positions.put(writer, word, &postings)?;
        self.postings_highlights.put(writer, word, &postings)?;

        Ok(())
    }

    pub fn del_postings_list(self, writer: &mut heed::RwTxn<MainT>, word: &[u8]) -> ZResult<bool> {
        let deleted_ids = self.postings_ids.delete(writer, word)?;
        let deleted_ids_indexes = self.postings_ids_indexes.delete(writer, word)?;
        let deleted_positions = self.postings_positions.delete(writer, word)?;
        let deleted_highlights = self.postings_highlights.delete(writer, word)?;
        Ok(deleted_ids && deleted_ids_indexes && deleted_positions && deleted_highlights)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.postings_ids.clear(writer)?;
        self.postings_ids_indexes.clear(writer)?;
        self.postings_positions.clear(writer)?;
        self.postings_highlights.clear(writer)?;
        Ok(())
    }

    pub fn postings_list<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        word: &[u8],
    ) -> ZResult<Option<Postings<'txn>>>
    {
        unimplemented!();
        self.postings_lists.get(reader, word)
    }
}

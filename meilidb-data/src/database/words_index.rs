use meilidb_core::DocIndex;
use sdset::{Set, SetBuf};
use zerocopy::{LayoutVerified, AsBytes};

use crate::database::raw_index::InnerRawIndex;

#[derive(Clone)]
pub struct WordsIndex(pub(crate) InnerRawIndex);

impl WordsIndex {
    pub fn doc_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, rocksdb::Error> {
        // we must force an allocation to make the memory aligned
        match self.0.get(word)? {
            Some(bytes) => {
                let layout = LayoutVerified::new_slice(bytes.as_ref()).expect("invalid layout");
                let slice = layout.into_slice();
                let setbuf = SetBuf::new_unchecked(slice.to_vec());
                Ok(Some(setbuf))
            },
            None => Ok(None),
        }
    }

    pub fn set_doc_indexes(&self, word: &[u8], set: &Set<DocIndex>) -> Result<(), rocksdb::Error> {
        self.0.set(word, set.as_bytes())?;
        Ok(())
    }

    pub fn del_doc_indexes(&self, word: &[u8]) -> Result<(), rocksdb::Error> {
        self.0.delete(word)?;
        Ok(())
    }
}

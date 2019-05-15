use std::sync::Arc;

use meilidb_core::DocIndex;
use sdset::{Set, SetBuf};
use zerocopy::{LayoutVerified, AsBytes};

#[derive(Clone)]
pub struct WordsIndex(pub(crate) Arc<sled::Tree>);

impl WordsIndex {
    pub fn doc_indexes(&self, word: &[u8]) -> sled::Result<Option<SetBuf<DocIndex>>> {
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

    pub fn set_doc_indexes(&self, word: &[u8], set: &Set<DocIndex>) -> sled::Result<()> {
        self.0.set(word, set.as_bytes())?;
        Ok(())
    }

    pub fn del_doc_indexes(&self, word: &[u8]) -> sled::Result<()> {
        self.0.del(word)?;
        Ok(())
    }
}

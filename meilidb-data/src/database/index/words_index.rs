use std::sync::Arc;
use meilidb_core::DocIndex;
use sdset::{Set, SetBuf};
use zerocopy::{LayoutVerified, AsBytes};

#[derive(Clone)]
pub struct WordsIndex(pub(crate) Arc<sled::Tree>);

impl WordsIndex {
    pub fn doc_indexes(&self, word: &[u8]) -> sled::Result<Option<SetBuf<DocIndex>>> {
        // we must force an allocation to make the memory aligned
        match self.0.get(word)? {
            Some(bytes) => {
                let vec = match LayoutVerified::new_slice(bytes.as_ref()) {
                    Some(layout) => layout.into_slice().to_vec(),
                    None => {
                        let len = bytes.as_ref().len();
                        let count = len / std::mem::size_of::<DocIndex>();
                        let mut buf: Vec<DocIndex> = Vec::with_capacity(count);
                        unsafe {
                            let src = bytes.as_ref().as_ptr();
                            let dst = buf.as_mut_ptr() as *mut u8;
                            std::ptr::copy_nonoverlapping(src, dst, len);
                            buf.set_len(count);
                        }
                        buf
                    }
                };

                let setbuf = SetBuf::new_unchecked(vec);

                Ok(Some(setbuf))
            },
            None => Ok(None),
        }
    }

    pub fn set_doc_indexes(&self, word: &[u8], set: &Set<DocIndex>) -> sled::Result<()> {
        self.0.insert(word, set.as_bytes()).map(drop)
    }

    pub fn del_doc_indexes(&self, word: &[u8]) -> sled::Result<()> {
        self.0.remove(word).map(drop)
    }
}

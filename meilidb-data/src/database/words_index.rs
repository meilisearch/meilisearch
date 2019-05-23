use std::sync::Arc;

use meilidb_core::DocIndex;
use sdset::{Set, SetBuf};
use zerocopy::{LayoutVerified, AsBytes};

#[derive(Clone)]
pub struct WordsIndex(pub Arc<rocksdb::DB>, pub String);

impl WordsIndex {
    pub fn doc_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, rocksdb::Error> {
        let cf = self.0.cf_handle(&self.1).unwrap();
        match self.0.get_cf(cf, word)? {
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
        let cf = self.0.cf_handle(&self.1).unwrap();
        self.0.put_cf(cf, word, set.as_bytes())?;
        Ok(())
    }

    pub fn del_doc_indexes(&self, word: &[u8]) -> Result<(), rocksdb::Error> {
        let cf = self.0.cf_handle(&self.1).unwrap();
        self.0.delete_cf(cf, word)?;
        Ok(())
    }
}

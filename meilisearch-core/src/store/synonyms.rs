use std::borrow::Cow;

use heed::Result as ZResult;
use heed::types::ByteSlice;

use crate::database::MainT;
use crate::{FstSetCow, MResult};

#[derive(Copy, Clone)]
pub struct Synonyms {
    pub(crate) synonyms: heed::Database<ByteSlice, ByteSlice>,
}

impl Synonyms {
    pub fn put_synonyms<A>(self, writer: &mut heed::RwTxn<MainT>, word: &[u8], synonyms: &fst::Set<A>) -> ZResult<()>
    where A: AsRef<[u8]>,
    {
        let bytes = synonyms.as_fst().as_bytes();
        self.synonyms.put(writer, word, bytes)
    }

    pub fn del_synonyms(self, writer: &mut heed::RwTxn<MainT>, word: &[u8]) -> ZResult<bool> {
        self.synonyms.delete(writer, word)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.synonyms.clear(writer)
    }

    pub(crate) fn synonyms_fst<'txn>(self, reader: &'txn heed::RoTxn<MainT>, word: &[u8]) -> ZResult<FstSetCow<'txn>> {
        match self.synonyms.get(reader, word)? {
            Some(bytes) => Ok(fst::Set::new(bytes).unwrap().map_data(Cow::Borrowed).unwrap()),
            None => Ok(fst::Set::default().map_data(Cow::Owned).unwrap()),
        }
    }

    pub fn synonyms(self, reader: &heed::RoTxn<MainT>, word: &[u8]) -> MResult<Vec<String>> {
        let synonyms = self
            .synonyms_fst(&reader, word)?
            .stream()
            .into_strs()?;
        Ok(synonyms)
    }
}

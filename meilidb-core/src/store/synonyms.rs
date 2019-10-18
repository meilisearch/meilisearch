use std::sync::Arc;
use zlmdb::types::ByteSlice;
use zlmdb::Result as ZResult;

#[derive(Copy, Clone)]
pub struct Synonyms {
    pub(crate) synonyms: zlmdb::Database<ByteSlice, ByteSlice>,
}

impl Synonyms {
    pub fn put_synonyms(
        self,
        writer: &mut zlmdb::RwTxn,
        word: &[u8],
        synonyms: &fst::Set,
    ) -> ZResult<()> {
        let bytes = synonyms.as_fst().as_bytes();
        self.synonyms.put(writer, word, bytes)
    }

    pub fn del_synonyms(self, writer: &mut zlmdb::RwTxn, word: &[u8]) -> ZResult<bool> {
        self.synonyms.delete(writer, word)
    }

    pub fn synonyms(self, reader: &zlmdb::RoTxn, word: &[u8]) -> ZResult<Option<fst::Set>> {
        match self.synonyms.get(reader, word)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::from(bytes);
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(Some(fst::Set::from(fst)))
            }
            None => Ok(None),
        }
    }
}

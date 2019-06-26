use crate::database::raw_index::InnerRawIndex;

#[derive(Clone)]
pub struct SynonymsIndex(pub(crate) InnerRawIndex);

impl SynonymsIndex {
    pub fn alternatives_to(&self, word: &[u8]) -> Result<Option<fst::Set>, rocksdb::Error> {
        match self.0.get(word)? {
            Some(vector) => Ok(Some(fst::Set::from_bytes(vector.to_vec()).unwrap())),
            None => Ok(None),
        }
    }

    pub fn set_alternatives_to(&self, word: &[u8], value: Vec<u8>) -> Result<(), rocksdb::Error> {
        self.0.set(word, value)?;
        Ok(())
    }

    pub fn del_alternatives_of(&self, word: &[u8]) -> Result<(), rocksdb::Error> {
        self.0.delete(word)?;
        Ok(())
    }
}

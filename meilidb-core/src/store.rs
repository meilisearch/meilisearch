use std::error::Error;
use fst::Set;
use sdset::SetBuf;
use crate::DocIndex;

pub trait Store {
    type Error: Error;

    fn words(&self) -> Result<&Set, Self::Error>;
    fn word_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error>;
}

impl<T> Store for &'_ T where T: Store {
    type Error = T::Error;

    fn words(&self) -> Result<&Set, Self::Error> {
        (*self).words()
    }

    fn word_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error> {
        (*self).word_indexes(word)
    }
}

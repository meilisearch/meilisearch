use std::error::Error;
use fst::Set;
use sdset::SetBuf;
use crate::DocIndex;

pub trait Store {
    type Error: Error;

    fn words(&self) -> Result<&Set, Self::Error>;
    fn word_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error>;

    fn synonyms(&self) -> Result<&Set, Self::Error>;
    fn alternatives_to(&self, word: &[u8]) -> Result<Option<Set>, Self::Error>;
}

impl<T> Store for &'_ T where T: Store {
    type Error = T::Error;

    fn words(&self) -> Result<&Set, Self::Error> {
        (*self).words()
    }

    fn word_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error> {
        (*self).word_indexes(word)
    }

    fn synonyms(&self) -> Result<&Set, Self::Error> {
        (*self).synonyms()
    }

    fn alternatives_to(&self, word: &[u8]) -> Result<Option<Set>, Self::Error> {
        (*self).alternatives_to(word)
    }
}

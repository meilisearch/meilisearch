use std::collections::BTreeMap;
use fst::{set, IntoStreamer, Streamer};
use sdset::{Set, SetBuf, SetOperation};
use sdset::duo::{Union, DifferenceByKey};
use crate::{DocIndex, DocumentId};

pub type Word = Vec<u8>; // TODO should be a smallvec

pub trait Store: Clone {
    type Error: std::error::Error;

    fn get_fst(&self) -> Result<fst::Set, Self::Error>;
    fn set_fst(&self, set: &fst::Set) -> Result<(), Self::Error>;

    fn get_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error>;
    fn set_indexes(&self, word: &[u8], indexes: &Set<DocIndex>) -> Result<(), Self::Error>;
    fn del_indexes(&self, word: &[u8]) -> Result<(), Self::Error>;
}

pub struct Index<S> {
    pub set: fst::Set,
    pub store: S,
}

impl<S> Index<S>
where S: Store,
{
    pub fn from_store(store: S) -> Result<Index<S>, S::Error> {
        let set = store.get_fst()?;
        Ok(Index { set, store })
    }

    pub fn remove_documents(&self, documents: &Set<DocumentId>) -> Result<Index<S>, S::Error> {
        let mut buffer = Vec::new();
        let mut builder = fst::SetBuilder::memory();
        let mut stream = self.into_stream();

        while let Some((input, result)) = stream.next() {
            let indexes = match result? {
                Some(indexes) => indexes,
                None => continue,
            };

            let op = DifferenceByKey::new(&indexes, documents, |x| x.document_id, |x| *x);
            buffer.clear();
            op.extend_vec(&mut buffer);

            if buffer.is_empty() {
                self.store.del_indexes(input)?;
            } else {
                builder.insert(input).unwrap();
                let indexes = Set::new_unchecked(&buffer);
                self.store.set_indexes(input, indexes)?;
            }
        }

        let set = builder.into_inner().and_then(fst::Set::from_bytes).unwrap();
        self.store.set_fst(&set)?;

        Ok(Index { set, store: self.store.clone() })
    }

    pub fn insert_indexes(&self, map: BTreeMap<Word, SetBuf<DocIndex>>) -> Result<Index<S>, S::Error> {
        let mut buffer = Vec::new();
        let mut builder = fst::SetBuilder::memory();
        let set = fst::Set::from_iter(map.keys()).unwrap();
        let mut union_ = self.set.op().add(&set).r#union();

        while let Some(input) = union_.next() {
            let remote = self.store.get_indexes(input)?;
            let locale = map.get(input);

            match (remote, locale) {
                (Some(remote), Some(locale)) => {
                    buffer.clear();
                    Union::new(&remote, &locale).extend_vec(&mut buffer);
                    let indexes = Set::new_unchecked(&buffer);

                    if !indexes.is_empty() {
                        self.store.set_indexes(input, indexes)?;
                        builder.insert(input).unwrap();
                    } else {
                        self.store.del_indexes(input)?;
                    }
                },
                (None, Some(locale)) => {
                    self.store.set_indexes(input, &locale)?;
                    builder.insert(input).unwrap();
                },
                (Some(_), None) => {
                    builder.insert(input).unwrap();
                },
                (None, None) => unreachable!(),
            }
        }

        let set = builder.into_inner().and_then(fst::Set::from_bytes).unwrap();
        self.store.set_fst(&set)?;

        Ok(Index { set, store: self.store.clone() })
    }
}

pub struct Stream<'m, S> {
    set_stream: set::Stream<'m>,
    store: &'m S,
}

impl<'m, 'a, S> Streamer<'a> for Stream<'m, S>
where S: 'a + Store,
{
    type Item = (&'a [u8], Result<Option<SetBuf<DocIndex>>, S::Error>);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.set_stream.next() {
            Some(input) => Some((input, self.store.get_indexes(input))),
            None => None,
        }
    }
}

impl<'m, 'a, S> IntoStreamer<'a> for &'m Index<S>
where S: 'a + Store,
{
    type Item = (&'a [u8], Result<Option<SetBuf<DocIndex>>, S::Error>);
    type Into = Stream<'m, S>;

    fn into_stream(self) -> Self::Into {
        Stream {
            set_stream: self.set.into_stream(),
            store: &self.store,
        }
    }
}

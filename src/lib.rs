extern crate bincode;
extern crate fst;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate smallvec;

use std::ops::{Deref, DerefMut};
use std::io::Write;
use std::fs::File;
use std::path::Path;
use std::str::from_utf8_unchecked;

pub use fst::MapBuilder;
use smallvec::SmallVec;

type SmallVec32<T> = SmallVec<[T; 16]>;

#[derive(Debug)]
pub struct MultiMap {
    map: fst::Map,
    values: Box<[SmallVec32<u64>]>,
}

impl MultiMap {
    pub unsafe fn from_paths<P, Q>(map: P, values: Q) -> fst::Result<MultiMap>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>
    {
        let map = fst::Map::from_path(map)?;

        // TODO handle error !!!
        let values_file = File::open(values).unwrap();
        let values = bincode::deserialize_from(values_file).unwrap();

        Ok(MultiMap { map, values })
    }

    pub fn stream(&self) -> Stream {
        Stream {
            inner: self.map.stream(),
            values: &self.values,
        }
    }

    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> bool {
        self.map.contains_key(key)
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[u64]> {
        self.map.get(key).map(|i| &*self.values[i as usize])
    }

    pub fn search<A: fst::Automaton>(&self, aut: A) -> StreamBuilder<A> {
        StreamBuilder {
            inner: self.map.search(aut),
            values: &self.values,
        }
    }
}

pub struct StreamBuilder<'a, A: fst::Automaton> {
    inner: fst::map::StreamBuilder<'a, A>,
    values: &'a [SmallVec32<u64>],
}

impl<'a, A: fst::Automaton> Deref for StreamBuilder<'a, A> {
    type Target = fst::map::StreamBuilder<'a, A>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, A: fst::Automaton> DerefMut for StreamBuilder<'a, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'a, A: fst::Automaton> fst::IntoStreamer<'a> for StreamBuilder<'a, A> {
    type Item = (&'a str, &'a [u64]);

    type Into = Stream<'a, A>;

    fn into_stream(self) -> Self::Into {
        Stream {
            inner: self.inner.into_stream(),
            values: self.values,
        }
    }
}

pub struct Stream<'a, A: fst::Automaton = fst::automaton::AlwaysMatch> {
    inner: fst::map::Stream<'a, A>,
    values: &'a [SmallVec32<u64>],
}

impl<'a, 'm, A: fst::Automaton> fst::Streamer<'a> for Stream<'m, A> {
    type Item = (&'a str, &'a [u64]);

    fn next(&'a mut self) -> Option<Self::Item> {
        // Here we can't just `map` because of some borrow rules
        match self.inner.next() {
            Some((key, i)) => {
                let key = unsafe { from_utf8_unchecked(key) };
                Some((key, &*self.values[i as usize]))
            },
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct MultiMapBuilder {
    map: Vec<(String, u64)>,
    values: Vec<SmallVec32<u64>>,
}

impl<'a> MultiMapBuilder {
    pub fn new() -> MultiMapBuilder {
        MultiMapBuilder {
            map: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn insert<S: Into<String>>(&mut self, key: S, value: u64) {
        let key = key.into();
        match self.map.binary_search_by_key(&key.as_str(), |&(ref k, _)| k) {
            Ok(index) => {
                let (_, index) = self.map[index];
                let values = &mut self.values[index as usize];
                if let Err(index) = values.binary_search(&value) {
                    values.insert(index, value)
                }
            },
            Err(index) => {
                let values = {
                    let mut vec = SmallVec32::new();
                    vec.push(value);
                    vec
                };
                self.values.push(values);
                let values_index = (self.values.len() - 1) as u64;

                let value = (key, values_index);
                self.map.insert(index, value);
            },
        }
    }

    pub fn build_memory(self) -> fst::Result<MultiMap> {
        Ok(MultiMap {
            map: fst::Map::from_iter(self.map)?,
            values: self.values.into_boxed_slice(),
        })
    }

    pub fn build<W, X>(self, map_wrt: W, mut values_wrt: X) -> fst::Result<(W, X)>
    where
        W: Write,
        X: Write
    {
        let mut builder = MapBuilder::new(map_wrt)?;
        builder.extend_iter(self.map)?;
        let map = builder.into_inner()?;

        // TODO handle that !!!
        bincode::serialize_into(&mut values_wrt, &self.values).unwrap();

        Ok((map, values_wrt))
    }
}

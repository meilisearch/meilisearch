use bincode;
use fst::{self, Map, MapBuilder, Automaton};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::fs::File;
use std::io::{Write, BufReader};
use std::ops::Range;
use std::path::Path;
use {StreamBuilder, Stream};

#[derive(Debug)]
pub struct FstMap<T> {
    inner: Map,
    values: Values<T>,
}

impl<T> FstMap<T> {
    pub unsafe fn from_paths<P, Q>(map: P, values: Q) -> fst::Result<Self>
    where
        T: DeserializeOwned,
        P: AsRef<Path>,
        Q: AsRef<Path>
    {
        let inner = Map::from_path(map)?;

        // TODO handle errors !!!
        let values = File::open(values).unwrap();
        let values = BufReader::new(values);
        let values = bincode::deserialize_from(values).unwrap();

        Ok(Self { inner, values })
    }

    pub fn from_bytes(map: Vec<u8>, values: &[u8]) -> fst::Result<Self>
    where
        T: DeserializeOwned
    {
        let inner = Map::from_bytes(map)?;
        let values = bincode::deserialize(values).unwrap();

        Ok(Self { inner, values })
    }

    pub fn stream(&self) -> Stream<T> {
        Stream {
            inner: self.inner.stream(),
            values: &self.values,
        }
    }

    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> bool {
        self.inner.contains_key(key)
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[T]> {
        self.inner.get(key).map(|i| unsafe { self.values.get_unchecked(i as usize) })
    }

    pub fn search<A: Automaton>(&self, aut: A) -> StreamBuilder<T, A> {
        StreamBuilder {
            inner: self.inner.search(aut),
            values: &self.values,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Values<T> {
    ranges: Box<[Range<u64>]>,
    values: Box<[T]>,
}

impl<T> Values<T> {
    fn new(raw: Vec<Vec<T>>) -> Self {
        let cap = raw.len();
        let mut ranges = Vec::with_capacity(cap);
        let cap = raw.iter().map(Vec::len).sum();
        let mut values = Vec::with_capacity(cap);

        for v in &raw {
            let len = v.len() as u64;
            let start = ranges.last().map(|&Range { end, .. }| end).unwrap_or(0);

            let range = Range { start, end: start + len };
            ranges.push(range);
        }

        values.extend(raw.into_iter().flat_map(IntoIterator::into_iter));

        let ranges = ranges.into_boxed_slice();
        let values = values.into_boxed_slice();

        Self { ranges, values }
    }

    pub unsafe fn get_unchecked(&self, index: usize) -> &[T] {
        let range = self.ranges.get_unchecked(index);
        let range = Range { start: range.start as usize, end: range.end as usize };
        self.values.get_unchecked(range)
    }
}

#[derive(Debug)]
pub struct FstMapBuilder<T> {
    map: Vec<(String, u64)>,
    // This makes many memory indirections but it is only used
    // at index time, not kept for query time.
    values: Vec<Vec<T>>,
}

impl<T> FstMapBuilder<T> {
    pub fn new() -> Self {
        Self {
            map: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn insert<S: Into<String>>(&mut self, key: S, value: T) {
        let key = key.into();
        match self.map.binary_search_by_key(&key.as_str(), |&(ref k, _)| k) {
            Ok(index) => {
                let (_, index) = self.map[index];
                let values = &mut self.values[index as usize];

                values.push(value);
            },
            Err(index) => {
                self.values.push(vec![value]);
                let values_index = (self.values.len() - 1) as u64;

                let value = (key, values_index);
                self.map.insert(index, value);
            },
        }
    }

    pub fn build_memory(self) -> fst::Result<FstMap<T>> {
        Ok(FstMap {
            inner: Map::from_iter(self.map)?,
            values: Values::new(self.values),
        })
    }

    pub fn build<W, X>(self, map_wrt: W, mut values_wrt: X) -> fst::Result<(W, X)>
    where
        T: Serialize,
        W: Write,
        X: Write
    {
        let mut builder = MapBuilder::new(map_wrt)?;
        builder.extend_iter(self.map)?;
        let map = builder.into_inner()?;
        let values = Values::new(self.values);

        // TODO handle that error !!!
        bincode::serialize_into(&mut values_wrt, &values).unwrap();

        Ok((map, values_wrt))
    }
}

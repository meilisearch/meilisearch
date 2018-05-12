use bincode;
use fst::{self, Automaton};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::fs::File;
use std::io::{Write, BufReader};
use std::ops::Range;
use std::path::Path;

#[derive(Debug)]
pub struct Map<T> {
    inner: fst::Map,
    values: Values<T>,
}

impl<T> Map<T> {
    pub unsafe fn from_paths<P, Q>(map: P, values: Q) -> fst::Result<Self>
    where
        T: DeserializeOwned,
        P: AsRef<Path>,
        Q: AsRef<Path>
    {
        let inner = fst::Map::from_path(map)?;

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
        let inner = fst::Map::from_bytes(map)?;
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

    pub fn as_map(&self) -> &fst::Map {
        &self.inner
    }

    pub fn values(&self) -> &Values<T> {
        &self.values
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
pub struct MapBuilder<T> {
    map: Vec<(String, u64)>,
    // This makes many memory indirections but it is only used
    // at index time, not kept for query time.
    values: Vec<Vec<T>>,
}

impl<T> MapBuilder<T> {
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

    pub fn build_in_memory(self) -> fst::Result<Map<T>> {
        Ok(Map {
            inner: fst::Map::from_iter(self.map)?,
            values: Values::new(self.values),
        })
    }

    pub fn build<W, X>(self, map_wrt: W, mut values_wrt: X) -> fst::Result<(W, X)>
    where
        T: Serialize,
        W: Write,
        X: Write
    {
        let mut builder = fst::MapBuilder::new(map_wrt)?;
        builder.extend_iter(self.map)?;
        let map = builder.into_inner()?;
        let values = Values::new(self.values);

        // TODO handle that error !!!
        bincode::serialize_into(&mut values_wrt, &values).unwrap();

        Ok((map, values_wrt))
    }
}

pub struct OpBuilder<'m, 'v, T: 'v> {
    inner: fst::map::OpBuilder<'m>,
    values: &'v Values<T>,
}

impl<'m, 'v, T: 'v> OpBuilder<'m, 'v, T> {
    pub fn new(values: &'v Values<T>) -> Self {
        OpBuilder {
            inner: fst::map::OpBuilder::new(),
            values: values,
        }
    }

    pub fn add<I, S>(mut self, streamable: I) -> Self
    where
        I: for<'a> fst::IntoStreamer<'a, Into=S, Item=(&'a [u8], u64)>,
        S: 'm + for<'a> fst::Streamer<'a, Item=(&'a [u8], u64)>,
    {
        self.push(streamable);
        self
    }

    pub fn push<I, S>(&mut self, streamable: I)
    where
        I: for<'a> fst::IntoStreamer<'a, Into=S, Item=(&'a [u8], u64)>,
        S: 'm + for<'a> fst::Streamer<'a, Item=(&'a [u8], u64)>,
    {
        self.inner.push(streamable);
    }

    pub fn union(self) -> Union<'m, 'v, T> {
        Union {
            inner: self.inner.union(),
            outs: Vec::new(),
            values: self.values,
        }
    }
}

pub struct Union<'m, 'v, T: 'v> {
    inner: fst::map::Union<'m>,
    outs: Vec<IndexedValues<'v, T>>,
    values: &'v Values<T>,
}

impl<'a, 'm, 'v, T: 'v + 'a> fst::Streamer<'a> for Union<'m, 'v, T> {
    type Item = (&'a [u8], &'a [IndexedValues<'a, T>]);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((s, ivalues)) => {
                self.outs.clear();
                for ivalue in ivalues {
                    let index = ivalue.index;
                    let values = unsafe { self.values.get_unchecked(ivalue.value as usize) };
                    self.outs.push(IndexedValues { index, values })
                }
                Some((s, &self.outs))
            },
            None => None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexedValues<'a, T: 'a> {
    pub index: usize,
    pub values: &'a [T],
}

pub struct OpWithStateBuilder<'m, 'v, T: 'v, U> {
    inner: fst::map::OpWithStateBuilder<'m, U>,
    values: &'v Values<T>,
}

impl<'m, 'v, T: 'v, U: 'static> OpWithStateBuilder<'m, 'v, T, U> {
    pub fn new(values: &'v Values<T>) -> Self {
        Self {
            inner: fst::map::OpWithStateBuilder::new(),
            values: values,
        }
    }

    pub fn add<I, S>(mut self, streamable: I) -> Self
    where
        I: for<'a> fst::IntoStreamer<'a, Into=S, Item=(&'a [u8], u64, U)>,
        S: 'm + for<'a> fst::Streamer<'a, Item=(&'a [u8], u64, U)>,
    {
        self.push(streamable);
        self
    }

    pub fn push<I, S>(&mut self, streamable: I)
    where
        I: for<'a> fst::IntoStreamer<'a, Into=S, Item=(&'a [u8], u64, U)>,
        S: 'm + for<'a> fst::Streamer<'a, Item=(&'a [u8], u64, U)>,
    {
        self.inner.push(streamable);
    }

    pub fn union(self) -> UnionWithState<'m, 'v, T, U> {
        UnionWithState {
            inner: self.inner.union(),
            outs: Vec::new(),
            values: self.values,
        }
    }
}

pub struct UnionWithState<'m, 'v, T: 'v, U> {
    inner: fst::map::UnionWithState<'m, U>,
    outs: Vec<IndexedValuesWithState<'v, T, U>>,
    values: &'v Values<T>,
}

impl<'a, 'm, 'v, T: 'v + 'a, U: 'a> fst::Streamer<'a> for UnionWithState<'m, 'v, T, U>
where
    U: Clone,
{
    type Item = (&'a [u8], &'a [IndexedValuesWithState<'a, T, U>]);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((s, ivalues)) => {
                self.outs.clear();
                for ivalue in ivalues {
                    let index = ivalue.index;
                    let values = unsafe { self.values.get_unchecked(ivalue.value as usize) };
                    let state = ivalue.state.clone();
                    self.outs.push(IndexedValuesWithState { index, values, state })
                }
                Some((s, &self.outs))
            },
            None => None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexedValuesWithState<'a, T: 'a, U> {
    pub index: usize,
    pub values: &'a [T],
    pub state: U,
}

pub struct StreamBuilder<'m, 'v, T: 'v, A> {
    inner: fst::map::StreamBuilder<'m, A>,
    values: &'v Values<T>,
}

impl<'m, 'v, T: 'v, A> StreamBuilder<'m, 'v, T, A> {
    pub fn with_state(self) -> StreamWithStateBuilder<'m, 'v, T, A> {
        StreamWithStateBuilder {
            inner: self.inner.with_state(),
            values: self.values,
        }
    }
}

impl<'m, 'v, 'a, T: 'v + 'a, A: Automaton> fst::IntoStreamer<'a> for StreamBuilder<'m, 'v, T, A> {
    type Item = <Self::Into as fst::Streamer<'a>>::Item;
    type Into = Stream<'m, 'v, T, A>;

    fn into_stream(self) -> Self::Into {
        Stream {
            inner: self.inner.into_stream(),
            values: self.values,
        }
    }
}

pub struct Stream<'m, 'v, T: 'v, A: Automaton = fst::automaton::AlwaysMatch> {
    inner: fst::map::Stream<'m, A>,
    values: &'v Values<T>,
}

impl<'m, 'v, 'a, T: 'v + 'a, A: Automaton> fst::Streamer<'a> for Stream<'m, 'v, T, A> {
    type Item = (&'a [u8], &'a [T]);

    fn next(&'a mut self) -> Option<Self::Item> {
        // Here we can't just `map` because of some borrow rules
        match self.inner.next() {
            Some((key, i)) => {
                let values = unsafe { self.values.get_unchecked(i as usize) };
                Some((key, values))
            },
            None => None,
        }
    }
}

pub struct StreamWithStateBuilder<'m, 'v, T: 'v, A> {
    inner: fst::map::StreamWithStateBuilder<'m, A>,
    values: &'v Values<T>,
}

impl<'m, 'v, 'a, T: 'v + 'a, A: 'a> fst::IntoStreamer<'a> for StreamWithStateBuilder<'m, 'v, T, A>
where
    A: Automaton,
    A::State: Clone,
{
    type Item = <Self::Into as fst::Streamer<'a>>::Item;
    type Into = StreamWithState<'m, 'v, T, A>;

    fn into_stream(self) -> Self::Into {
        StreamWithState {
            inner: self.inner.into_stream(),
            values: self.values,
        }
    }
}

pub struct StreamWithState<'m, 'v, T: 'v, A: Automaton = fst::automaton::AlwaysMatch> {
    inner: fst::map::StreamWithState<'m, A>,
    values: &'v Values<T>,
}

impl<'m, 'v, 'a, T: 'v + 'a, A: 'a> fst::Streamer<'a> for StreamWithState<'m, 'v, T, A>
where
    A: Automaton,
    A::State: Clone,
{
    type Item = (&'a [u8], &'a [T], A::State);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((key, i, state)) => {
                let values = unsafe { self.values.get_unchecked(i as usize) };
                Some((key, values, state))
            },
            None => None,
        }
    }
}

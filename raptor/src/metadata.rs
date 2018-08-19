use std::sync::Arc;
use std::ops::Deref;
use std::error::Error;
use std::path::Path;
use std::collections::btree_map::{Entry, BTreeMap};
use std::slice::from_raw_parts;
use std::io::{self, Write};
use std::mem;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fst::{self, Map, MapBuilder, Automaton};
use fst::raw::MmapReadOnly;
use DocIndex;

#[repr(C)]
struct Range {
    start: u64,
    end: u64,
}

#[derive(Clone)]
enum DocIndexesData {
    Shared {
        vec: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    },
    Mmap(MmapReadOnly),
}

impl Deref for DocIndexesData {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            DocIndexesData::Shared { vec, offset, len } => {
                &vec[*offset..offset + len]
            },
            DocIndexesData::Mmap(m) => m.as_slice(),
        }
    }
}

#[derive(Clone)]
pub struct DocIndexes {
    ranges: DocIndexesData,
    indexes: DocIndexesData,
}

impl DocIndexes {
    pub unsafe fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mmap = MmapReadOnly::open_path(path)?;

        let range_len = mmap.as_slice().read_u64::<LittleEndian>()?;
        let range_len = range_len as usize * mem::size_of::<Range>();

        let offset = mem::size_of::<u64>() as usize;
        let ranges = DocIndexesData::Mmap(mmap.range(offset, range_len));

        let len = mmap.len() - range_len - offset;
        let offset = offset + range_len;
        let indexes = DocIndexesData::Mmap(mmap.range(offset, len));

        Ok(DocIndexes { ranges, indexes })
    }

    pub fn from_bytes(vec: Vec<u8>) -> io::Result<Self> {
        let vec = Arc::new(vec);

        let range_len = vec.as_slice().read_u64::<LittleEndian>()?;
        let range_len = range_len as usize * mem::size_of::<Range>();

        let offset = mem::size_of::<u64>() as usize;
        let ranges = DocIndexesData::Shared {
            vec: vec.clone(),
            offset,
            len: range_len
        };

        let len = vec.len() - range_len - offset;
        let offset = offset + range_len;
        let indexes = DocIndexesData::Shared { vec, offset, len };

        Ok(DocIndexes { ranges, indexes })
    }

    pub fn get(&self, index: u64) -> Option<&[DocIndex]> {
        self.ranges().get(index as usize).map(|Range { start, end }| {
            let start = *start as usize;
            let end = *end as usize;
            &self.indexes()[start..end]
        })
    }

    fn ranges(&self) -> &[Range] {
        let slice = &self.ranges;
        let ptr = slice.as_ptr() as *const Range;
        let len = slice.len() / mem::size_of::<Range>();
        unsafe { from_raw_parts(ptr, len) }
    }

    fn indexes(&self) -> &[DocIndex] {
        let slice = &self.indexes;
        let ptr = slice.as_ptr() as *const DocIndex;
        let len = slice.len() / mem::size_of::<DocIndex>();
        unsafe { from_raw_parts(ptr, len) }
    }
}

pub struct Metadata {
    map: Map,
    indexes: DocIndexes,
}

impl Metadata {
    pub unsafe fn from_paths<P, Q>(map: P, indexes: Q) -> Result<Self, Box<Error>>
    where P: AsRef<Path>,
          Q: AsRef<Path>,
    {
        let map = Map::from_path(map)?;
        let indexes = DocIndexes::from_path(indexes)?;
        Ok(Metadata { map, indexes })
    }

    pub fn from_bytes(map: Vec<u8>, indexes: Vec<u8>) -> Result<Self, Box<Error>> {
        let map = Map::from_bytes(map)?;
        let indexes = DocIndexes::from_bytes(indexes)?;
        Ok(Metadata { map, indexes })
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[DocIndex]> {
        self.map.get(key).and_then(|index| self.indexes.get(index))
    }

    pub fn as_map(&self) -> &Map {
        &self.map
    }

    pub fn as_indexes(&self) -> &DocIndexes {
        &self.indexes
    }

    pub fn explode(self) -> (Map, DocIndexes) {
        (self.map, self.indexes)
    }
}

pub struct Inner {
    keys: BTreeMap<String, u64>,
    indexes: Vec<Vec<DocIndex>>,
    number_docs: usize,
}

impl Inner {
    pub fn new() -> Self {
        Inner {
            keys: BTreeMap::new(),
            indexes: Vec::new(),
            number_docs: 0,
        }
    }

    pub fn number_doc_indexes(&self) -> usize {
        self.number_docs
    }

    pub fn insert(&mut self, key: String, value: DocIndex) {
        match self.keys.entry(key) {
            Entry::Vacant(e) => {
                let index = self.indexes.len() as u64;
                self.indexes.push(vec![value]);
                e.insert(index);
            },
            Entry::Occupied(e) => {
                let index = *e.get();
                let vec = &mut self.indexes[index as usize];
                vec.push(value);
            },
        }
        self.number_docs += 1;
    }
}

pub struct MetadataBuilder<W, X> {
    inner: Inner,
    map: W,
    indexes: X,
}

impl<W: Write, X: Write> MetadataBuilder<W, X>
{
    pub fn new(map: W, indexes: X) -> Self {
        Self { inner: Inner::new(), map, indexes }
    }

    pub fn insert(&mut self, key: String, index: DocIndex) {
        self.inner.insert(key, index)
    }

    pub fn finish(self) -> Result<(), Box<Error>> {
        self.into_inner().map(|_| ())
    }

    pub fn into_inner(mut self) -> Result<(W, X), Box<Error>> {
        let number_docs = self.inner.number_doc_indexes();

        let mut keys_builder = MapBuilder::new(self.map)?;
        keys_builder.extend_iter(self.inner.keys)?;
        let map = keys_builder.into_inner()?;

        // write down doc_indexes into the indexes Writer
        let (ranges, values) = into_sliced_ranges(self.inner.indexes, number_docs);
        let len = ranges.len() as u64;

        // TODO check if this is correct
        self.indexes.write_u64::<LittleEndian>(len)?;
        unsafe {
            // write Ranges first
            let slice = into_u8_slice(ranges.as_slice());
            self.indexes.write_all(slice)?;

            // write Values after
            let slice = into_u8_slice(values.as_slice());
            self.indexes.write_all(slice)?;
        }
        self.indexes.flush()?;

        Ok((map, self.indexes))
    }
}

fn into_sliced_ranges<T>(vecs: Vec<Vec<T>>, number_docs: usize) -> (Vec<Range>, Vec<T>) {
    let cap = vecs.len();
    let mut ranges = Vec::with_capacity(cap);
    let mut values = Vec::with_capacity(number_docs);

    for mut v in &vecs {
        let len = v.len() as u64;
        let start = ranges.last().map(|&Range { end, .. }| end).unwrap_or(0);

        let range = Range { start, end: start + len };
        ranges.push(range);
    }

    values.extend(vecs.into_iter().flatten());

    (ranges, values)
}

unsafe fn into_u8_slice<T>(slice: &[T]) -> &[u8] {
    let ptr = slice.as_ptr() as *const u8;
    let len = slice.len() * mem::size_of::<T>();
    from_raw_parts(ptr, len)
}

pub struct OpWithStateBuilder<'m, 'v, U> {
    inner: fst::map::OpWithStateBuilder<'m, U>,
    indexes: &'v DocIndexes,
}

impl<'m, 'v, U: 'static> OpWithStateBuilder<'m, 'v, U> {
    pub fn new(indexes: &'v DocIndexes) -> Self {
        Self {
            inner: fst::map::OpWithStateBuilder::new(),
            indexes: indexes,
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

    pub fn union(self) -> UnionWithState<'m, 'v, U> {
        UnionWithState {
            inner: self.inner.union(),
            outs: Vec::new(),
            indexes: self.indexes,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexedValuesWithState<'a, U> {
    pub index: usize,
    pub values: &'a [DocIndex],
    pub state: U,
}

pub struct UnionWithState<'m, 'v, U> {
    inner: fst::map::UnionWithState<'m, U>,
    outs: Vec<IndexedValuesWithState<'v, U>>,
    indexes: &'v DocIndexes,
}

impl<'a, 'm, 'v, U: 'a> fst::Streamer<'a> for UnionWithState<'m, 'v, U>
where
    U: Clone,
{
    type Item = (&'a [u8], &'a [IndexedValuesWithState<'a, U>]);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((s, ivalues)) => {
                self.outs.clear();
                self.outs.reserve(ivalues.len());
                for ivalue in ivalues {
                    if let Some(values) = self.indexes.get(ivalue.value) {
                        let index = ivalue.index;
                        let state = ivalue.state.clone();
                        self.outs.push(IndexedValuesWithState { index, values, state })
                    }
                }
                Some((s, &self.outs))
            },
            None => None,
        }
    }
}

pub struct StreamWithStateBuilder<'m, 'v, A> {
    inner: fst::map::StreamWithStateBuilder<'m, A>,
    indexes: &'v DocIndexes,
}

impl<'m, 'v, 'a, A: 'a> fst::IntoStreamer<'a> for StreamWithStateBuilder<'m, 'v, A>
where
    A: Automaton,
    A::State: Clone,
{
    type Item = <Self::Into as fst::Streamer<'a>>::Item;
    type Into = StreamWithState<'m, 'v, A>;

    fn into_stream(self) -> Self::Into {
        StreamWithState {
            inner: self.inner.into_stream(),
            indexes: self.indexes,
        }
    }
}

pub struct StreamWithState<'m, 'v, A: Automaton = fst::automaton::AlwaysMatch> {
    inner: fst::map::StreamWithState<'m, A>,
    indexes: &'v DocIndexes,
}

impl<'m, 'v, 'a, A: 'a> fst::Streamer<'a> for StreamWithState<'m, 'v, A>
where
    A: Automaton,
    A::State: Clone,
{
    type Item = (&'a [u8], &'a [DocIndex], A::State);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((key, i, state)) => {
                match self.indexes.get(i) {
                    Some(values) => Some((key, values, state)),
                    None => None,
                }
            },
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_serialize_deserialize() {
        let mapw = Vec::new();
        let indexesw = Vec::new();

        let builder = MetadataBuilder::new(mapw, indexesw);
        let (map, indexes) = builder.into_inner().unwrap();

        let metas = Metadata::from_bytes(map, indexes).unwrap();
        assert_eq!(metas.get("chameau"), None);
    }

    #[test]
    fn one_doc_serialize_deserialize() {
        let mapw = Vec::new();
        let indexesw = Vec::new();

        let mut builder = MetadataBuilder::new(mapw, indexesw);

        let doc = DocIndex { document: 12, attribute: 1, attribute_index: 22 };
        builder.insert("chameau".into(), doc);

        let (map, indexes) = builder.into_inner().unwrap();

        let metas = Metadata::from_bytes(map, indexes).unwrap();
        assert_eq!(metas.get("chameau"), Some(&[doc][..]));
    }

    #[test]
    fn multiple_docs_serialize_deserialize() {
        let mapw = Vec::new();
        let indexesw = Vec::new();

        let mut builder = MetadataBuilder::new(mapw, indexesw);

        let doc1 = DocIndex { document: 12, attribute: 1, attribute_index: 22 };
        let doc2 = DocIndex { document: 31, attribute: 0, attribute_index: 1 };
        builder.insert("chameau".into(), doc1);
        builder.insert("chameau".into(), doc2);

        let (map, indexes) = builder.into_inner().unwrap();

        let metas = Metadata::from_bytes(map, indexes).unwrap();
        assert_eq!(metas.get("chameau"), Some(&[doc1, doc2][..]));
    }
}

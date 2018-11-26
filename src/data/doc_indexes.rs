use std::collections::btree_map::{BTreeMap, Iter, Entry};
use std::slice::from_raw_parts;
use std::io::{self, Write};
use std::path::Path;
use std::ops::Deref;
use std::sync::Arc;
use std::mem;

use fst::raw::MmapReadOnly;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::ser::{Serialize, Serializer, SerializeTuple};

use crate::DocIndex;
use crate::data::Data;

#[repr(C)]
struct Range {
    start: u64,
    end: u64,
}

#[derive(Clone, Default)]
pub struct DocIndexes {
    ranges: Data,
    indexes: Data,
}

impl DocIndexes {
    pub unsafe fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mmap = MmapReadOnly::open_path(path)?;

        let ranges_len_offset = mmap.as_slice().len() - mem::size_of::<u64>();
        let ranges_len = (&mmap.as_slice()[ranges_len_offset..]).read_u64::<LittleEndian>()?;
        let ranges_len = ranges_len as usize * mem::size_of::<Range>();

        let ranges_offset = ranges_len_offset - ranges_len;
        let ranges = Data::Mmap(mmap.range(ranges_offset, ranges_len));

        let indexes = Data::Mmap(mmap.range(0, ranges_offset));

        Ok(DocIndexes { ranges, indexes })
    }

    pub fn from_bytes(vec: Vec<u8>) -> io::Result<Self> {
        let vec = Arc::new(vec);

        let ranges_len_offset = vec.len() - mem::size_of::<u64>();
        let ranges_len = (&vec[ranges_len_offset..]).read_u64::<LittleEndian>()?;
        let ranges_len = ranges_len as usize * mem::size_of::<Range>();

        let ranges_offset = ranges_len_offset - ranges_len;
        let ranges = Data::Shared {
            vec: vec.clone(),
            offset: ranges_offset,
            len: ranges_len,
        };

        let indexes = Data::Shared {
            vec: vec,
            offset: 0,
            len: ranges_offset,
        };

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

impl Serialize for DocIndexes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(2)?;
        tuple.serialize_element(self.ranges.as_ref())?;
        tuple.serialize_element(self.indexes.as_ref())?;
        tuple.end()
    }
}

pub struct RawDocIndexesBuilder<W> {
    ranges: Vec<Range>,
    wtr: W,
}

impl RawDocIndexesBuilder<Vec<u8>> {
    pub fn memory() -> Self {
        RawDocIndexesBuilder::new(Vec::new())
    }
}

impl<W: Write> RawDocIndexesBuilder<W> {
    pub fn new(wtr: W) -> Self {
        RawDocIndexesBuilder {
            ranges: Vec::new(),
            wtr: wtr,
        }
    }

    pub fn insert(&mut self, indexes: &[DocIndex]) -> io::Result<()> {
        let len = indexes.len() as u64;
        let start = self.ranges.last().map(|r| r.start).unwrap_or(0);
        let range = Range { start, end: start + len };
        self.ranges.push(range);

        // write the values
        let indexes = unsafe { into_u8_slice(indexes) };
        self.wtr.write_all(indexes)
    }

    pub fn finish(self) -> io::Result<()> {
        self.into_inner().map(drop)
    }

    pub fn into_inner(mut self) -> io::Result<W> {
        // write the ranges
        let ranges = unsafe { into_u8_slice(self.ranges.as_slice()) };
        self.wtr.write_all(ranges)?;

        // write the length of the ranges
        let len = ranges.len() as u64;
        self.wtr.write_u64::<LittleEndian>(len)?;

        Ok(self.wtr)
    }
}

pub struct DocIndexesBuilder<W> {
    keys: BTreeMap<String, u64>,
    indexes: Vec<Vec<DocIndex>>,
    number_docs: usize,
    wtr: W,
}

impl<W: Write> DocIndexesBuilder<W> {
    pub fn new(wtr: W) -> Self {
        Self {
            keys: BTreeMap::new(),
            indexes: Vec::new(),
            number_docs: 0,
            wtr: wtr,
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

    pub fn keys(&self) -> Iter<String, u64> {
        self.keys.iter()
    }

    pub fn finish(self) -> io::Result<()> {
        self.into_inner().map(drop)
    }

    pub fn into_inner(mut self) -> io::Result<W> {
        for vec in &mut self.indexes {
            vec.sort_unstable();
        }

        let (ranges, values) = into_sliced_ranges(self.indexes, self.number_docs);

        // write values first
        let slice = unsafe { into_u8_slice(values.as_slice()) };
        self.wtr.write_all(slice)?;

        // write ranges after
        let slice = unsafe { into_u8_slice(ranges.as_slice()) };
        self.wtr.write_all(slice)?;

        // write the length of the ranges
        let len = ranges.len() as u64;
        self.wtr.write_u64::<LittleEndian>(len)?;

        self.wtr.flush()?;
        Ok(self.wtr)
    }
}

fn into_sliced_ranges<T>(vecs: Vec<Vec<T>>, number_docs: usize) -> (Vec<Range>, Vec<T>) {
    let cap = vecs.len();
    let mut ranges = Vec::with_capacity(cap);
    let mut values = Vec::with_capacity(number_docs);

    for v in &vecs {
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

use std::collections::btree_map::{BTreeMap, Iter, Entry};
use std::slice::from_raw_parts;
use std::io::{self, Write};
use std::path::Path;
use std::ops::Deref;
use std::sync::Arc;
use std::mem;

use fst::raw::MmapReadOnly;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::DocIndex;
use crate::data::Data;

#[repr(C)]
struct Range {
    start: u64,
    end: u64,
}

#[derive(Clone)]
pub struct DocIndexes {
    ranges: Data,
    indexes: Data,
}

impl DocIndexes {
    pub unsafe fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mmap = MmapReadOnly::open_path(path)?;

        let range_len = mmap.as_slice().read_u64::<LittleEndian>()?;
        let range_len = range_len as usize * mem::size_of::<Range>();

        let offset = mem::size_of::<u64>() as usize;
        let ranges = Data::Mmap(mmap.range(offset, range_len));

        let len = mmap.len() - range_len - offset;
        let offset = offset + range_len;
        let indexes = Data::Mmap(mmap.range(offset, len));

        Ok(DocIndexes { ranges, indexes })
    }

    pub fn from_bytes(vec: Vec<u8>) -> io::Result<Self> {
        let vec = Arc::new(vec);

        let range_len = vec.as_slice().read_u64::<LittleEndian>()?;
        let range_len = range_len as usize * mem::size_of::<Range>();

        let offset = mem::size_of::<u64>() as usize;
        let ranges = Data::Shared {
            vec: vec.clone(),
            offset,
            len: range_len
        };

        let len = vec.len() - range_len - offset;
        let offset = offset + range_len;
        let indexes = Data::Shared { vec, offset, len };

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
        self.into_inner().map(|_| ())
    }

    pub fn into_inner(mut self) -> io::Result<W> {

        for vec in &mut self.indexes {
            vec.sort_unstable();
        }

        let (ranges, values) = into_sliced_ranges(self.indexes, self.number_docs);
        let len = ranges.len() as u64;

        // TODO check if this is correct
        self.wtr.write_u64::<LittleEndian>(len)?;
        unsafe {
            // write Ranges first
            let slice = into_u8_slice(ranges.as_slice());
            self.wtr.write_all(slice)?;

            // write Values after
            let slice = into_u8_slice(values.as_slice());
            self.wtr.write_all(slice)?;
        }

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

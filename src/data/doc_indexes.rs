use std::slice::from_raw_parts;
use std::io::{self, Write};
use std::mem::size_of;
use std::ops::Index;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use sdset::Set;

use crate::DocIndex;
use crate::data::SharedData;

#[derive(Debug)]
#[repr(C)]
struct Range {
    start: u64,
    end: u64,
}

#[derive(Clone, Default)]
pub struct DocIndexes {
    ranges: SharedData,
    indexes: SharedData,
}

impl DocIndexes {
    pub fn from_bytes(vec: Vec<u8>) -> io::Result<Self> {
        let len = vec.len();
        DocIndexes::from_shared_bytes(Arc::new(vec), 0, len)
    }

    pub fn from_shared_bytes(bytes: Arc<Vec<u8>>, offset: usize, len: usize) -> io::Result<Self> {
        let data = SharedData { bytes, offset, len };
        DocIndexes::from_data(data)
    }

    fn from_data(data: SharedData) -> io::Result<Self> {
        let ranges_len_offset = data.len() - size_of::<u64>();
        let ranges_len = (&data[ranges_len_offset..]).read_u64::<LittleEndian>()?;
        let ranges_len = ranges_len as usize;

        let ranges_offset = ranges_len_offset - ranges_len;
        let ranges = data.range(ranges_offset, ranges_len);

        let indexes = data.range(0, ranges_offset);

        Ok(DocIndexes { ranges, indexes })
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let capacity = self.indexes.len() + self.ranges.len() + size_of::<u64>();
        let mut bytes = Vec::with_capacity(capacity);

        bytes.extend_from_slice(&self.indexes);
        bytes.extend_from_slice(&self.ranges);
        bytes.write_u64::<LittleEndian>(self.ranges.len() as u64).unwrap();

        bytes
    }

    pub fn get(&self, index: usize) -> Option<&Set<DocIndex>> {
        self.ranges().get(index as usize).map(|Range { start, end }| {
            let start = *start as usize;
            let end = *end as usize;
            let slice = &self.indexes()[start..end];
            Set::new_unchecked(slice)
        })
    }

    fn ranges(&self) -> &[Range] {
        let slice = &self.ranges;
        let ptr = slice.as_ptr() as *const Range;
        let len = slice.len() / size_of::<Range>();
        unsafe { from_raw_parts(ptr, len) }
    }

    fn indexes(&self) -> &[DocIndex] {
        let slice = &self.indexes;
        let ptr = slice.as_ptr() as *const DocIndex;
        let len = slice.len() / size_of::<DocIndex>();
        unsafe { from_raw_parts(ptr, len) }
    }
}

impl Index<usize> for DocIndexes {
    type Output = [DocIndex];

    fn index(&self, index: usize) -> &Self::Output {
        match self.get(index) {
            Some(indexes) => indexes,
            None => panic!("index {} out of range for a maximum of {} ranges", index, self.ranges().len()),
        }
    }
}

pub struct DocIndexesBuilder<W> {
    ranges: Vec<Range>,
    wtr: W,
}

impl DocIndexesBuilder<Vec<u8>> {
    pub fn memory() -> Self {
        DocIndexesBuilder::new(Vec::new())
    }
}

impl<W: Write> DocIndexesBuilder<W> {
    pub fn new(wtr: W) -> Self {
        DocIndexesBuilder {
            ranges: Vec::new(),
            wtr: wtr,
        }
    }

    pub fn insert(&mut self, indexes: &Set<DocIndex>) -> io::Result<()> {
        let len = indexes.len() as u64;
        let start = self.ranges.last().map(|r| r.end).unwrap_or(0);
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

unsafe fn into_u8_slice<T>(slice: &[T]) -> &[u8] {
    let ptr = slice.as_ptr() as *const u8;
    let len = slice.len() * size_of::<T>();
    from_raw_parts(ptr, len)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::error::Error;
    use crate::{Attribute, WordArea};

    use crate::DocumentId;

    #[test]
    fn builder_serialize_deserialize() -> Result<(), Box<Error>> {
        let a = DocIndex {
            document_id: DocumentId(0),
            attribute: Attribute::new_faillible(3, 11),
            word_area: WordArea::new_faillible(30, 4)
        };
        let b = DocIndex {
            document_id: DocumentId(1),
            attribute: Attribute::new_faillible(4, 21),
            word_area: WordArea::new_faillible(35, 6)
        };
        let c = DocIndex {
            document_id: DocumentId(2),
            attribute: Attribute::new_faillible(8, 2),
            word_area: WordArea::new_faillible(89, 6)
        };

        let mut builder = DocIndexesBuilder::memory();

        builder.insert(Set::new(&[a])?)?;
        builder.insert(Set::new(&[a, b, c])?)?;
        builder.insert(Set::new(&[a, c])?)?;

        let bytes = builder.into_inner()?;
        let docs = DocIndexes::from_bytes(bytes)?;

        assert_eq!(docs.get(0), Some(Set::new(&[a])?));
        assert_eq!(docs.get(1), Some(Set::new(&[a, b, c])?));
        assert_eq!(docs.get(2), Some(Set::new(&[a, c])?));
        assert_eq!(docs.get(3), None);

        Ok(())
    }

    #[test]
    fn serialize_deserialize() -> Result<(), Box<Error>> {
        let a = DocIndex {
            document_id: DocumentId(0),
            attribute: Attribute::new_faillible(3, 11),
            word_area: WordArea::new_faillible(30, 4)
        };
        let b = DocIndex {
            document_id: DocumentId(1),
            attribute: Attribute::new_faillible(4, 21),
            word_area: WordArea::new_faillible(35, 6)
        };
        let c = DocIndex {
            document_id: DocumentId(2),
            attribute: Attribute::new_faillible(8, 2),
            word_area: WordArea::new_faillible(89, 6)
        };

        let mut builder = DocIndexesBuilder::memory();

        builder.insert(Set::new(&[a])?)?;
        builder.insert(Set::new(&[a, b, c])?)?;
        builder.insert(Set::new(&[a, c])?)?;

        let builder_bytes = builder.into_inner()?;
        let docs = DocIndexes::from_bytes(builder_bytes.clone())?;
        let bytes = docs.to_vec();

        assert_eq!(builder_bytes, bytes);

        Ok(())
    }
}

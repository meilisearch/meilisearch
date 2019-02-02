use std::io::{self, Write, Cursor, BufRead};
use std::slice::from_raw_parts;
use std::mem::size_of;
use std::ops::Index;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use sdset::Set;

use crate::DocIndex;
use crate::data::SharedData;
use super::into_u8_slice;

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
    pub fn from_bytes(bytes: Vec<u8>) -> io::Result<DocIndexes> {
        let bytes = Arc::new(bytes);
        let len = bytes.len();
        let data = SharedData::new(bytes, 0, len);
        let mut  cursor = Cursor::new(data);
        DocIndexes::from_cursor(&mut cursor)
    }

    pub fn from_cursor(cursor: &mut Cursor<SharedData>) -> io::Result<DocIndexes> {
        let len = cursor.read_u64::<LittleEndian>()? as usize;
        let offset = cursor.position() as usize;
        let ranges = cursor.get_ref().range(offset, len);
        cursor.consume(len);

        let len = cursor.read_u64::<LittleEndian>()? as usize;
        let offset = cursor.position() as usize;
        let indexes = cursor.get_ref().range(offset, len);
        cursor.consume(len);

        Ok(DocIndexes { ranges, indexes })
    }

    pub fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        let ranges_len = self.ranges.len() as u64;
        let _ = bytes.write_u64::<LittleEndian>(ranges_len);
        bytes.extend_from_slice(&self.ranges);

        let indexes_len = self.indexes.len() as u64;
        let _ = bytes.write_u64::<LittleEndian>(indexes_len);
        bytes.extend_from_slice(&self.indexes);
    }

    pub fn get(&self, index: usize) -> Option<&Set<DocIndex>> {
        self.ranges().get(index).map(|Range { start, end }| {
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
    indexes: Vec<DocIndex>,
    wtr: W,
}

impl DocIndexesBuilder<Vec<u8>> {
    pub fn memory() -> Self {
        DocIndexesBuilder {
            ranges: Vec::new(),
            indexes: Vec::new(),
            wtr: Vec::new(),
        }
    }
}

impl<W: Write> DocIndexesBuilder<W> {
    pub fn new(wtr: W) -> Self {
        DocIndexesBuilder {
            ranges: Vec::new(),
            indexes: Vec::new(),
            wtr: wtr,
        }
    }

    pub fn insert(&mut self, indexes: &Set<DocIndex>) {
        let len = indexes.len() as u64;
        let start = self.ranges.last().map(|r| r.end).unwrap_or(0);
        let range = Range { start, end: start + len };
        self.ranges.push(range);

        self.indexes.extend_from_slice(indexes);
    }

    pub fn finish(self) -> io::Result<()> {
        self.into_inner().map(drop)
    }

    pub fn into_inner(mut self) -> io::Result<W> {
        let ranges = unsafe { into_u8_slice(&self.ranges) };
        let len = ranges.len() as u64;
        self.wtr.write_u64::<LittleEndian>(len)?;
        self.wtr.write_all(ranges)?;

        let indexes = unsafe { into_u8_slice(&self.indexes) };
        let len = indexes.len() as u64;
        self.wtr.write_u64::<LittleEndian>(len)?;
        self.wtr.write_all(indexes)?;

        Ok(self.wtr)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use crate::DocumentId;
    use super::*;

    #[test]
    fn builder_serialize_deserialize() -> Result<(), Box<Error>> {
        let a = DocIndex {
            document_id: DocumentId(0),
            attribute: 3,
            word_index: 11,
            char_index: 30,
            char_length: 4,
        };
        let b = DocIndex {
            document_id: DocumentId(1),
            attribute: 4,
            word_index: 21,
            char_index: 35,
            char_length: 6,
        };
        let c = DocIndex {
            document_id: DocumentId(2),
            attribute: 8,
            word_index: 2,
            char_index: 89,
            char_length: 6,
        };

        let mut builder = DocIndexesBuilder::memory();

        builder.insert(Set::new(&[a])?);
        builder.insert(Set::new(&[a, b, c])?);
        builder.insert(Set::new(&[a, c])?);

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
            attribute: 3,
            word_index: 11,
            char_index: 30,
            char_length: 4,
        };
        let b = DocIndex {
            document_id: DocumentId(1),
            attribute: 4,
            word_index: 21,
            char_index: 35,
            char_length: 6,
        };
        let c = DocIndex {
            document_id: DocumentId(2),
            attribute: 8,
            word_index: 2,
            char_index: 89,
            char_length: 6,
        };

        let mut builder = DocIndexesBuilder::memory();

        builder.insert(Set::new(&[a])?);
        builder.insert(Set::new(&[a, b, c])?);
        builder.insert(Set::new(&[a, c])?);

        let builder_bytes = builder.into_inner()?;
        let docs = DocIndexes::from_bytes(builder_bytes.clone())?;

        let mut bytes = Vec::new();
        docs.write_to_bytes(&mut bytes);

        assert_eq!(builder_bytes, bytes);

        Ok(())
    }
}

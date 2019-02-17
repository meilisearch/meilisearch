use std::error::Error;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fst::{map, Map, IntoStreamer, Streamer};
use fst::raw::Fst;
use sdset::duo::{Union, DifferenceByKey};
use sdset::{Set, SetOperation};

use crate::shared_data_cursor::{SharedDataCursor, FromSharedDataCursor};
use crate::write_to_bytes::WriteToBytes;
use crate::data::{DocIndexes, DocIndexesBuilder};
use crate::{DocumentId, DocIndex};

#[derive(Default)]
pub struct Index {
    pub map: Map,
    pub indexes: DocIndexes,
}

impl Index {
    pub fn remove_documents(&self, documents: &Set<DocumentId>) -> Index {
        let mut buffer = Vec::new();
        let mut builder = IndexBuilder::new();
        let mut stream = self.into_stream();

        while let Some((key, indexes)) = stream.next() {
            buffer.clear();

            let op = DifferenceByKey::new(indexes, documents, |x| x.document_id, |x| *x);
            op.extend_vec(&mut buffer);

            if !buffer.is_empty() {
                let indexes = Set::new_unchecked(&buffer);
                builder.insert(key, indexes).unwrap();
            }
        }

        builder.build()
    }

    pub fn union(&self, other: &Index) -> Index {
        let mut builder = IndexBuilder::new();
        let mut stream = map::OpBuilder::new().add(&self.map).add(&other.map).union();

        let mut buffer = Vec::new();
        while let Some((key, ivalues)) = stream.next() {
            buffer.clear();
            match ivalues {
                [a, b] => {
                    let indexes = if a.index == 0 { &self.indexes } else { &other.indexes };
                    let indexes = &indexes[a.value as usize];
                    let a = Set::new_unchecked(indexes);

                    let indexes = if b.index == 0 { &self.indexes } else { &other.indexes };
                    let indexes = &indexes[b.value as usize];
                    let b = Set::new_unchecked(indexes);

                    let op = Union::new(a, b);
                    op.extend_vec(&mut buffer);
                },
                [x] => {
                    let indexes = if x.index == 0 { &self.indexes } else { &other.indexes };
                    let indexes = &indexes[x.value as usize];
                    buffer.extend_from_slice(indexes)
                },
                _ => continue,
            }

            if !buffer.is_empty() {
                let indexes = Set::new_unchecked(&buffer);
                builder.insert(key, indexes).unwrap();
            }
        }

        builder.build()
    }
}

impl FromSharedDataCursor for Index {
    type Error = Box<Error>;

    fn from_shared_data_cursor(cursor: &mut SharedDataCursor) -> Result<Index, Self::Error> {
        let len = cursor.read_u64::<LittleEndian>()? as usize;
        let data = cursor.extract(len);

        let fst = Fst::from_shared_bytes(data.bytes, data.offset, data.len)?;
        let map = Map::from(fst);

        let indexes = DocIndexes::from_shared_data_cursor(cursor)?;

        Ok(Index { map, indexes})
    }
}

impl WriteToBytes for Index {
    fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        let slice = self.map.as_fst().as_bytes();
        let len = slice.len() as u64;
        let _ = bytes.write_u64::<LittleEndian>(len);
        bytes.extend_from_slice(slice);

        self.indexes.write_to_bytes(bytes);
    }
}

impl<'m, 'a> IntoStreamer<'a> for &'m Index {
    type Item = (&'a [u8], &'a Set<DocIndex>);
    type Into = Stream<'m>;

    fn into_stream(self) -> Self::Into {
        Stream {
            map_stream: self.map.into_stream(),
            indexes: &self.indexes,
        }
    }
}

pub struct Stream<'m> {
    map_stream: map::Stream<'m>,
    indexes: &'m DocIndexes,
}

impl<'m, 'a> Streamer<'a> for Stream<'m> {
    type Item = (&'a [u8], &'a Set<DocIndex>);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.map_stream.next() {
            Some((input, index)) => {
                let indexes = &self.indexes[index as usize];
                let indexes = Set::new_unchecked(indexes);
                Some((input, indexes))
            },
            None => None,
        }
    }
}

pub struct IndexBuilder {
    map: fst::MapBuilder<Vec<u8>>,
    indexes: DocIndexesBuilder<Vec<u8>>,
    value: u64,
}

impl IndexBuilder {
    pub fn new() -> Self {
        IndexBuilder {
            map: fst::MapBuilder::memory(),
            indexes: DocIndexesBuilder::memory(),
            value: 0,
        }
    }

    /// If a key is inserted that is less than or equal to any previous key added,
    /// then an error is returned. Similarly, if there was a problem writing
    /// to the underlying writer, an error is returned.
    // FIXME what if one write doesn't work but the other do ?
    pub fn insert<K>(&mut self, key: K, indexes: &Set<DocIndex>) -> fst::Result<()>
    where K: AsRef<[u8]>,
    {
        self.map.insert(key, self.value)?;
        self.indexes.insert(indexes);
        self.value += 1;
        Ok(())
    }

    pub fn build(self) -> Index {
        let map = self.map.into_inner().unwrap();
        let indexes = self.indexes.into_inner().unwrap();

        let map = Map::from_bytes(map).unwrap();
        let indexes = DocIndexes::from_bytes(indexes).unwrap();

        Index { map, indexes }
    }
}

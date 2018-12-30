use std::io::{Write, BufRead, Cursor};
use std::mem::size_of;
use std::error::Error;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fst::{map, Map, Streamer, IntoStreamer};
use sdset::{Set, SetOperation};
use sdset::duo::Union;
use fst::raw::Fst;

use crate::data::{DocIndexes, DocIndexesBuilder};
use crate::DocIndex;

#[derive(Default)]
pub struct Positive {
    pub map: Map,
    pub indexes: DocIndexes,
}

impl Positive {
    pub fn from_shared_bytes(
        bytes: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    ) -> Result<(Positive, usize), Box<Error>>
    {
        let mut cursor = Cursor::new(&bytes[..len]);
        cursor.consume(offset);

        let map_len = cursor.read_u64::<LittleEndian>()? as usize;
        let map_offset = cursor.position() as usize;
        let fst = Fst::from_shared_bytes(bytes.clone(), map_offset, map_len)?;
        let map = Map::from(fst);

        cursor.consume(map_len);
        let indexes_len = cursor.read_u64::<LittleEndian>()? as usize;
        let indexes_offset = cursor.position() as usize;
        let indexes = DocIndexes::from_shared_bytes(bytes, indexes_offset, indexes_len)?;

        let positive = Positive { map, indexes };
        let len = indexes_offset + indexes_len;

        Ok((positive, len))
    }

    pub fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        // indexes
        let slice = self.map.as_fst().as_bytes();
        let len = slice.len() as u64;
        let _ = bytes.write_u64::<LittleEndian>(len);
        bytes.extend_from_slice(slice);

        // map
        self.indexes.write_to_bytes(bytes);
    }

    pub fn union(&self, other: &Positive) -> Result<Positive, Box<Error>> {
        let mut builder = PositiveBuilder::memory();
        let mut stream = map::OpBuilder::new().add(&self.map).add(&other.map).union();

        let mut buffer = Vec::new();
        while let Some((key, ivalues)) = stream.next() {
            buffer.clear();
            match ivalues {
                [a, b] => {
                    let indexes = if a.index == 0 { &self.indexes } else { &other.indexes };
                    let indexes = indexes.get(a.value as usize).ok_or(format!("index not found"))?;
                    let a = Set::new_unchecked(indexes);

                    let indexes = if b.index == 0 { &self.indexes } else { &other.indexes };
                    let indexes = indexes.get(b.value as usize).ok_or(format!("index not found"))?;
                    let b = Set::new_unchecked(indexes);

                    let op = Union::new(a, b);
                    op.extend_vec(&mut buffer);
                },
                [a] => {
                    let indexes = if a.index == 0 { &self.indexes } else { &other.indexes };
                    let indexes = indexes.get(a.value as usize).ok_or(format!("index not found"))?;
                    buffer.extend_from_slice(indexes)
                },
                _ => continue,
            }

            if !buffer.is_empty() {
                let indexes = Set::new_unchecked(&buffer);
                builder.insert(key, indexes)?;
            }
        }

        let (map, indexes) = builder.into_inner()?;
        let map = Map::from_bytes(map)?;
        let indexes = DocIndexes::from_bytes(indexes)?;
        Ok(Positive { map, indexes })
    }
}

impl<'m, 'a> IntoStreamer<'a> for &'m Positive {
    type Item = (&'a [u8], &'a Set<DocIndex>);
    /// The type of the stream to be constructed.
    type Into = Stream<'m>;

    /// Construct a stream from `Self`.
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

pub struct PositiveBuilder<W, X> {
    map: fst::MapBuilder<W>,
    indexes: DocIndexesBuilder<X>,
    value: u64,
}

impl PositiveBuilder<Vec<u8>, Vec<u8>> {
    pub fn memory() -> Self {
        PositiveBuilder {
            map: fst::MapBuilder::memory(),
            indexes: DocIndexesBuilder::memory(),
            value: 0,
        }
    }
}

impl<W: Write, X: Write> PositiveBuilder<W, X> {
    /// If a key is inserted that is less than or equal to any previous key added,
    /// then an error is returned. Similarly, if there was a problem writing
    /// to the underlying writer, an error is returned.
    // FIXME what if one write doesn't work but the other do ?
    pub fn insert<K>(&mut self, key: K, indexes: &Set<DocIndex>) -> Result<(), Box<Error>>
    where K: AsRef<[u8]>,
    {
        self.map.insert(key, self.value)?;
        self.indexes.insert(indexes)?;
        self.value += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<(), Box<Error>> {
        self.into_inner().map(drop)
    }

    pub fn into_inner(self) -> Result<(W, X), Box<Error>> {
        let map = self.map.into_inner()?;
        let indexes = self.indexes.into_inner()?;
        Ok((map, indexes))
    }
}

use std::fmt;
use std::io::Write;
use std::path::Path;
use std::error::Error;

use fst::{map, Map, Streamer, IntoStreamer};

use crate::DocIndex;
use crate::data::{DocIndexes, RawDocIndexesBuilder};
use serde::ser::{Serialize, Serializer, SerializeTuple};
use serde::de::{self, Deserialize, Deserializer, SeqAccess, Visitor};

#[derive(Default)]
pub struct PositiveBlob {
    map: Map,
    indexes: DocIndexes,
}

impl PositiveBlob {
    pub unsafe fn from_paths<P, Q>(map: P, indexes: Q) -> Result<Self, Box<Error>>
    where P: AsRef<Path>,
          Q: AsRef<Path>,
    {
        let map = Map::from_path(map)?;
        let indexes = DocIndexes::from_path(indexes)?;
        Ok(PositiveBlob { map, indexes })
    }

    pub fn from_bytes(map: Vec<u8>, indexes: Vec<u8>) -> Result<Self, Box<Error>> {
        let map = Map::from_bytes(map)?;
        let indexes = DocIndexes::from_bytes(indexes)?;
        Ok(PositiveBlob { map, indexes })
    }

    pub fn from_raw(map: Map, indexes: DocIndexes) -> Self {
        PositiveBlob { map, indexes }
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

impl<'m, 'a> IntoStreamer<'a> for &'m PositiveBlob {
    type Item = (&'a [u8], &'a [DocIndex]);
    /// The type of the stream to be constructed.
    type Into = PositiveBlobStream<'m>;

    /// Construct a stream from `Self`.
    fn into_stream(self) -> Self::Into {
        PositiveBlobStream {
            map_stream: self.map.into_stream(),
            doc_indexes: &self.indexes,
        }
    }
}

pub struct PositiveBlobStream<'m> {
    map_stream: map::Stream<'m>,
    doc_indexes: &'m DocIndexes,
}

impl<'m, 'a> Streamer<'a> for PositiveBlobStream<'m> {
    type Item = (&'a [u8], &'a [DocIndex]);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.map_stream.next() {
            Some((input, index)) => {
                let doc_indexes = self.doc_indexes.get(index);
                let doc_indexes = doc_indexes.expect("BUG: could not find document indexes");
                Some((input, doc_indexes))
            },
            None => None,
        }
    }
}

impl Serialize for PositiveBlob {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(2)?;
        tuple.serialize_element(&self.map.as_fst().to_vec())?;
        tuple.serialize_element(&self.indexes)?;
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for PositiveBlob {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<PositiveBlob, D::Error> {
        struct TupleVisitor;

        impl<'de> Visitor<'de> for TupleVisitor {
            type Value = PositiveBlob;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a PositiveBlob struct")
            }

            #[inline]
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let map = match seq.next_element()? {
                    Some(bytes) => match Map::from_bytes(bytes) {
                        Ok(value) => value,
                        Err(err) => return Err(de::Error::custom(err)),
                    },
                    None => return Err(de::Error::invalid_length(0, &self)),
                };

                let indexes = match seq.next_element()? {
                    Some(bytes) => match DocIndexes::from_bytes(bytes) {
                        Ok(value) => value,
                        Err(err) => return Err(de::Error::custom(err)),
                    },
                    None => return Err(de::Error::invalid_length(1, &self)),
                };

                Ok(PositiveBlob { map, indexes })
            }
        }

        deserializer.deserialize_tuple(2, TupleVisitor)
    }
}

pub struct PositiveBlobBuilder<W, X> {
    map: fst::MapBuilder<W>,
    indexes: RawDocIndexesBuilder<X>,
    value: u64,
}

impl PositiveBlobBuilder<Vec<u8>, Vec<u8>> {
    pub fn memory() -> Self {
        PositiveBlobBuilder {
            map: fst::MapBuilder::memory(),
            indexes: RawDocIndexesBuilder::memory(),
            value: 0,
        }
    }
}

impl<W: Write, X: Write> PositiveBlobBuilder<W, X> {
    pub fn new(map: W, indexes: X) -> Result<Self, Box<Error>> {
        Ok(PositiveBlobBuilder {
            map: fst::MapBuilder::new(map)?,
            indexes: RawDocIndexesBuilder::new(indexes),
            value: 0,
        })
    }

    /// If a key is inserted that is less than or equal to any previous key added,
    /// then an error is returned. Similarly, if there was a problem writing
    /// to the underlying writer, an error is returned.
    // FIXME what if one write doesn't work but the other do ?
    pub fn insert(&mut self, key: &[u8], doc_indexes: &[DocIndex]) -> Result<(), Box<Error>> {
        self.map.insert(key, self.value)?;
        self.indexes.insert(doc_indexes)?;
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

use std::fmt;
use std::io::Write;
use std::path::Path;
use std::error::Error;

use fst::{map, Map, Streamer, IntoStreamer};
use sdset::Set;

use crate::DocIndex;
use crate::data::{DocIndexes, DocIndexesBuilder};
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
        self.map.get(key).map(|index| &self.indexes[index as usize])
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

impl fmt::Debug for PositiveBlob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PositiveBlob([")?;
        let mut stream = self.into_stream();
        let mut first = true;
        while let Some((k, v)) = stream.next() {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "({}, {:?})", String::from_utf8_lossy(k), v)?;
        }
        write!(f, "])")
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
                let doc_indexes = &self.doc_indexes[index as usize];
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
        tuple.serialize_element(&self.indexes.to_vec())?;
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
    indexes: DocIndexesBuilder<X>,
    value: u64,
}

impl PositiveBlobBuilder<Vec<u8>, Vec<u8>> {
    pub fn memory() -> Self {
        PositiveBlobBuilder {
            map: fst::MapBuilder::memory(),
            indexes: DocIndexesBuilder::memory(),
            value: 0,
        }
    }
}

impl<W: Write, X: Write> PositiveBlobBuilder<W, X> {
    pub fn new(map: W, indexes: X) -> Result<Self, Box<Error>> {
        Ok(PositiveBlobBuilder {
            map: fst::MapBuilder::new(map)?,
            indexes: DocIndexesBuilder::new(indexes),
            value: 0,
        })
    }

    /// If a key is inserted that is less than or equal to any previous key added,
    /// then an error is returned. Similarly, if there was a problem writing
    /// to the underlying writer, an error is returned.
    // FIXME what if one write doesn't work but the other do ?
    pub fn insert<K>(&mut self, key: K, doc_indexes: &Set<DocIndex>) -> Result<(), Box<Error>>
    where K: AsRef<[u8]>,
    {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn serialize_deserialize() -> Result<(), Box<Error>> {
        let a = DocIndex { document_id: 0, attribute: 3, attribute_index: 11 };
        let b = DocIndex { document_id: 1, attribute: 4, attribute_index: 21 };
        let c = DocIndex { document_id: 2, attribute: 8, attribute_index: 2 };

        let mut builder = PositiveBlobBuilder::memory();

        builder.insert("aaa", Set::new(&[a])?)?;
        builder.insert("aab", Set::new(&[a, b, c])?)?;
        builder.insert("aac", Set::new(&[a, c])?)?;

        let (map_bytes, indexes_bytes) = builder.into_inner()?;
        let positive_blob = PositiveBlob::from_bytes(map_bytes, indexes_bytes)?;

        assert_eq!(positive_blob.get("aaa"), Some(&[a][..]));
        assert_eq!(positive_blob.get("aab"), Some(&[a, b, c][..]));
        assert_eq!(positive_blob.get("aac"), Some(&[a, c][..]));
        assert_eq!(positive_blob.get("aad"), None);

        Ok(())
    }

    #[test]
    fn serde_serialize_deserialize() -> Result<(), Box<Error>> {
        let a = DocIndex { document_id: 0, attribute: 3, attribute_index: 11 };
        let b = DocIndex { document_id: 1, attribute: 4, attribute_index: 21 };
        let c = DocIndex { document_id: 2, attribute: 8, attribute_index: 2 };

        let mut builder = PositiveBlobBuilder::memory();

        builder.insert("aaa", Set::new(&[a])?)?;
        builder.insert("aab", Set::new(&[a, b, c])?)?;
        builder.insert("aac", Set::new(&[a, c])?)?;

        let (map_bytes, indexes_bytes) = builder.into_inner()?;
        let positive_blob = PositiveBlob::from_bytes(map_bytes, indexes_bytes)?;

        let bytes = bincode::serialize(&positive_blob)?;
        let positive_blob: PositiveBlob = bincode::deserialize(&bytes)?;

        assert_eq!(positive_blob.get("aaa"), Some(&[a][..]));
        assert_eq!(positive_blob.get("aab"), Some(&[a, b, c][..]));
        assert_eq!(positive_blob.get("aac"), Some(&[a, c][..]));
        assert_eq!(positive_blob.get("aad"), None);

        Ok(())
    }
}

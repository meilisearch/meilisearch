use std::io::{Write, Cursor, BufRead};
use std::convert::From;
use std::error::Error;
use std::sync::Arc;
use std::fmt;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fst::{map, Map, Streamer, IntoStreamer};
use fst::raw::Fst;
use sdset::Set;

use crate::DocIndex;
use crate::data::{DocIndexes, DocIndexesBuilder};

#[derive(Default)]
pub struct PositiveBlob {
    map: Map,
    indexes: DocIndexes,
}

impl PositiveBlob {
    pub fn from_bytes(map: Vec<u8>, indexes: Vec<u8>) -> Result<Self, Box<Error>> {
        let map = Map::from_bytes(map)?;
        let indexes = DocIndexes::from_bytes(indexes)?;
        Ok(PositiveBlob { map, indexes })
    }

    pub fn from_raw(map: Map, indexes: DocIndexes) -> Self {
        PositiveBlob { map, indexes }
    }

    pub fn from_shared_bytes(bytes: Arc<Vec<u8>>, offset: usize, len: usize) -> Result<Self, Box<Error>> {
        let mut cursor = Cursor::new(&bytes.as_slice()[..len]);
        cursor.consume(offset);

        let map_len = cursor.read_u64::<LittleEndian>()? as usize;
        let offset = cursor.position() as usize;
        let map = Map::from(Fst::from_shared_bytes(bytes.clone(), offset, map_len)?);

        cursor.consume(map_len);

        let doc_len = cursor.read_u64::<LittleEndian>()? as usize;
        let offset = cursor.position() as usize;
        let doc_indexes = DocIndexes::from_shared_bytes(bytes, offset, doc_len)?;

        Ok(PositiveBlob::from_raw(map, doc_indexes))
    }

    pub fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        let map_bytes = self.map.as_fst().as_bytes();
        bytes.write_u64::<LittleEndian>(map_bytes.len() as u64).unwrap();
        bytes.extend_from_slice(&map_bytes);

        let doc_indexes_vec = self.indexes.to_vec(); // FIXME patch to have a as_slice() function
        bytes.write_u64::<LittleEndian>(doc_indexes_vec.len() as u64).unwrap();
        bytes.extend_from_slice(&doc_indexes_vec);
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
    use crate::{Attribute, WordArea};

    use crate::DocumentId;

    #[test]
    fn create_query() -> Result<(), Box<Error>> {
        let a = DocIndex { document_id: DocumentId(0), attribute: Attribute::new(3, 11), word_area: WordArea::new(30, 4) };
        let b = DocIndex { document_id: DocumentId(1), attribute: Attribute::new(4, 21), word_area: WordArea::new(35, 6) };
        let c = DocIndex { document_id: DocumentId(2), attribute: Attribute::new(8, 2), word_area: WordArea::new(89, 6) };

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
    fn serialize_deserialize() -> Result<(), Box<Error>> {
        let a = DocIndex { document_id: DocumentId(0), attribute: Attribute::new(3, 11), word_area: WordArea::new(30, 4) };
        let b = DocIndex { document_id: DocumentId(1), attribute: Attribute::new(4, 21), word_area: WordArea::new(35, 6) };
        let c = DocIndex { document_id: DocumentId(2), attribute: Attribute::new(8, 2), word_area: WordArea::new(89, 6) };

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
        let a = DocIndex { document_id: DocumentId(0), attribute: Attribute::new(3, 11), word_area: WordArea::new(30, 4) };
        let b = DocIndex { document_id: DocumentId(1), attribute: Attribute::new(4, 21), word_area: WordArea::new(35, 6) };
        let c = DocIndex { document_id: DocumentId(2), attribute: Attribute::new(8, 2), word_area: WordArea::new(89, 6) };

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
}

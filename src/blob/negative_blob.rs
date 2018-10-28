use std::error::Error;
use std::path::Path;
use std::io::Write;

use fst::{Map, MapBuilder};

use crate::DocIndex;
use crate::doc_indexes::{DocIndexes, DocIndexesBuilder};

pub struct NegativeBlob {
    map: Map,
    indexes: DocIndexes,
}

impl NegativeBlob {
    pub unsafe fn from_paths<P, Q>(map: P, indexes: Q) -> Result<Self, Box<Error>>
    where P: AsRef<Path>,
          Q: AsRef<Path>,
    {
        let map = Map::from_path(map)?;
        let indexes = DocIndexes::from_path(indexes)?;
        Ok(NegativeBlob { map, indexes })
    }

    pub fn from_bytes(map: Vec<u8>, indexes: Vec<u8>) -> Result<Self, Box<Error>> {
        let map = Map::from_bytes(map)?;
        let indexes = DocIndexes::from_bytes(indexes)?;
        Ok(NegativeBlob { map, indexes })
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

pub struct NegativeBlobBuilder<W, X> {
    map: W,
    indexes: DocIndexesBuilder<X>,
}

impl<W: Write, X: Write> NegativeBlobBuilder<W, X> {
    pub fn new(map: W, indexes: X) -> Self {
        Self { map, indexes: DocIndexesBuilder::new(indexes) }
    }

    pub fn insert<S: Into<String>>(&mut self, key: S, index: DocIndex) {
        self.indexes.insert(key.into(), index)
    }

    pub fn finish(self) -> Result<(), Box<Error>> {
        self.into_inner().map(|_| ())
    }

    pub fn into_inner(self) -> Result<(W, X), Box<Error>> {
        // FIXME insert a magic number that indicates if the endianess
        //       of the input is the same as the machine that is reading it.

        let map = {
            let mut keys_builder = MapBuilder::new(self.map)?;
            let keys = self.indexes.keys().map(|(s, v)| (s, *v));
            keys_builder.extend_iter(keys)?;
            keys_builder.into_inner()?
        };

        let indexes = self.indexes.into_inner()?;

        Ok((map, indexes))
    }
}

impl NegativeBlobBuilder<Vec<u8>, Vec<u8>> {
    pub fn build(self) -> Result<NegativeBlob, Box<Error>> {
        self.into_inner().and_then(|(m, i)| NegativeBlob::from_bytes(m, i))
    }
}

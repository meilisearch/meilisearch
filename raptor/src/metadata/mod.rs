pub mod ops;
pub mod stream_ops;
pub mod doc_indexes;
pub mod difference;
pub mod ops_indexed_value;

use fst::{Map, MapBuilder};
use std::error::Error;
use std::path::Path;
use std::io::Write;
use crate::DocIndex;
use self::doc_indexes::{DocIndexes, DocIndexesBuilder};

pub struct Metadata {
    map: Map,
    indexes: DocIndexes,
}

impl Metadata {
    pub unsafe fn from_paths<P, Q>(map: P, indexes: Q) -> Result<Self, Box<Error>>
    where P: AsRef<Path>,
          Q: AsRef<Path>,
    {
        let map = Map::from_path(map)?;
        let indexes = DocIndexes::from_path(indexes)?;
        Ok(Metadata { map, indexes })
    }

    pub fn from_bytes(map: Vec<u8>, indexes: Vec<u8>) -> Result<Self, Box<Error>> {
        let map = Map::from_bytes(map)?;
        let indexes = DocIndexes::from_bytes(indexes)?;
        Ok(Metadata { map, indexes })
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

pub struct MetadataBuilder<W, X> {
    map: W,
    indexes: DocIndexesBuilder<X>,
}

impl<W: Write, X: Write> MetadataBuilder<W, X> {
    pub fn new(map: W, indexes: X) -> Self {
        Self { map, indexes: DocIndexesBuilder::new(indexes) }
    }

    pub fn insert(&mut self, key: String, index: DocIndex) {
        self.indexes.insert(key, index)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_serialize_deserialize() {
        let mapw = Vec::new();
        let indexesw = Vec::new();

        let builder = MetadataBuilder::new(mapw, indexesw);
        let (map, indexes) = builder.into_inner().unwrap();

        let metas = Metadata::from_bytes(map, indexes).unwrap();
        assert_eq!(metas.get("chameau"), None);
    }

    #[test]
    fn one_doc_serialize_deserialize() {
        let mapw = Vec::new();
        let indexesw = Vec::new();

        let mut builder = MetadataBuilder::new(mapw, indexesw);

        let doc = DocIndex { document: 12, attribute: 1, attribute_index: 22 };
        builder.insert("chameau".into(), doc);

        let (map, indexes) = builder.into_inner().unwrap();

        let metas = Metadata::from_bytes(map, indexes).unwrap();
        assert_eq!(metas.get("chameau"), Some(&[doc][..]));
    }

    #[test]
    fn multiple_docs_serialize_deserialize() {
        let mapw = Vec::new();
        let indexesw = Vec::new();

        let mut builder = MetadataBuilder::new(mapw, indexesw);

        let doc1 = DocIndex { document: 12, attribute: 1, attribute_index: 22 };
        let doc2 = DocIndex { document: 31, attribute: 0, attribute_index: 1 };
        builder.insert("chameau".into(), doc1);
        builder.insert("chameau".into(), doc2);

        let (map, indexes) = builder.into_inner().unwrap();

        let metas = Metadata::from_bytes(map, indexes).unwrap();
        assert_eq!(metas.get("chameau"), Some(&[doc1, doc2][..]));
    }
}

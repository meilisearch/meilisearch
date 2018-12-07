#![allow(unused)]

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;

use crate::database::blob::positive::PositiveBlobBuilder;
use crate::DocIndex;

pub struct UnorderedPositiveBlobBuilder<W, X> {
    builder: PositiveBlobBuilder<W, X>,
    map: BTreeMap<Vec<u8>, Vec<DocIndex>>,
}

impl UnorderedPositiveBlobBuilder<Vec<u8>, Vec<u8>> {
    pub fn memory() -> Self {
        Self {
            builder: PositiveBlobBuilder::memory(),
            map: BTreeMap::new(),
        }
    }
}

impl<W: Write, X: Write> UnorderedPositiveBlobBuilder<W, X> {
    pub fn new(map_wtr: W, doc_wtr: X) -> Result<Self, Box<Error>> {
        Ok(UnorderedPositiveBlobBuilder {
            builder: PositiveBlobBuilder::new(map_wtr, doc_wtr)?,
            map: BTreeMap::new(),
        })
    }

    pub fn insert<K: Into<Vec<u8>>>(&mut self, input: K, doc_index: DocIndex) {
        self.map.entry(input.into()).or_insert_with(Vec::new).push(doc_index);
    }

    pub fn finish(self) -> Result<(), Box<Error>> {
        self.into_inner().map(drop)
    }

    pub fn into_inner(mut self) -> Result<(W, X), Box<Error>> {
        for (key, mut doc_indexes) in self.map {
            doc_indexes.sort_unstable();
            self.builder.insert(&key, &doc_indexes)?;
        }
        self.builder.into_inner()
    }
}

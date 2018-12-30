use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::error::Error;

use fst::map::{Map, MapBuilder};
use rocksdb::rocksdb_options;
use serde::Serialize;
use sdset::Set;

use crate::database::index::{Index, Positive, PositiveBuilder, Negative};
use crate::database::{DATA_INDEX, Schema, DocumentKeyAttr};
use crate::data::{DocIds, DocIndexes};
use crate::{DocumentId, DocIndex};
use super::Update;

type Token = Vec<u8>; // TODO could be replaced by a SmallVec
type Value = Vec<u8>;

pub struct UpdateBuilder {
    sst_file: PathBuf,
    schema: Schema,
    removed_documents: BTreeSet<DocumentId>,
    words_indexes: BTreeMap<Token, Vec<DocIndex>>,
    keys_values: BTreeMap<DocumentKeyAttr, Value>,
}

impl UpdateBuilder {
    pub fn new(path: PathBuf, schema: Schema) -> UpdateBuilder {
        UpdateBuilder {
            sst_file: path,
            schema: schema,
            removed_documents: BTreeSet::new(),
            words_indexes: BTreeMap::new(),
            keys_values: BTreeMap::new(),
        }
    }

    pub fn update_document<T>(&mut self, document: T) -> Result<DocumentId, Box<Error>>
    where T: Serialize,
    {
        unimplemented!()
    }

    pub fn remove_document<T>(&mut self, document: T) -> Result<DocumentId, Box<Error>>
    where T: Serialize,
    {
        unimplemented!()
    }

    pub fn build(self) -> Result<Update, Box<Error>> {
        let tree = {
            let negative = {
                let documents_ids = self.removed_documents.into_iter().collect();
                let doc_ids = DocIds::from_raw(documents_ids);
                Negative { doc_ids }
            };

            let positive = {
                let mut builder = PositiveBuilder::memory();

                for (key, mut indexes) in self.words_indexes {
                    indexes.sort_unstable();
                    let indexes = Set::new_unchecked(&indexes);
                    builder.insert(key, indexes);
                }

                let (map, indexes) = builder.into_inner()?;
                let map = Map::from_bytes(map)?;
                let indexes = DocIndexes::from_bytes(indexes)?;
                Positive { map, indexes }
            };

            Index { negative, positive }
        };

        let env_options = rocksdb_options::EnvOptions::new();
        let column_family_options = rocksdb_options::ColumnFamilyOptions::new();
        let mut file_writer = rocksdb::SstFileWriter::new(env_options, column_family_options);
        file_writer.open(&self.sst_file.to_string_lossy())?;

        // write the data-index
        let mut bytes = Vec::new();
        tree.write_to_bytes(&mut bytes);
        file_writer.merge(DATA_INDEX, &bytes)?;

        // write all the documents attributes updates
        for (key, value) in self.keys_values {
            file_writer.put(key.as_ref(), &value)?;
        }

        file_writer.finish()?;

        Ok(Update { sst_file: self.sst_file })
    }
}

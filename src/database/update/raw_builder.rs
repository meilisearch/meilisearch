use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::error::Error;

use rocksdb::rocksdb_options;
use fst::map::Map;
use sdset::Set;

use crate::database::index::{Index, Positive, PositiveBuilder, Negative};
use crate::database::{DATA_INDEX, DocumentKeyAttr};
use crate::data::{DocIds, DocIndexes};
use crate::{DocumentId, DocIndex};
use super::Update;

type Token = Vec<u8>; // TODO could be replaced by a SmallVec
type Value = Vec<u8>;

pub struct RawUpdateBuilder {
    sst_file: PathBuf,
    removed_documents: BTreeSet<DocumentId>,
    words_indexes: BTreeMap<Token, Vec<DocIndex>>,
    keys_values: BTreeMap<DocumentKeyAttr, Value>,
}

impl RawUpdateBuilder {
    pub fn new(path: PathBuf) -> RawUpdateBuilder {
        RawUpdateBuilder {
            sst_file: path,
            removed_documents: BTreeSet::new(),
            words_indexes: BTreeMap::new(),
            keys_values: BTreeMap::new(),
        }
    }

    pub fn insert_doc_index(&mut self, token: Vec<u8>, doc_index: DocIndex) {
        self.words_indexes.entry(token).or_insert_with(Vec::new).push(doc_index)
    }

    pub fn insert_attribute_value(&mut self, key_attr: DocumentKeyAttr, value: Vec<u8>) -> Option<Vec<u8>> {
        self.keys_values.insert(key_attr, value)
    }

    pub fn remove_document(&mut self, id: DocumentId) {
        self.removed_documents.insert(id);
    }

    pub fn build(self) -> Result<Update, Box<Error>> {
        let tree = {
            let negative = {
                let documents_ids: Vec<_> = self.removed_documents.into_iter().collect();
                let documents_ids = Set::new_unchecked(&documents_ids);
                let doc_ids = DocIds::new(documents_ids);
                Negative::new(doc_ids)
            };

            let positive = {
                let mut builder = PositiveBuilder::memory();

                for (key, mut indexes) in self.words_indexes {
                    indexes.sort_unstable();
                    let indexes = Set::new_unchecked(&indexes);
                    builder.insert(key, indexes)?;
                }

                let (map, indexes) = builder.into_inner()?;
                let map = Map::from_bytes(map)?;
                let indexes = DocIndexes::from_bytes(indexes)?;
                Positive::new(map, indexes)
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

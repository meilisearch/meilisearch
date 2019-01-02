use std::collections::BTreeMap;
use std::path::PathBuf;
use std::error::Error;

use rocksdb::rocksdb_options;
use hashbrown::HashMap;
use fst::map::Map;
use sdset::Set;

use crate::database::index::{Index, Positive, PositiveBuilder, Negative};
use crate::database::{DATA_INDEX, DocumentKeyAttr};
use crate::database::schema::SchemaAttr;
use crate::data::{DocIds, DocIndexes};
use crate::{DocumentId, DocIndex};
use super::Update;

type Token = Vec<u8>; // TODO could be replaced by a SmallVec
type Value = Vec<u8>;

pub struct RawUpdateBuilder {
    sst_file: PathBuf,
    document_updates: BTreeMap<DocumentId, DocumentUpdate>,
}

pub struct DocumentUpdate {
    cleared: bool,
    words_indexes: HashMap<Token, Vec<DocIndex>>,
    attributes: BTreeMap<SchemaAttr, Value>,
}

impl DocumentUpdate {
    pub fn new() -> DocumentUpdate {
        DocumentUpdate {
            cleared: false,
            words_indexes: HashMap::new(),
            attributes: BTreeMap::new(),
        }
    }

    pub fn remove(&mut self) {
        self.cleared = true;
        self.words_indexes.clear();
        self.attributes.clear();
    }

    pub fn insert_attribute_value(&mut self, attr: SchemaAttr, value: Vec<u8>) {
        self.attributes.insert(attr, value);
    }

    pub fn insert_doc_index(&mut self, token: Vec<u8>, doc_index: DocIndex) {
        self.words_indexes.entry(token).or_insert_with(Vec::new).push(doc_index)
    }
}

impl RawUpdateBuilder {
    pub fn new(path: PathBuf) -> RawUpdateBuilder {
        RawUpdateBuilder {
            sst_file: path,
            document_updates: BTreeMap::new(),
        }
    }

    pub fn document_update(&mut self, document_id: DocumentId) -> &mut DocumentUpdate {
        self.document_updates.entry(document_id).or_insert_with(DocumentUpdate::new)
    }

    pub fn build(mut self) -> Result<Update, Box<Error>> {
        let mut removed_document_ids = Vec::new();
        let mut words_indexes = BTreeMap::new();

        for (&id, update) in self.document_updates.iter_mut() {
            if update.cleared { removed_document_ids.push(id) }

            for (token, indexes) in &update.words_indexes {
                words_indexes.entry(token).or_insert_with(Vec::new).extend_from_slice(indexes)
            }
        }

        let negative = {
            let removed_document_ids = Set::new_unchecked(&removed_document_ids);
            let doc_ids = DocIds::new(removed_document_ids);
            Negative::new(doc_ids)
        };

        let positive = {
            let mut positive_builder = PositiveBuilder::memory();

            for (key, mut indexes) in words_indexes {
                indexes.sort_unstable();
                let indexes = Set::new_unchecked(&indexes);
                positive_builder.insert(key, indexes)?;
            }

            let (map, indexes) = positive_builder.into_inner()?;
            let map = Map::from_bytes(map)?;
            let indexes = DocIndexes::from_bytes(indexes)?;
            Positive::new(map, indexes)
        };

        let index = Index { negative, positive };

        let env_options = rocksdb_options::EnvOptions::new();
        let column_family_options = rocksdb_options::ColumnFamilyOptions::new();
        let mut file_writer = rocksdb::SstFileWriter::new(env_options, column_family_options);
        file_writer.open(&self.sst_file.to_string_lossy())?;

        // write the data-index
        let mut bytes = Vec::new();
        index.write_to_bytes(&mut bytes);
        file_writer.merge(DATA_INDEX, &bytes)?;

        // write all the documents attributes updates
        for (id, update) in self.document_updates {

            let mut last_attr: Option<SchemaAttr> = None;
            for (attr, value) in update.attributes {

                if update.cleared {
                    // if there is no last attribute, remove from the first attribute
                    let start_attr = match last_attr {
                        Some(attr) => attr.next(),
                        None       => Some(SchemaAttr::min())
                    };
                    let start = start_attr.map(|a| DocumentKeyAttr::new(id, a));
                    let end = attr.prev().map(|a| DocumentKeyAttr::new(id, a));

                    // delete_range between (last_attr + 1) and (attr - 1)
                    if let (Some(start), Some(end)) = (start, end) {
                        file_writer.delete_range(start.as_ref(), end.as_ref())?;
                    }
                }

                let key = DocumentKeyAttr::new(id, attr);
                file_writer.put(key.as_ref(), &value)?;
                last_attr = Some(attr);
            }

            if update.cleared {
                // if there is no last attribute, remove from the first attribute
                let start_attr = match last_attr {
                    Some(attr) => attr.next(),
                    None       => Some(SchemaAttr::min())
                };
                let start = start_attr.map(|a| DocumentKeyAttr::new(id, a));
                let end = DocumentKeyAttr::with_attribute_max(id);

                // delete_range between (last_attr + 1) and attr_max
                if let Some(start) = start {
                    file_writer.delete_range(start.as_ref(), end.as_ref())?;
                }
            }
        }

        file_writer.finish()?;

        Ok(Update { sst_file: self.sst_file })
    }
}

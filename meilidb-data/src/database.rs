use std::path::Path;
use std::sync::Arc;

use arc_swap::{ArcSwap, Lease};
use hashbrown::HashMap;
use meilidb_core::shared_data_cursor::{FromSharedDataCursor, SharedDataCursor};
use meilidb_core::write_to_bytes::WriteToBytes;
use meilidb_core::{DocumentId, Index as WordIndex};
use sled::IVec;

use crate::{Schema, SchemaAttr};

#[derive(Debug)]
pub enum Error {
    SchemaDiffer,
    SchemaMissing,
    WordIndexMissing,
    SledError(sled::Error),
    BincodeError(bincode::Error),
}

impl From<sled::Error> for Error {
    fn from(error: sled::Error) -> Error {
        Error::SledError(error)
    }
}

impl From<bincode::Error> for Error {
    fn from(error: bincode::Error) -> Error {
        Error::BincodeError(error)
    }
}

fn index_name(name: &str) -> Vec<u8> {
    format!("index-{}", name).into_bytes()
}

fn document_key(id: DocumentId, attr: SchemaAttr) -> Vec<u8> {
    let DocumentId(document_id) = id;
    let SchemaAttr(schema_attr) = attr;

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"document-");
    bytes.extend_from_slice(&document_id.to_be_bytes()[..]);
    bytes.extend_from_slice(&schema_attr.to_be_bytes()[..]);
    bytes
}

fn ivec_into_arc(ivec: IVec) -> Arc<[u8]> {
    match ivec {
        IVec::Inline(len, bytes) => Arc::from(&bytes[..len as usize]),
        IVec::Remote { buf } => buf,
    }
}

#[derive(Clone)]
pub struct Database {
    opened: Arc<ArcSwap<HashMap<String, Index>>>,
    inner: sled::Db,
}

impl Database {
    pub fn start_default<P: AsRef<Path>>(path: P) -> Result<Database, Error> {
        let inner = sled::Db::start_default(path)?;
        let opened = Arc::new(ArcSwap::new(Arc::new(HashMap::new())));
        Ok(Database { opened, inner })
    }

    pub fn open_index(&self, name: &str) -> Result<Option<Index>, Error> {
        // check if the index was already opened
        if let Some(index) = self.opened.lease().get(name) {
            return Ok(Some(index.clone()))
        }

        let raw_name = index_name(name);
        if self.inner.tree_names().into_iter().any(|tn| tn == raw_name) {
            let tree = self.inner.open_tree(raw_name)?;
            let index = Index::from_raw(tree)?;

            self.opened.rcu(|opened| {
                let mut opened = HashMap::clone(opened);
                opened.insert(name.to_string(), index.clone());
                opened
            });

            return Ok(Some(index))
        }

        Ok(None)
    }

    pub fn create_index(&self, name: String, schema: Schema) -> Result<Index, Error> {
        match self.open_index(&name)? {
            Some(index) => {
                if index.schema != schema {
                    return Err(Error::SchemaDiffer);
                }

                Ok(index)
            },
            None => {
                let raw_name = index_name(&name);
                let tree = self.inner.open_tree(raw_name)?;
                let index = Index::new_from_raw(tree, schema)?;

                self.opened.rcu(|opened| {
                    let mut opened = HashMap::clone(opened);
                    opened.insert(name.clone(), index.clone());
                    opened
                });

                Ok(index)
            },
        }
    }
}

#[derive(Clone)]
pub struct Index {
    schema: Schema,
    word_index: Arc<ArcSwap<WordIndex>>,
    inner: Arc<sled::Tree>,
}

impl Index {
    fn from_raw(inner: Arc<sled::Tree>) -> Result<Index, Error> {
        let bytes = inner.get("schema")?;
        let bytes = bytes.ok_or(Error::SchemaMissing)?;
        let schema = Schema::read_from_bin(bytes.as_ref())?;

        let bytes = inner.get("word-index")?;
        let bytes = bytes.ok_or(Error::WordIndexMissing)?;
        let word_index = {
            let len = bytes.len();
            let bytes = ivec_into_arc(bytes);
            let mut cursor = SharedDataCursor::from_shared_bytes(bytes, 0, len);

            // TODO must handle this error
            let word_index = WordIndex::from_shared_data_cursor(&mut cursor).unwrap();

            Arc::new(ArcSwap::new(Arc::new(word_index)))
        };

        Ok(Index { schema, word_index, inner })
    }

    fn new_from_raw(inner: Arc<sled::Tree>, schema: Schema) -> Result<Index, Error> {
        let mut schema_bytes = Vec::new();
        schema.write_to_bin(&mut schema_bytes)?;
        inner.set("schema", schema_bytes)?;

        let word_index = WordIndex::default();
        inner.set("word-index", word_index.into_bytes())?;
        let word_index = Arc::new(ArcSwap::new(Arc::new(word_index)));

        Ok(Index { schema, word_index, inner })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn word_index(&self) -> Lease<Arc<WordIndex>> {
        self.word_index.lease()
    }

    fn update_word_index(&self, word_index: Arc<WordIndex>) {
        self.word_index.store(word_index)
    }

    pub fn set_document_attribute<V>(
        &self,
        id: DocumentId,
        attr: SchemaAttr,
        value: V,
    ) -> Result<Option<IVec>, Error>
    where IVec: From<V>,
    {
        let key = document_key(id, attr);
        Ok(self.inner.set(key, value)?)
    }

    pub fn get_document_attribute(
        &self,
        id: DocumentId,
        attr: SchemaAttr
    ) -> Result<Option<IVec>, Error>
    {
        let key = document_key(id, attr);
        Ok(self.inner.get(key)?)
    }

    pub fn del_document_attribute(
        &self,
        id: DocumentId,
        attr: SchemaAttr
    ) -> Result<Option<IVec>, Error>
    {
        let key = document_key(id, attr);
        Ok(self.inner.del(key)?)
    }
}

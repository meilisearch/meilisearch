use std::sync::Arc;
use std::path::Path;

use meilidb_core::Index as WordIndex;
use meilidb_core::shared_data_cursor::{FromSharedDataCursor, SharedDataCursor};
use meilidb_core::write_to_bytes::WriteToBytes;
use sled::IVec;

use crate::Schema;

#[derive(Debug)]
pub enum Error {
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

fn ivec_into_arc(ivec: IVec) -> Arc<[u8]> {
    match ivec {
        IVec::Inline(len, bytes) => Arc::from(&bytes[..len as usize]),
        IVec::Remote { buf } => buf,
    }
}

#[derive(Clone)]
pub struct Database(sled::Db);

impl Database {
    pub fn start_default<P: AsRef<Path>>(path: P) -> Result<Database, Error> {
        sled::Db::start_default(path).map(Database).map_err(Into::into)
    }

    pub fn open_index(&self, name: &str) -> Result<Option<Index>, Error> {
        let name = index_name(name);

        if self.0.tree_names().into_iter().any(|tn| tn == name) {
            let tree = self.0.open_tree(name)?;
            let index = Index::from_raw(tree)?;
            return Ok(Some(index))
        }

        Ok(None)
    }

    pub fn create_index(&self, name: &str, schema: Schema) -> Result<Index, Error> {
        match self.open_index(name)? {
            Some(index) => {
                // TODO check if the schema is the same
                Ok(index)
            },
            None => {
                let name = index_name(name);
                let tree = self.0.open_tree(name)?;
                let index = Index::new_from_raw(tree, schema)?;
                Ok(index)
            },
        }
    }
}

#[derive(Clone)]
pub struct Index {
    schema: Schema,
    word_index: Arc<WordIndex>,
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

            Arc::new(word_index)
        };

        Ok(Index { schema, word_index, inner })
    }

    fn new_from_raw(inner: Arc<sled::Tree>, schema: Schema) -> Result<Index, Error> {
        let mut schema_bytes = Vec::new();
        schema.write_to_bin(&mut schema_bytes)?;
        inner.set("schema", schema_bytes)?;

        let word_index = WordIndex::default();
        inner.set("word-index", word_index.into_bytes())?;
        let word_index = Arc::new(word_index);

        Ok(Index { schema, word_index, inner })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn word_index(&self) -> &WordIndex {
        &self.word_index
    }
}

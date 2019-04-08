use std::sync::Arc;
use std::path::Path;

use crate::schema::Schema;

#[derive(Debug)]
pub enum Error {
    SchemaNotFound,
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

#[derive(Debug, Clone)]
pub struct Index {
    schema: Schema,
    inner: Arc<sled::Tree>,
}

impl Index {
    fn from_raw(inner: Arc<sled::Tree>) -> Result<Index, Error> {
        let bytes = inner.get("schema")?;
        let bytes = bytes.ok_or(Error::SchemaNotFound)?;

        let schema = Schema::read_from_bin(bytes.as_ref())?;
        Ok(Index { schema, inner })
    }

    fn new_from_raw(inner: Arc<sled::Tree>, schema: Schema) -> Result<Index, Error> {
        let mut schema_bytes = Vec::new();
        schema.write_to_bin(&mut schema_bytes);
        inner.set("schema", schema_bytes)?;
        Ok(Index { schema, inner })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

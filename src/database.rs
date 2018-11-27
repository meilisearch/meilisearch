use std::error::Error;
use std::ops::Deref;

use ::rocksdb::rocksdb::{DB, Snapshot};

use crate::index::schema::Schema;
use crate::blob::PositiveBlob;
use crate::DocumentId;

pub trait Retrieve {
    fn schema(&self) -> Result<Option<Schema>, Box<Error>>;
    fn data_index(&self) -> Result<PositiveBlob, Box<Error>>;
    fn get_documents<D>(&self, ids: &[DocumentId]) -> Result<Vec<D>, Box<Error>>;
}

impl<T> Retrieve for Snapshot<T>
where T: Deref<Target=DB>,
{
    fn schema(&self) -> Result<Option<Schema>, Box<Error>> {
        match self.deref().get(b"data-schema")? {
            Some(value) => Ok(Some(Schema::read_from(&*value)?)),
            None => Ok(None),
        }
    }

    fn data_index(&self) -> Result<PositiveBlob, Box<Error>> {
        match self.deref().get(b"data-index")? {
            Some(value) => Ok(bincode::deserialize(&value)?),
            None => Ok(PositiveBlob::default()),
        }
    }

    fn get_documents<D>(&self, ids: &[DocumentId]) -> Result<Vec<D>, Box<Error>> {
        if ids.is_empty() { return Ok(Vec::new()) }
        let schema = match self.schema()? {
            Some(schema) => schema,
            None => return Err(String::from("BUG: could not find schema").into()),
        };

        unimplemented!()
    }
}

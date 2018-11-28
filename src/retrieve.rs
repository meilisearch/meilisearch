use std::error::Error;
use std::ops::Deref;

use ::rocksdb::rocksdb::{DB, Snapshot, DBVector};

use crate::index::schema::{Schema, SchemaAttr};
use crate::blob::PositiveBlob;
use crate::DocumentId;

pub struct DocDatabase<'a, R: ?Sized> {
    retrieve: &'a R,
    schema: Schema,
}

impl<'a, R> DocDatabase<'a, R> {
    pub fn get_document<D>(&self, id: DocumentId) -> Result<Option<D>, Box<Error>> {
        // if ids.is_empty() { return Ok(Vec::new()) }
        unimplemented!()
    }

    pub fn get_document_attribute(&self, id: DocumentId, attr: SchemaAttr) -> Result<DBVector, Box<Error>> {
        unimplemented!()
    }
}

pub trait Retrieve {
    fn schema(&self) -> Result<Option<Schema>, Box<Error>>;
    fn data_index(&self) -> Result<PositiveBlob, Box<Error>>;
    fn doc_database(&self) -> Result<DocDatabase<Self>, Box<Error>>;
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

    fn doc_database(&self) -> Result<DocDatabase<Self>, Box<Error>> {
        let schema = match self.schema()? {
            Some(schema) => schema,
            None => return Err(String::from("BUG: could not find schema").into()),
        };

        Ok(DocDatabase {
            retrieve: self,
            schema: schema,
        })
    }
}

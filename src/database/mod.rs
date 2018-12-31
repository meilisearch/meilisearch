use std::error::Error;
use std::ops::Deref;
use std::sync::Arc;

use rocksdb::rocksdb::{DB, Snapshot};

pub use self::document_key::{DocumentKey, DocumentKeyAttr};
pub use self::database_view::{DatabaseView, DocumentIter};
pub use self::update::{Update, UpdateBuilder};
pub use self::serde::SerializerError;
pub use self::database::Database;
pub use self::schema::Schema;
pub use self::index::Index;

const DATA_INDEX:  &[u8] = b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

pub mod schema;
pub(crate) mod index;
mod update;
mod serde;
mod database;
mod document_key;
mod database_view;
mod deserializer;

fn retrieve_data_schema<D>(snapshot: &Snapshot<D>) -> Result<Schema, Box<Error>>
where D: Deref<Target=DB>
{
    match snapshot.get(DATA_SCHEMA)? {
        Some(vector) => Ok(Schema::read_from_bin(&*vector)?),
        None => Err(String::from("BUG: no schema found in the database").into()),
    }
}

fn retrieve_data_index<D>(snapshot: &Snapshot<D>) -> Result<Index, Box<Error>>
where D: Deref<Target=DB>
{
    match snapshot.get(DATA_INDEX)? {
        Some(vector) => {
            let bytes_len = vector.as_ref().len();
            let bytes = Arc::new(vector.as_ref().to_vec());
            Ok(Index::from_shared_bytes(bytes, 0, bytes_len)?)
        },
        None => Ok(Index::default()),
    }
}

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::error::Error;
use std::ops::Deref;

use rocksdb::rocksdb::{DB, Snapshot};

pub use self::update::{
    Update, PositiveUpdateBuilder, NewState,
    SerializerError, NegativeUpdateBuilder
};
pub use self::document_key::{DocumentKey, DocumentKeyAttr};
pub use self::database_view::{DatabaseView, DocumentIter};
pub use self::database::Database;
pub use self::schema::Schema;
use self::blob::positive::PositiveBlob;

const DATA_INDEX:  &[u8] = b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

macro_rules! forward_to_unserializable_type {
    ($($ty:ident => $se_method:ident,)*) => {
        $(
            fn $se_method(self, _v: $ty) -> Result<Self::Ok, Self::Error> {
                Err(SerializerError::UnserializableType { name: "$ty" })
            }
        )*
    }
}

pub mod blob;
pub mod schema;
mod update;
mod database;
mod document_key;
mod database_view;
mod deserializer;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn retrieve_data_schema<D>(snapshot: &Snapshot<D>) -> Result<Schema, Box<Error>>
where D: Deref<Target=DB>
{
    match snapshot.get(DATA_SCHEMA)? {
        Some(vector) => Ok(Schema::read_from(&*vector)?),
        None => Err(String::from("BUG: no schema found in the database").into()),
    }
}

fn retrieve_data_index<D>(snapshot: &Snapshot<D>) -> Result<PositiveBlob, Box<Error>>
where D: Deref<Target=DB>
{
    match snapshot.get(DATA_INDEX)? {
        Some(vector) => Ok(bincode::deserialize(&*vector)?),
        None => Ok(PositiveBlob::default()),
    }
}

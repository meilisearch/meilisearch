use std::{fmt, marker};
use std::error::Error;
use std::path::Path;

use rocksdb::rocksdb::{DB, Snapshot};
use rocksdb::rocksdb_options::ReadOptions;
use serde::de::{DeserializeOwned, Visitor};
use serde::de::value::MapDeserializer;
use serde::forward_to_deserialize_any;

use crate::index::schema::Schema;
use crate::blob::positive::PositiveBlob;
use crate::index::update::Update;
use crate::database::document_key::{DocumentKey, DocumentKeyAttr};
use crate::DocumentId;

pub mod document_key;

const DATA_INDEX:  &[u8] = b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

// FIXME Do not panic!
fn retrieve_data_schema(snapshot: &Snapshot<&DB>) -> Result<Schema, Box<Error>> {
    match snapshot.get(DATA_SCHEMA)? {
        Some(vector) => Ok(Schema::read_from(&*vector)?),
        None => panic!("BUG: no schema found in the database"),
    }
}

fn retrieve_data_index(snapshot: &Snapshot<&DB>) -> Result<PositiveBlob, Box<Error>> {
    match snapshot.get(DATA_INDEX)? {
        Some(vector) => Ok(bincode::deserialize(&*vector)?),
        None => Ok(PositiveBlob::default()),
    }
}

pub struct Database(DB);

impl Database {
    pub fn create(path: &Path) -> Result<Database, ()> {
        unimplemented!()
    }

    pub fn open(path: &Path) -> Result<Database, ()> {
        unimplemented!()
    }

    pub fn ingest_update_file(&self, update: Update) -> Result<(), ()> {
        unimplemented!()
    }

    pub fn view(&self) -> Result<DatabaseView, Box<Error>> {
        let snapshot = self.0.snapshot();
        DatabaseView::new(snapshot)
    }
}

pub struct DatabaseView<'a> {
    snapshot: Snapshot<&'a DB>,
    schema: Schema,
}

impl<'a> DatabaseView<'a> {
    pub fn new(snapshot: Snapshot<&'a DB>) -> Result<DatabaseView, Box<Error>> {
        let schema = retrieve_data_schema(&snapshot)?;
        Ok(DatabaseView { snapshot, schema })
    }

    pub fn into_snapshot(self) -> Snapshot<&'a DB> {
        self.snapshot
    }

    // TODO create an enum error type
    pub fn retrieve_document<D>(&self, id: DocumentId) -> Result<D, Box<Error>>
    where D: DeserializeOwned
    {
        let mut deserializer = Deserializer::new(&self.snapshot, &self.schema, id);
        Ok(D::deserialize(&mut deserializer)?)
    }

    pub fn retrieve_documents<D, I>(&self, ids: I) -> DocumentIter<D, I::IntoIter>
    where D: DeserializeOwned,
          I: IntoIterator<Item=DocumentId>,
    {
        DocumentIter {
            database_view: self,
            document_ids: ids.into_iter(),
            _phantom: marker::PhantomData,
        }
    }
}

// TODO impl ExactSizeIterator, DoubleEndedIterator
pub struct DocumentIter<'a, D, I> {
    database_view: &'a DatabaseView<'a>,
    document_ids: I,
    _phantom: marker::PhantomData<D>,
}

impl<'a, D, I> Iterator for DocumentIter<'a, D, I>
where D: DeserializeOwned,
      I: Iterator<Item=DocumentId>,
{
    type Item = Result<D, Box<Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.document_ids.next() {
            Some(id) => Some(self.database_view.retrieve_document(id)),
            None => None
        }
    }
}

struct Deserializer<'a> {
    snapshot: &'a Snapshot<&'a DB>,
    schema: &'a Schema,
    document_id: DocumentId,
}

impl<'a> Deserializer<'a> {
    fn new(snapshot: &'a Snapshot<&DB>, schema: &'a Schema, doc: DocumentId) -> Self {
        Deserializer { snapshot, schema, document_id: doc }
    }
}

impl<'de, 'a, 'b> serde::de::Deserializer<'de> for &'b mut Deserializer<'a> {
    type Error = DeserializerError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        self.deserialize_map(visitor)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit seq
        bytes byte_buf unit_struct tuple_struct
        identifier tuple ignored_any option newtype_struct enum
        struct
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        let mut options = ReadOptions::new();
        let lower = DocumentKey::new(self.document_id);
        let upper = DocumentKey::new(self.document_id + 1);
        options.set_iterate_lower_bound(lower.as_ref());
        options.set_iterate_upper_bound(upper.as_ref());

        let mut db_iter = self.snapshot.iter_opt(options);
        let iter = db_iter.map(|(key, value)| {
            // retrieve the schema attribute name
            // from the schema attribute number
            let document_key_attr = DocumentKeyAttr::from_bytes(&key);
            let schema_attr = document_key_attr.attribute();
            let attribute_name = self.schema.attribute_name(schema_attr);
            (attribute_name, value)
        });

        let map_deserializer = MapDeserializer::new(iter);
        visitor.visit_map(map_deserializer)
    }
}

#[derive(Debug)]
enum DeserializerError {
    Custom(String),
}

impl serde::de::Error for DeserializerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        DeserializerError::Custom(msg.to_string())
    }
}

impl fmt::Display for DeserializerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeserializerError::Custom(s) => f.write_str(&s),
        }
    }
}

impl Error for DeserializerError {}

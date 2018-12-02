use std::io::{Cursor, Write};
use std::{fmt, marker};
use std::error::Error;
use std::path::Path;

use rocksdb::rocksdb::{DB, Snapshot, DBVector};
use rocksdb::rocksdb_options::ReadOptions;
use byteorder::{NetworkEndian, WriteBytesExt};
use serde::de::{DeserializeOwned, Visitor};
use serde::de::value::MapDeserializer;

use crate::index::schema::{Schema, SchemaAttr};
use crate::blob::positive::PositiveBlob;
use crate::index::update::Update;
use crate::DocumentId;

const DATA_INDEX:  &[u8] = b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

const DOC_KEY_LEN:      usize = 4 + std::mem::size_of::<u64>();
const DOC_KEY_ATTR_LEN: usize = DOC_KEY_LEN + 1 + std::mem::size_of::<u32>();

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

fn retrieve_document_attribute(
    snapshot: &Snapshot<&DB>,
    id: DocumentId,
    attr: SchemaAttr
) -> Result<Option<DBVector>, Box<Error>>
{
    let attribute_key = document_key_attr(id, attr);
    Ok(snapshot.get(&attribute_key)?)
}

fn document_key(id: DocumentId) -> [u8; DOC_KEY_LEN] {
    let mut key = [0; DOC_KEY_LEN];

    let mut wtr = Cursor::new(&mut key[..]);
    wtr.write_all(b"doc-").unwrap();
    wtr.write_u64::<NetworkEndian>(id).unwrap();

    key
}

fn document_key_attr(id: DocumentId, attr: SchemaAttr) -> [u8; DOC_KEY_ATTR_LEN] {
    let mut key = [0; DOC_KEY_ATTR_LEN];
    let raw_key = document_key(id);

    let mut wtr = Cursor::new(&mut key[..]);
    wtr.write_all(&raw_key).unwrap();
    wtr.write_all(b"-").unwrap();
    wtr.write_u32::<NetworkEndian>(attr.as_u32()).unwrap();

    key
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
        unimplemented!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_tuple<V>(
        self,
        len: usize,
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        let mut options = ReadOptions::new();
        options.set_iterate_lower_bound(&document_key(self.document_id));
        options.set_iterate_upper_bound(&document_key(self.document_id + 1));

        let mut db_iter = self.snapshot.iter_opt(options);
        let iter = db_iter.map(|(key, value)| ("hello", "ok"));

        // Create the DocumentKey and DocumentKeyAttr types
        // to help create and parse document keys attributes...
        unimplemented!();

        let map_deserializer = MapDeserializer::new(iter);
        visitor.visit_map(map_deserializer)
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V
    ) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de>
    {
        unimplemented!()
    }
}

#[derive(Debug)]
struct DeserializerError;

impl serde::de::Error for DeserializerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        unimplemented!()
    }
}

impl fmt::Display for DeserializerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!()
    }
}

impl Error for DeserializerError {}

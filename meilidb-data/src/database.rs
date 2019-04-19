use std::collections::HashSet;
use std::io::{self, Cursor, BufRead};
use std::iter::FromIterator;
use std::path::Path;
use std::sync::Arc;
use std::{error, fmt};

use arc_swap::{ArcSwap, Lease};
use byteorder::{ReadBytesExt, BigEndian};
use hashbrown::HashMap;
use meilidb_core::criterion::Criteria;
use meilidb_core::QueryBuilder;
use meilidb_core::shared_data_cursor::{FromSharedDataCursor, SharedDataCursor};
use meilidb_core::write_to_bytes::WriteToBytes;
use meilidb_core::DocumentId;
use rmp_serde::decode::{Error as RmpError};
use sdset::SetBuf;
use serde::de;
use sled::IVec;

use crate::{Schema, SchemaAttr, RankedMap};
use crate::serde::{extract_document_id, Serializer, Deserializer, SerializerError};
use crate::indexer::{Indexer, WordIndexTree};

pub type WordIndex = meilidb_core::Index<WordIndexTree>;

#[derive(Debug)]
pub enum Error {
    SchemaDiffer,
    SchemaMissing,
    WordIndexMissing,
    MissingDocumentId,
    SledError(sled::Error),
    BincodeError(bincode::Error),
    SerializerError(SerializerError),
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

impl From<SerializerError> for Error {
    fn from(error: SerializerError) -> Error {
        Error::SerializerError(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match self {
            SchemaDiffer => write!(f, "schemas differ"),
            SchemaMissing => write!(f, "this index does not have a schema"),
            WordIndexMissing => write!(f, "this index does not have a word index"),
            MissingDocumentId => write!(f, "document id is missing"),
            SledError(e) => write!(f, "sled error; {}", e),
            BincodeError(e) => write!(f, "bincode error; {}", e),
            SerializerError(e) => write!(f, "serializer error; {}", e),
        }
    }
}

impl error::Error for Error { }

fn index_name(name: &str) -> Vec<u8> {
    format!("index-{}", name).into_bytes()
}

fn word_index_name(name: &str) -> Vec<u8> {
    format!("word-index-{}", name).into_bytes()
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

trait CursorExt {
    fn consume_if_eq(&mut self, needle: &[u8]) -> bool;
}

impl<T: AsRef<[u8]>> CursorExt for Cursor<T> {
    fn consume_if_eq(&mut self, needle: &[u8]) -> bool {
        let position = self.position() as usize;
        let slice = self.get_ref().as_ref();

        if slice[position..].starts_with(needle) {
            self.consume(needle.len());
            true
        } else {
            false
        }
    }
}

fn extract_document_key(key: Vec<u8>) -> io::Result<(DocumentId, SchemaAttr)> {
    let mut key = Cursor::new(key);

    if !key.consume_if_eq(b"document-") {
        return Err(io::Error::from(io::ErrorKind::InvalidData))
    }

    let document_id = key.read_u64::<BigEndian>().map(DocumentId)?;
    let schema_attr = key.read_u16::<BigEndian>().map(SchemaAttr)?;

    Ok((document_id, schema_attr))
}

#[derive(Clone)]
pub struct Database {
    opened: Arc<ArcSwap<HashMap<String, RawIndex>>>,
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
        if let Some(raw_index) = self.opened.lease().get(name) {
            return Ok(Some(Index(raw_index.clone())))
        }

        let raw_name = index_name(name);
        if self.inner.tree_names().into_iter().any(|tn| tn == raw_name) {
            let tree = self.inner.open_tree(raw_name)?;
            let word_index_tree = self.inner.open_tree(word_index_name(name))?;
            let raw_index = RawIndex::from_raw(tree, word_index_tree)?;

            self.opened.rcu(|opened| {
                let mut opened = HashMap::clone(opened);
                opened.insert(name.to_string(), raw_index.clone());
                opened
            });

            return Ok(Some(Index(raw_index)))
        }

        Ok(None)
    }

    pub fn create_index(&self, name: String, schema: Schema) -> Result<Index, Error> {
        match self.open_index(&name)? {
            Some(index) => {
                if index.schema() != &schema {
                    return Err(Error::SchemaDiffer);
                }

                Ok(index)
            },
            None => {
                let raw_name = index_name(&name);
                let tree = self.inner.open_tree(raw_name)?;
                let word_index_tree = self.inner.open_tree(word_index_name(&name))?;
                let raw_index = RawIndex::new_from_raw(tree, word_index_tree, schema)?;

                self.opened.rcu(|opened| {
                    let mut opened = HashMap::clone(opened);
                    opened.insert(name.clone(), raw_index.clone());
                    opened
                });

                Ok(Index(raw_index))
            },
        }
    }
}

#[derive(Clone)]
pub struct RawIndex {
    schema: Schema,
    word_index: Arc<ArcSwap<WordIndex>>,
    ranked_map: Arc<ArcSwap<RankedMap>>,
    inner: Arc<sled::Tree>,
}

impl RawIndex {
    fn from_raw(inner: Arc<sled::Tree>, word_index: Arc<sled::Tree>) -> Result<RawIndex, Error> {
        let schema = {
            let bytes = inner.get("schema")?;
            let bytes = bytes.ok_or(Error::SchemaMissing)?;
            Schema::read_from_bin(bytes.as_ref())?
        };

        let store = WordIndexTree(word_index);
        let word_index = WordIndex::from_store(store)?;
        let word_index = Arc::new(ArcSwap::new(Arc::new(word_index)));

        let ranked_map = {
            let map = match inner.get("ranked-map")? {
                Some(bytes) => bincode::deserialize(bytes.as_ref())?,
                None => RankedMap::default(),
            };

            Arc::new(ArcSwap::new(Arc::new(map)))
        };

        Ok(RawIndex { schema, word_index, ranked_map, inner })
    }

    fn new_from_raw(
        inner: Arc<sled::Tree>,
        word_index: Arc<sled::Tree>,
        schema: Schema,
    ) -> Result<RawIndex, Error>
    {
        let mut schema_bytes = Vec::new();
        schema.write_to_bin(&mut schema_bytes)?;
        inner.set("schema", schema_bytes)?;

        let store = WordIndexTree(word_index);
        let word_index = WordIndex::from_store(store)?;
        let word_index = Arc::new(ArcSwap::new(Arc::new(word_index)));

        let ranked_map = Arc::new(ArcSwap::new(Arc::new(RankedMap::default())));

        Ok(RawIndex { schema, word_index, ranked_map, inner })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn word_index(&self) -> Lease<Arc<WordIndex>> {
        self.word_index.lease()
    }

    pub fn ranked_map(&self) -> Lease<Arc<RankedMap>> {
        self.ranked_map.lease()
    }

    pub fn update_word_index(&self, word_index: Arc<WordIndex>) {
        self.word_index.store(word_index)
    }

    pub fn update_ranked_map(&self, ranked_map: Arc<RankedMap>) -> sled::Result<()> {
        let data = bincode::serialize(ranked_map.as_ref()).unwrap();
        self.inner.set("ranked-map", data).map(drop)?;
        self.ranked_map.store(ranked_map);

        Ok(())
    }

    pub fn set_document_attribute<V>(
        &self,
        id: DocumentId,
        attr: SchemaAttr,
        value: V,
    ) -> Result<Option<IVec>, sled::Error>
    where IVec: From<V>,
    {
        let key = document_key(id, attr);
        Ok(self.inner.set(key, value)?)
    }

    pub fn get_document_attribute(
        &self,
        id: DocumentId,
        attr: SchemaAttr
    ) -> Result<Option<IVec>, sled::Error>
    {
        let key = document_key(id, attr);
        Ok(self.inner.get(key)?)
    }

    pub fn get_document_fields(&self, id: DocumentId) -> DocumentFieldsIter {
        let start = document_key(id, SchemaAttr::min());
        let end = document_key(id, SchemaAttr::max());
        DocumentFieldsIter(self.inner.range(start..=end))
    }

    pub fn del_document_attribute(
        &self,
        id: DocumentId,
        attr: SchemaAttr
    ) -> Result<Option<IVec>, sled::Error>
    {
        let key = document_key(id, attr);
        Ok(self.inner.del(key)?)
    }
}

pub struct DocumentFieldsIter<'a>(sled::Iter<'a>);

impl<'a> Iterator for DocumentFieldsIter<'a> {
    type Item = Result<(DocumentId, SchemaAttr, IVec), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Ok((key, value))) => {
                let (id, attr) = extract_document_key(key).unwrap();
                Some(Ok((id, attr, value)))
            },
            Some(Err(e)) => Some(Err(Error::SledError(e))),
            None => None,
        }
    }
}

#[derive(Clone)]
pub struct Index(RawIndex);

impl Index {
    pub fn query_builder(&self) -> QueryBuilder<Lease<Arc<WordIndex>>> {
        let word_index = self.word_index();
        QueryBuilder::new(word_index)
    }

    pub fn query_builder_with_criteria<'c>(
        &self,
        criteria: Criteria<'c>,
    ) -> QueryBuilder<'c, Lease<Arc<WordIndex>>>
    {
        let word_index = self.word_index();
        QueryBuilder::with_criteria(word_index, criteria)
    }

    pub fn schema(&self) -> &Schema {
        self.0.schema()
    }

    pub fn word_index(&self) -> Lease<Arc<WordIndex>> {
        self.0.word_index()
    }

    pub fn ranked_map(&self) -> Lease<Arc<RankedMap>> {
        self.0.ranked_map()
    }

    pub fn documents_addition(&self) -> DocumentsAddition {
        let index = self.0.clone();
        let ranked_map = self.0.ranked_map().clone();
        DocumentsAddition::from_raw(index, ranked_map)
    }

    pub fn documents_deletion(&self) -> DocumentsDeletion {
        let index = self.0.clone();
        DocumentsDeletion::from_raw(index)
    }

    pub fn document<T>(
        &self,
        fields: Option<&HashSet<&str>>,
        id: DocumentId,
    ) -> Result<Option<T>, RmpError>
    where T: de::DeserializeOwned,
    {
        let fields = match fields {
            Some(fields) => {
                let iter = fields.iter().filter_map(|n| self.0.schema().attribute(n));
                Some(HashSet::from_iter(iter))
            },
            None => None,
        };

        let mut deserializer = Deserializer {
            document_id: id,
            raw_index: &self.0,
            fields: fields.as_ref(),
        };

        // TODO: currently we return an error if all document fields are missing,
        //       returning None would have been better
        T::deserialize(&mut deserializer).map(Some)
    }
}

pub struct DocumentsAddition {
    inner: RawIndex,
    indexer: Indexer,
    ranked_map: RankedMap,
}

impl DocumentsAddition {
    pub fn from_raw(inner: RawIndex, ranked_map: RankedMap) -> DocumentsAddition {
        DocumentsAddition { inner, indexer: Indexer::new(), ranked_map }
    }

    pub fn update_document<D>(&mut self, document: D) -> Result<(), Error>
    where D: serde::Serialize,
    {
        let schema = self.inner.schema();
        let identifier = schema.identifier_name();

        let document_id = match extract_document_id(identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        let serializer = Serializer {
            schema,
            index: &self.inner,
            indexer: &mut self.indexer,
            ranked_map: &mut self.ranked_map,
            document_id,
        };

        document.serialize(serializer)?;

        Ok(())
    }

    pub fn finalize(self) -> sled::Result<()> {
        let delta_index = self.indexer.build();

        let index = self.inner.word_index();
        let new_index = index.insert_indexes(delta_index)?;

        let new_index = Arc::from(new_index);
        self.inner.update_word_index(new_index);

        Ok(())
    }
}

pub struct DocumentsDeletion {
    inner: RawIndex,
    documents: Vec<DocumentId>,
}

impl DocumentsDeletion {
    pub fn from_raw(inner: RawIndex) -> DocumentsDeletion {
        DocumentsDeletion {
            inner,
            documents: Vec::new(),
        }
    }

    pub fn delete_document(&mut self, id: DocumentId) {
        self.documents.push(id);
    }

    pub fn finalize(mut self) -> Result<(), Error> {
        self.documents.sort_unstable();
        self.documents.dedup();

        let idset = SetBuf::new_unchecked(self.documents);
        let index = self.inner.word_index();

        let new_index = index.remove_documents(&idset)?;
        let new_index = Arc::from(new_index);

        self.inner.update_word_index(new_index);

        Ok(())
    }
}

use std::collections::HashSet;
use std::convert::TryInto;
use std::io::{self, Cursor, BufRead};
use std::iter::FromIterator;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::{error, fmt};

use arc_swap::{ArcSwap, Lease};
use byteorder::{ReadBytesExt, BigEndian};
use hashbrown::HashMap;
use meilidb_core::{criterion::Criteria, QueryBuilder, DocumentId, DocIndex};
use rmp_serde::decode::{Error as RmpError};
use sdset::SetBuf;
use serde::de;
use sled::IVec;
use zerocopy::{AsBytes, LayoutVerified};

use crate::{Schema, SchemaAttr, RankedMap};
use crate::serde::{extract_document_id, Serializer, Deserializer, SerializerError};
use crate::indexer::{Indexer, WordIndexTree};
use crate::document_attr_key::DocumentAttrKey;

pub type WordIndex = meilidb_core::Index<WordIndexTree>;

#[derive(Debug)]
pub enum Error {
    SchemaDiffer,
    SchemaMissing,
    WordIndexMissing,
    MissingDocumentId,
    SledError(sled::Error),
    FstError(fst::Error),
    BincodeError(bincode::Error),
    SerializerError(SerializerError),
}

impl From<sled::Error> for Error {
    fn from(error: sled::Error) -> Error {
        Error::SledError(error)
    }
}

impl From<fst::Error> for Error {
    fn from(error: fst::Error) -> Error {
        Error::FstError(error)
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
            FstError(e) => write!(f, "fst error; {}", e),
            BincodeError(e) => write!(f, "bincode error; {}", e),
            SerializerError(e) => write!(f, "serializer error; {}", e),
        }
    }
}

impl error::Error for Error { }

pub struct Database {
    cache: RwLock<HashMap<String, Arc<Index>>>,
    inner: sled::Db,
}

impl Database {
    pub fn start_default<P: AsRef<Path>>(path: P) -> Result<Database, Error> {
        let cache = RwLock::new(HashMap::new());
        let inner = sled::Db::start_default(path)?;
        Ok(Database { cache, inner })
    }

    pub fn open_index(&self, name: &str) -> Result<Option<Arc<Index>>, Error> {
        {
            let cache = self.cache.read().unwrap();
            if let Some(index) = cache.get(name).cloned() {
                return Ok(Some(index))
            }
        }

        let indexes: HashSet<&str> = match self.inner.get("indexes")? {
            Some(bytes) => bincode::deserialize(&bytes)?,
            None => return Ok(None),
        };

        if indexes.get(name).is_none() {
            return Ok(None);
        }

        let main = {
            let tree = self.inner.open_tree(name)?;
            MainIndex(tree)
        };

        let words = {
            let tree_name = format!("{}-words", name);
            let tree = self.inner.open_tree(tree_name)?;
            WordsIndex(tree)
        };

        let documents = {
            let tree_name = format!("{}-documents", name);
            let tree = self.inner.open_tree(tree_name)?;
            DocumentsIndex(tree)
        };

        let raw_index = RawIndex { main, words, documents };
        let index = Arc::new(Index(raw_index));

        {
            let cache = self.cache.write().unwrap();
            cache.insert(name.to_string(), index.clone());
        }

        Ok(Some(index))
    }

    pub fn create_index(&self, name: &str, schema: Schema) -> Result<Arc<Index>, Error> {
        {
            let cache = self.cache.read().unwrap();
            if let Some(index) = cache.get(name).cloned() {
                // TODO check if schemas are the same
                return Ok(index)
            }
        }

        let mut indexes: HashSet<&str> = match self.inner.get("indexes")? {
            Some(bytes) => bincode::deserialize(&bytes)?,
            None => HashSet::new(),
        };

        let new_insertion = indexes.insert(name);

        let main = {
            let tree = self.inner.open_tree(name)?;
            MainIndex(tree)
        };

        if let Some(prev_schema) = main.schema()? {
            if prev_schema != schema {
                return Err(Error::SchemaDiffer)
            }
        }

        let words = {
            let tree_name = format!("{}-words", name);
            let tree = self.inner.open_tree(tree_name)?;
            WordsIndex(tree)
        };

        let documents = {
            let tree_name = format!("{}-documents", name);
            let tree = self.inner.open_tree(tree_name)?;
            DocumentsIndex(tree)
        };

        let raw_index = RawIndex { main, words, documents };
        let index = Arc::new(Index(raw_index));

        {
            let cache = self.cache.write().unwrap();
            cache.insert(name.to_string(), index.clone());
        }

        Ok(index)
    }
}

#[derive(Clone)]
struct RawIndex {
    main: MainIndex,
    words: WordsIndex,
    documents: DocumentsIndex,
}

#[derive(Clone)]
struct MainIndex(Arc<sled::Tree>);

impl MainIndex {
    fn schema(&self) -> Result<Option<Schema>, Error> {
        match self.0.get("schema")? {
            Some(bytes) => {
                let schema = Schema::read_from_bin(bytes.as_ref())?;
                Ok(Some(schema))
            },
            None => Ok(None),
        }
    }

    fn words_set(&self) -> Result<Option<fst::Set>, Error> {
        match self.0.get("words")? {
            Some(bytes) => {
                let len = bytes.len();
                let value = bytes.into();
                let fst = fst::raw::Fst::from_shared_bytes(value, 0, len)?;
                Ok(Some(fst::Set::from(fst)))
            },
            None => Ok(None),
        }
    }

    fn ranked_map(&self) -> Result<Option<RankedMap>, Error> {
        match self.0.get("ranked-map")? {
            Some(bytes) => {
                let ranked_map = RankedMap::read_from_bin(bytes.as_ref())?;
                Ok(Some(ranked_map))
            },
            None => Ok(None),
        }
    }
}

#[derive(Clone)]
struct WordsIndex(Arc<sled::Tree>);

impl WordsIndex {
    fn doc_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Error> {
        match self.0.get(word)? {
            Some(bytes) => {
                let layout = LayoutVerified::new_slice(bytes.as_ref()).expect("invalid layout");
                let slice = layout.into_slice();
                let setbuf = SetBuf::new_unchecked(slice.to_vec());
                Ok(Some(setbuf))
            },
            None => Ok(None),
        }
    }
}

#[derive(Clone)]
struct DocumentsIndex(Arc<sled::Tree>);

impl DocumentsIndex {
    fn document_field(&self, id: DocumentId, attr: SchemaAttr) -> Result<Option<IVec>, Error> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.get(key).map_err(Into::into)
    }

    fn document_fields(&self, id: DocumentId) -> DocumentFieldsIter {
        let start = DocumentAttrKey::new(id, SchemaAttr::min());
        let start = start.to_be_bytes();

        let end = DocumentAttrKey::new(id, SchemaAttr::max());
        let end = end.to_be_bytes();

        DocumentFieldsIter(self.0.range(start..=end))
    }
}

pub struct DocumentFieldsIter<'a>(sled::Iter<'a>);

impl<'a> Iterator for DocumentFieldsIter<'a> {
    type Item = Result<(SchemaAttr, IVec), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Ok((key, value))) => {
                let slice: &[u8] = key.as_ref();
                let array = slice.try_into().unwrap();
                let key = DocumentAttrKey::from_be_bytes(array);
                Some(Ok((key.attribute, value)))
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

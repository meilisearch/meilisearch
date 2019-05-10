use std::collections::{BTreeSet, HashSet, HashMap};
use std::collections::hash_map::Entry;
use std::convert::TryInto;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::{error, fmt};

use arc_swap::{ArcSwap, Lease};
use meilidb_core::{criterion::Criteria, QueryBuilder, Store, DocumentId, DocIndex};
use rmp_serde::decode::{Error as RmpError};
use sdset::{Set, SetBuf, SetOperation, duo::{Union, DifferenceByKey}};
use serde::de;
use sled::IVec;
use zerocopy::{AsBytes, LayoutVerified};
use fst::{SetBuilder, set::OpBuilder, Streamer};

use crate::{Schema, SchemaAttr, RankedMap};
use crate::serde::{extract_document_id, Serializer, Deserializer, SerializerError};
use crate::indexer::{Indexer, Indexed};
use crate::document_attr_key::DocumentAttrKey;

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

    pub fn indexes(&self) -> Result<Option<HashSet<String>>, Error> {
        let bytes = match self.inner.get("indexes")? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let indexes = bincode::deserialize(&bytes)?;
        Ok(Some(indexes))
    }

    pub fn set_indexes(&self, value: &HashSet<String>) -> Result<(), Error> {
        let bytes = bincode::serialize(value)?;
        self.inner.set("indexes", bytes)?;
        Ok(())
    }

    pub fn open_index(&self, name: &str) -> Result<Option<Arc<Index>>, Error> {
        {
            let cache = self.cache.read().unwrap();
            if let Some(index) = cache.get(name).cloned() {
                return Ok(Some(index))
            }
        }

        let mut cache = self.cache.write().unwrap();
        let index = match cache.entry(name.to_string()) {
            Entry::Occupied(occupied) => {
                occupied.get().clone()
            },
            Entry::Vacant(vacant) => {
                if !self.indexes()?.map_or(false, |x| !x.contains(name)) {
                    return Ok(None)
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

                let attrs_words = {
                    let tree_name = format!("{}-attrs-words", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    AttrsWords(tree)
                };

                let documents = {
                    let tree_name = format!("{}-documents", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    DocumentsIndex(tree)
                };

                let raw_index = RawIndex { main, words, attrs_words, documents };
                let index = Index::from_raw(raw_index)?;

                vacant.insert(Arc::new(index)).clone()
            },
        };

        Ok(Some(index))
    }

    pub fn create_index(&self, name: &str, schema: Schema) -> Result<Arc<Index>, Error> {
        let mut cache = self.cache.write().unwrap();

        let index = match cache.entry(name.to_string()) {
            Entry::Occupied(occupied) => {
                occupied.get().clone()
            },
            Entry::Vacant(vacant) => {
                let main = {
                    let tree = self.inner.open_tree(name)?;
                    MainIndex(tree)
                };

                if let Some(prev_schema) = main.schema()? {
                    if prev_schema != schema {
                        return Err(Error::SchemaDiffer)
                    }
                }

                main.set_schema(&schema)?;

                let words = {
                    let tree_name = format!("{}-words", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    WordsIndex(tree)
                };

                let attrs_words = {
                    let tree_name = format!("{}-attrs-words", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    AttrsWords(tree)
                };

                let documents = {
                    let tree_name = format!("{}-documents", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    DocumentsIndex(tree)
                };

                let mut indexes = self.indexes()?.unwrap_or_else(HashSet::new);
                indexes.insert(name.to_string());
                self.set_indexes(&indexes)?;

                let raw_index = RawIndex { main, words, attrs_words, documents };
                let index = Index::from_raw(raw_index)?;

                vacant.insert(Arc::new(index)).clone()
            },
        };

        Ok(index)
    }
}

#[derive(Clone)]
pub struct RawIndex {
    pub main: MainIndex,
    pub words: WordsIndex,
    pub attrs_words: AttrsWords,
    pub documents: DocumentsIndex,
}

#[derive(Clone)]
pub struct MainIndex(Arc<sled::Tree>);

impl MainIndex {
    pub fn schema(&self) -> Result<Option<Schema>, Error> {
        match self.0.get("schema")? {
            Some(bytes) => {
                let schema = Schema::read_from_bin(bytes.as_ref())?;
                Ok(Some(schema))
            },
            None => Ok(None),
        }
    }

    pub fn set_schema(&self, schema: &Schema) -> Result<(), Error> {
        let mut bytes = Vec::new();
        schema.write_to_bin(&mut bytes)?;
        self.0.set("schema", bytes)?;
        Ok(())
    }

    pub fn words_set(&self) -> Result<Option<fst::Set>, Error> {
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

    pub fn set_words_set(&self, value: &fst::Set) -> Result<(), Error> {
        self.0.set("words", value.as_fst().as_bytes())?;
        Ok(())
    }

    pub fn ranked_map(&self) -> Result<Option<RankedMap>, Error> {
        match self.0.get("ranked-map")? {
            Some(bytes) => {
                let ranked_map = RankedMap::read_from_bin(bytes.as_ref())?;
                Ok(Some(ranked_map))
            },
            None => Ok(None),
        }
    }

    pub fn set_ranked_map(&self, value: &RankedMap) -> Result<(), Error> {
        let mut bytes = Vec::new();
        value.write_to_bin(&mut bytes)?;
        self.0.set("ranked_map", bytes)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct WordsIndex(Arc<sled::Tree>);

impl WordsIndex {
    pub fn doc_indexes(&self, word: &[u8]) -> sled::Result<Option<SetBuf<DocIndex>>> {
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

    pub fn set_doc_indexes(&self, word: &[u8], set: &Set<DocIndex>) -> sled::Result<()> {
        self.0.set(word, set.as_bytes())?;
        Ok(())
    }

    pub fn del_doc_indexes(&self, word: &[u8]) -> sled::Result<()> {
        self.0.del(word)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AttrsWords(Arc<sled::Tree>);

impl AttrsWords {
    pub fn attr_words(&self, id: DocumentId, attr: SchemaAttr) -> Result<Option<fst::Set>, Error> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        match self.0.get(key)? {
            Some(bytes) => {
                let len = bytes.len();
                let value = bytes.into();
                let fst = fst::raw::Fst::from_shared_bytes(value, 0, len)?;
                Ok(Some(fst::Set::from(fst)))
            },
            None => Ok(None)
        }
    }

    pub fn attrs_words(&self, id: DocumentId) -> DocumentAttrsWordsIter {
        let start = DocumentAttrKey::new(id, SchemaAttr::min());
        let start = start.to_be_bytes();

        let end = DocumentAttrKey::new(id, SchemaAttr::max());
        let end = end.to_be_bytes();

        DocumentAttrsWordsIter(self.0.range(start..=end))
    }

    pub fn set_attr_words(&self, id: DocumentId, attr: SchemaAttr, words: &fst::Set) -> Result<(), Error> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.set(key, words.as_fst().as_bytes())?;
        Ok(())
    }

    pub fn del_attr_words(&self, id: DocumentId, attr: SchemaAttr) -> Result<(), Error> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.del(key)?;
        Ok(())
    }
}

pub struct DocumentAttrsWordsIter<'a>(sled::Iter<'a>);

impl<'a> Iterator for DocumentAttrsWordsIter<'a> {
    type Item = sled::Result<(SchemaAttr, fst::Set)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Ok((key, bytes))) => {
                let slice: &[u8] = key.as_ref();
                let array = slice.try_into().unwrap();
                let key = DocumentAttrKey::from_be_bytes(array);

                let len = bytes.len();
                let value = bytes.into();
                let fst = fst::raw::Fst::from_shared_bytes(value, 0, len).unwrap();
                let set = fst::Set::from(fst);

                Some(Ok((key.attribute, set)))
            },
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }
}

#[derive(Clone)]
pub struct DocumentsIndex(Arc<sled::Tree>);

impl DocumentsIndex {
    pub fn document_field(&self, id: DocumentId, attr: SchemaAttr) -> sled::Result<Option<IVec>> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.get(key)
    }

    pub fn set_document_field(&self, id: DocumentId, attr: SchemaAttr, value: Vec<u8>) -> sled::Result<()> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.set(key, value)?;
        Ok(())
    }

    pub fn del_document_field(&self, id: DocumentId, attr: SchemaAttr) -> sled::Result<()> {
        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        self.0.del(key)?;
        Ok(())
    }

    pub fn document_fields(&self, id: DocumentId) -> DocumentFieldsIter {
        let start = DocumentAttrKey::new(id, SchemaAttr::min());
        let start = start.to_be_bytes();

        let end = DocumentAttrKey::new(id, SchemaAttr::max());
        let end = end.to_be_bytes();

        DocumentFieldsIter(self.0.range(start..=end))
    }
}

pub struct DocumentFieldsIter<'a>(sled::Iter<'a>);

impl<'a> Iterator for DocumentFieldsIter<'a> {
    type Item = sled::Result<(SchemaAttr, IVec)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Ok((key, value))) => {
                let slice: &[u8] = key.as_ref();
                let array = slice.try_into().unwrap();
                let key = DocumentAttrKey::from_be_bytes(array);
                Some(Ok((key.attribute, value)))
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

#[derive(Clone)]
pub struct Index(ArcSwap<InnerIndex>);

pub struct InnerIndex {
    pub words: fst::Set,
    pub schema: Schema,
    pub ranked_map: RankedMap,
    pub raw: RawIndex, // TODO this will be a snapshot in the future
}

impl Index {
    fn from_raw(raw: RawIndex) -> Result<Index, Error> {
        let words = match raw.main.words_set()? {
            Some(words) => words,
            None => fst::Set::default(),
        };

        let schema = match raw.main.schema()? {
            Some(schema) => schema,
            None => return Err(Error::SchemaMissing),
        };

        let ranked_map = match raw.main.ranked_map()? {
            Some(map) => map,
            None => RankedMap::default(),
        };

        let inner = InnerIndex { words, schema, ranked_map, raw };
        let index = Index(ArcSwap::new(Arc::new(inner)));

        Ok(index)
    }

    pub fn query_builder(&self) -> QueryBuilder<IndexLease> {
        let lease = IndexLease(self.0.lease());
        QueryBuilder::new(lease)
    }

    pub fn query_builder_with_criteria<'c>(
        &self,
        criteria: Criteria<'c>,
    ) -> QueryBuilder<'c, IndexLease>
    {
        let lease = IndexLease(self.0.lease());
        QueryBuilder::with_criteria(lease, criteria)
    }

    pub fn lease_inner(&self) -> Lease<Arc<InnerIndex>> {
        self.0.lease()
    }

    pub fn documents_addition(&self) -> DocumentsAddition {
        let ranked_map = self.0.lease().ranked_map.clone();
        DocumentsAddition::new(self, ranked_map)
    }

    pub fn documents_deletion(&self) -> DocumentsDeletion {
        DocumentsDeletion::new(self)
    }

    pub fn document<T>(
        &self,
        fields: Option<&HashSet<&str>>,
        id: DocumentId,
    ) -> Result<Option<T>, RmpError>
    where T: de::DeserializeOwned,
    {
        let schema = &self.lease_inner().schema;
        let fields = fields
            .map(|fields| {
                fields
                    .into_iter()
                    .filter_map(|name| schema.attribute(name))
                    .collect()
            });

        let mut deserializer = Deserializer {
            document_id: id,
            index: &self,
            fields: fields.as_ref(),
        };

        // TODO: currently we return an error if all document fields are missing,
        //       returning None would have been better
        T::deserialize(&mut deserializer).map(Some)
    }
}

pub struct IndexLease(Lease<Arc<InnerIndex>>);

impl Store for IndexLease {
    type Error = Error;

    fn words(&self) -> Result<&fst::Set, Self::Error> {
        Ok(&self.0.words)
    }

    fn word_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error> {
        Ok(self.0.raw.words.doc_indexes(word)?)
    }
}

pub struct DocumentsAddition<'a> {
    inner: &'a Index,
    indexer: Indexer,
    ranked_map: RankedMap,
}

impl<'a> DocumentsAddition<'a> {
    fn new(inner: &'a Index, ranked_map: RankedMap) -> DocumentsAddition<'a> {
        DocumentsAddition { inner, indexer: Indexer::new(), ranked_map }
    }

    pub fn update_document<D>(&mut self, document: D) -> Result<(), Error>
    where D: serde::Serialize,
    {
        let schema = &self.inner.lease_inner().schema;
        let identifier = schema.identifier_name();

        let document_id = match extract_document_id(identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        // 1. remove the previous document match indexes
        let mut documents_deletion = DocumentsDeletion::new(self.inner);
        documents_deletion.delete_document(document_id);
        documents_deletion.finalize()?;

        // 2. index the document fields
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

    pub fn finalize(self) -> Result<(), Error> {
        let lease_inner = self.inner.lease_inner();
        let main = &lease_inner.raw.main;
        let words = &lease_inner.raw.words;
        let attrs_words = &lease_inner.raw.attrs_words;

        let Indexed { words_doc_indexes, docs_attrs_words } = self.indexer.build();
        let mut delta_words_builder = SetBuilder::memory();

        for (word, delta_set) in words_doc_indexes {
            delta_words_builder.insert(&word).unwrap();

            let set = match words.doc_indexes(&word)? {
                Some(set) => Union::new(&set, &delta_set).into_set_buf(),
                None => delta_set,
            };

            words.set_doc_indexes(&word, &set)?;
        }

        for ((id, attr), words) in docs_attrs_words {
            attrs_words.set_attr_words(id, attr, &words)?;
        }

        let delta_words = delta_words_builder
            .into_inner()
            .and_then(fst::Set::from_bytes)
            .unwrap();

        let words = match main.words_set()? {
            Some(words) => {
                let op = OpBuilder::new()
                    .add(words.stream())
                    .add(delta_words.stream())
                    .r#union();

                let mut words_builder = SetBuilder::memory();
                words_builder.extend_stream(op).unwrap();
                words_builder
                    .into_inner()
                    .and_then(fst::Set::from_bytes)
                    .unwrap()
            },
            None => delta_words,
        };

        main.set_words_set(&words)?;
        main.set_ranked_map(&self.ranked_map)?;

        // update the "consistent" view of the Index
        let ranked_map = self.ranked_map;
        let schema = lease_inner.schema.clone();
        let raw = lease_inner.raw.clone();

        let inner = InnerIndex { words, schema, ranked_map, raw };
        self.inner.0.store(Arc::new(inner));

        Ok(())
    }
}

pub struct DocumentsDeletion<'a> {
    inner: &'a Index,
    documents: Vec<DocumentId>,
}

impl<'a> DocumentsDeletion<'a> {
    fn new(inner: &'a Index) -> DocumentsDeletion {
        DocumentsDeletion { inner, documents: Vec::new() }
    }

    pub fn delete_document(&mut self, id: DocumentId) {
        self.documents.push(id);
    }

    pub fn finalize(mut self) -> Result<(), Error> {
        let lease_inner = self.inner.lease_inner();
        let main = &lease_inner.raw.main;
        let attrs_words = &lease_inner.raw.attrs_words;
        let words = &lease_inner.raw.words;
        let documents = &lease_inner.raw.documents;

        let idset = {
            self.documents.sort_unstable();
            self.documents.dedup();
            SetBuf::new_unchecked(self.documents)
        };

        let mut words_attrs = HashMap::new();
        for id in idset.into_vec() {
            for result in attrs_words.attrs_words(id) {
                let (attr, words) = result?;
                let mut stream = words.stream();
                while let Some(word) = stream.next() {
                    let word = word.to_vec();
                    words_attrs.entry(word).or_insert_with(Vec::new).push((id, attr));
                }
            }
        }

        let mut removed_words = BTreeSet::new();
        for (word, mut attrs) in words_attrs {
            attrs.sort_unstable();
            attrs.dedup();
            let attrs = SetBuf::new_unchecked(attrs);

            if let Some(doc_indexes) = words.doc_indexes(&word)? {
                let op = DifferenceByKey::new(&doc_indexes, &attrs, |d| d.document_id, |(id, _)| *id);
                let doc_indexes = op.into_set_buf();

                if !doc_indexes.is_empty() {
                    words.set_doc_indexes(&word, &doc_indexes)?;
                } else {
                    words.del_doc_indexes(&word)?;
                    removed_words.insert(word);
                }
            }

            for (id, attr) in attrs.into_vec() {
                documents.del_document_field(id, attr)?;
                attrs_words.del_attr_words(id, attr)?;
            }
        }

        let removed_words = fst::Set::from_iter(removed_words).unwrap();
        let words = match main.words_set()? {
            Some(words_set) => {
                let op = fst::set::OpBuilder::new()
                    .add(words_set.stream())
                    .add(removed_words.stream())
                    .difference();

                let mut words_builder = SetBuilder::memory();
                words_builder.extend_stream(op).unwrap();
                words_builder
                    .into_inner()
                    .and_then(fst::Set::from_bytes)
                    .unwrap()
            },
            None => fst::Set::default(),
        };

        main.set_words_set(&words)?;

        // TODO must update the ranked_map too!

        // update the "consistent" view of the Index
        let ranked_map = lease_inner.ranked_map.clone();
        let schema = lease_inner.schema.clone();
        let raw = lease_inner.raw.clone();

        let inner = InnerIndex { words, schema, ranked_map, raw };
        self.inner.0.store(Arc::new(inner));

        Ok(())
    }
}

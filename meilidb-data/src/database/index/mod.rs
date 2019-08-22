use std::collections::{HashSet, BTreeMap};
use std::convert::TryInto;
use std::sync::Arc;
use std::thread;

use arc_swap::{ArcSwap, Guard};
use meilidb_core::criterion::Criteria;
use meilidb_core::{DocIndex, Store, DocumentId, QueryBuilder};
use meilidb_schema::Schema;
use sdset::SetBuf;
use serde::{de, Serialize, Deserialize};
use sled::Transactional;

use crate::ranked_map::RankedMap;
use crate::serde::{Deserializer, DeserializerError};

use super::Error;

pub use self::custom_settings_index::CustomSettingsIndex;
use self::docs_words_index::DocsWordsIndex;
use self::documents_index::DocumentsIndex;
use self::main_index::MainIndex;
use self::synonyms_index::SynonymsIndex;
use self::words_index::WordsIndex;

use super::{
    DocumentsAddition, FinalDocumentsAddition,
    DocumentsDeletion, FinalDocumentsDeletion,
    SynonymsAddition, FinalSynonymsAddition,
    SynonymsDeletion, FinalSynonymsDeletion,
};

mod custom_settings_index;
mod docs_words_index;
mod documents_index;
mod main_index;
mod synonyms_index;
mod words_index;

fn event_is_set(event: &sled::Event) -> bool {
    match event {
        sled::Event::Set(_, _) => true,
        _ => false,
    }
}

#[derive(Deserialize)]
enum UpdateOwned {
    DocumentsAddition(Vec<serde_json::Value>),
    DocumentsDeletion(Vec<DocumentId>),
    SynonymsAddition(BTreeMap<String, Vec<String>>),
    SynonymsDeletion(BTreeMap<String, Option<Vec<String>>>),
}

#[derive(Serialize)]
enum Update<D: serde::Serialize> {
    DocumentsAddition(Vec<D>),
    DocumentsDeletion(Vec<DocumentId>),
    SynonymsAddition(BTreeMap<String, Vec<String>>),
    SynonymsDeletion(BTreeMap<String, Option<Vec<String>>>),
}

fn spawn_update_system(index: Index) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            let subscription = index.updates_index.watch_prefix(vec![]);
            while let Some(result) = index.updates_index.iter().next() {
                let (key, _) = result.unwrap();

                let updates = &index.updates_index;
                let results = &index.updates_results_index;
                (updates, results).transaction(|(updates, results)| {
                    let update = updates.remove(&key)?.unwrap();
                    let array_id = key.as_ref().try_into().unwrap();
                    let id = u64::from_be_bytes(array_id);

                    // this is an emulation of the try block (#31436)
                    let result: Result<(), Error> = (|| {
                        match bincode::deserialize(&update)? {
                            UpdateOwned::DocumentsAddition(documents) => {
                                let ranked_map = index.cache.load().ranked_map.clone();
                                let mut addition = FinalDocumentsAddition::new(&index, ranked_map);
                                for document in documents {
                                    addition.update_document(document)?;
                                }
                                addition.finalize()?;
                            },
                            UpdateOwned::DocumentsDeletion(documents) => {
                                let ranked_map = index.cache.load().ranked_map.clone();
                                let mut deletion = FinalDocumentsDeletion::new(&index, ranked_map);
                                deletion.extend(documents);
                                deletion.finalize()?;
                            },
                            UpdateOwned::SynonymsAddition(synonyms) => {
                                let addition = FinalSynonymsAddition::from_map(&index, synonyms);
                                addition.finalize()?;
                            },
                            UpdateOwned::SynonymsDeletion(synonyms) => {
                                let deletion = FinalSynonymsDeletion::from_map(&index, synonyms);
                                deletion.finalize()?;
                            },
                        }
                        Ok(())
                    })();

                    let result = result.map_err(|e| e.to_string());
                    let value = bincode::serialize(&result).unwrap();
                    results.insert(&array_id, value)
                })
                .unwrap();
            }

            // this subscription is just used to block
            // the loop until a new update is inserted
            subscription.filter(event_is_set).next();
        }
    })
}

#[derive(Copy, Clone)]
pub struct IndexStats {
    pub number_of_words: usize,
    pub number_of_documents: usize,
    pub number_attrs_in_ranked_map: usize,
}

#[derive(Clone)]
pub struct Index {
    pub(crate) cache: ArcSwap<Cache>,

    // TODO this will be a snapshot in the future
    main_index: MainIndex,
    synonyms_index: SynonymsIndex,
    words_index: WordsIndex,
    docs_words_index: DocsWordsIndex,
    documents_index: DocumentsIndex,
    custom_settings_index: CustomSettingsIndex,

    // used by the update system
    db: sled::Db,
    updates_index: Arc<sled::Tree>,
    updates_results_index: Arc<sled::Tree>,
}

pub(crate) struct Cache {
    pub words: Arc<fst::Set>,
    pub synonyms: Arc<fst::Set>,
    pub schema: Schema,
    pub ranked_map: RankedMap,
}

impl Index {
    pub fn new(db: sled::Db, name: &str) -> Result<Index, Error> {
        Index::new_raw(db, name, None)
    }

    pub fn with_schema(db: sled::Db, name: &str, schema: Schema) -> Result<Index, Error> {
        Index::new_raw(db, name, Some(schema))
    }

    fn new_raw(db: sled::Db, name: &str, schema: Option<Schema>) -> Result<Index, Error> {
        let main_index = db.open_tree(name).map(MainIndex)?;
        let synonyms_index = db.open_tree(format!("{}-synonyms", name)).map(SynonymsIndex)?;
        let words_index = db.open_tree(format!("{}-words", name)).map(WordsIndex)?;
        let docs_words_index = db.open_tree(format!("{}-docs-words", name)).map(DocsWordsIndex)?;
        let documents_index = db.open_tree(format!("{}-documents", name)).map(DocumentsIndex)?;
        let custom_settings_index = db.open_tree(format!("{}-custom", name)).map(CustomSettingsIndex)?;
        let updates_index = db.open_tree(format!("{}-updates", name))?;
        let updates_results_index = db.open_tree(format!("{}-updates-results", name))?;

        let words = match main_index.words_set()? {
            Some(words) => Arc::new(words),
            None => Arc::new(fst::Set::default()),
        };

        let synonyms = match main_index.synonyms_set()? {
            Some(synonyms) => Arc::new(synonyms),
            None => Arc::new(fst::Set::default()),
        };

        let schema = match (schema, main_index.schema()?) {
            (Some(ref expected), Some(ref current)) if current != expected => {
                return Err(Error::SchemaDiffer)
            },
            (Some(expected), Some(_)) => expected,
            (Some(expected), None) => {
                main_index.set_schema(&expected)?;
                expected
            },
            (None, Some(current)) => current,
            (None, None) => return Err(Error::SchemaMissing),
        };

        let ranked_map = match main_index.ranked_map()? {
            Some(map) => map,
            None => RankedMap::default(),
        };

        let cache = Cache { words, synonyms, schema, ranked_map };
        let cache = ArcSwap::from_pointee(cache);

        let index = Index {
            cache,
            main_index,
            synonyms_index,
            words_index,
            docs_words_index,
            documents_index,
            custom_settings_index,
            db,
            updates_index,
            updates_results_index,
        };

        let _handle = spawn_update_system(index.clone());

        Ok(index)
    }

    pub fn stats(&self) -> sled::Result<IndexStats> {
        let cache = self.cache.load();
        Ok(IndexStats {
            number_of_words: cache.words.len(),
            number_of_documents: self.documents_index.len()?,
            number_attrs_in_ranked_map: cache.ranked_map.len(),
        })
    }

    pub fn query_builder(&self) -> QueryBuilder<RefIndex> {
        let ref_index = self.as_ref();
        QueryBuilder::new(ref_index)
    }

    pub fn query_builder_with_criteria<'c>(
        &self,
        criteria: Criteria<'c>,
    ) -> QueryBuilder<'c, RefIndex>
    {
        let ref_index = self.as_ref();
        QueryBuilder::with_criteria(ref_index, criteria)
    }

    pub fn as_ref(&self) -> RefIndex {
        RefIndex {
            cache: self.cache.load(),
            main_index: &self.main_index,
            synonyms_index: &self.synonyms_index,
            words_index: &self.words_index,
            docs_words_index: &self.docs_words_index,
            documents_index: &self.documents_index,
            custom_settings_index: &self.custom_settings_index,
        }
    }

    pub fn schema(&self) -> Schema {
        self.cache.load().schema.clone()
    }

    pub fn custom_settings(&self) -> CustomSettingsIndex {
        self.custom_settings_index.clone()
    }

    pub fn documents_addition<D>(&self) -> DocumentsAddition<D> {
        DocumentsAddition::new(self)
    }

    pub fn documents_deletion(&self) -> DocumentsDeletion {
        DocumentsDeletion::new(self)
    }

    pub fn synonyms_addition(&self) -> SynonymsAddition {
        SynonymsAddition::new(self)
    }

    pub fn synonyms_deletion(&self) -> SynonymsDeletion {
        SynonymsDeletion::new(self)
    }

    pub fn document<T>(
        &self,
        fields: Option<&HashSet<&str>>,
        id: DocumentId,
    ) -> Result<Option<T>, DeserializerError>
    where T: de::DeserializeOwned,
    {
        let schema = self.schema();
        let fields = match fields {
            Some(fields) => fields.into_iter().map(|name| schema.attribute(name)).collect(),
            None => None,
        };

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

impl Index {
    pub(crate) fn push_documents_addition<D>(&self, addition: Vec<D>) -> Result<u64, Error>
    where D: serde::Serialize
    {
        let addition = Update::DocumentsAddition(addition);
        let update = bincode::serialize(&addition)?;
        self.raw_push_update(update)
    }

    pub(crate) fn push_documents_deletion(&self, deletion: Vec<DocumentId>) -> Result<u64, Error> {
        let update = bincode::serialize(&deletion)?;
        self.raw_push_update(update)
    }

    pub(crate) fn push_synonyms_addition(
        &self,
        addition: BTreeMap<String, Vec<String>>,
    ) -> Result<u64, Error>
    {
        let update = bincode::serialize(&addition)?;
        self.raw_push_update(update)
    }

    pub(crate) fn push_synonyms_deletion(
        &self,
        deletion: BTreeMap<String, Option<Vec<String>>>,
    ) -> Result<u64, Error>
    {
        let update = bincode::serialize(&deletion)?;
        self.raw_push_update(update)
    }

    fn raw_push_update(&self, raw_update: Vec<u8>) -> Result<u64, Error> {
        let update_id = self.db.generate_id()?;
        let update_id_array = update_id.to_be_bytes();

        self.updates_index.insert(update_id_array, raw_update)?;

        Ok(update_id)
    }
}

pub struct RefIndex<'a> {
    pub(crate) cache: Guard<'static, Arc<Cache>>,
    pub main_index: &'a MainIndex,
    pub synonyms_index: &'a SynonymsIndex,
    pub words_index: &'a WordsIndex,
    pub docs_words_index: &'a DocsWordsIndex,
    pub documents_index: &'a DocumentsIndex,
    pub custom_settings_index: &'a CustomSettingsIndex,
}

impl Store for RefIndex<'_> {
    type Error = Error;

    fn words(&self) -> Result<&fst::Set, Self::Error> {
        Ok(&self.cache.words)
    }

    fn word_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error> {
        Ok(self.words_index.doc_indexes(word)?)
    }

    fn synonyms(&self) -> Result<&fst::Set, Self::Error> {
        Ok(&self.cache.synonyms)
    }

    fn alternatives_to(&self, word: &[u8]) -> Result<Option<fst::Set>, Self::Error> {
        Ok(self.synonyms_index.alternatives_to(word)?)
    }
}

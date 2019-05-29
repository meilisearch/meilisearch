use std::collections::hash_map::Entry;
use std::collections::{HashSet, HashMap};
use std::path::Path;
use std::sync::{Arc, RwLock};
use meilidb_schema::Schema;

mod custom_settings;
mod docs_words_index;
mod documents_addition;
mod documents_deletion;
mod documents_index;
mod error;
mod index;
mod main_index;
mod raw_index;
mod words_index;

pub use self::error::Error;
pub use self::index::Index;
pub use self::custom_settings::CustomSettings;

use self::docs_words_index::DocsWordsIndex;
use self::documents_addition::DocumentsAddition;
use self::documents_deletion::DocumentsDeletion;
use self::documents_index::DocumentsIndex;
use self::index::InnerIndex;
use self::main_index::MainIndex;
use self::raw_index::{RawIndex, InnerRawIndex};
use self::words_index::WordsIndex;

pub struct Database {
    cache: RwLock<HashMap<String, Arc<Index>>>,
    inner: Arc<rocksdb::DB>,
}

impl Database {
    pub fn start_default<P: AsRef<Path>>(path: P) -> Result<Database, Error> {
        let path = path.as_ref();
        let cache = RwLock::new(HashMap::new());

        let options = {
            let mut options = rocksdb::Options::default();
            options.create_if_missing(true);
            options
        };
        let cfs = rocksdb::DB::list_cf(&options, path).unwrap_or(Vec::new());
        let inner = Arc::new(rocksdb::DB::open_cf(&options, path, &cfs)?);
        let database = Database { cache, inner };

        let mut indexes: Vec<_> = cfs.iter()
            .filter_map(|c| c.split('-').nth(0).filter(|&c| c != "default"))
            .collect();
        indexes.sort_unstable();
        indexes.dedup();

        for index in indexes {
            database.open_index(index)?;
        }

        Ok(database)
    }

    pub fn indexes(&self) -> Result<Option<HashSet<String>>, Error> {
        let bytes = match self.inner.get("indexes")? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let indexes = bincode::deserialize(&bytes)?;
        Ok(Some(indexes))
    }

    fn set_indexes(&self, value: &HashSet<String>) -> Result<(), Error> {
        let bytes = bincode::serialize(value)?;
        self.inner.put("indexes", bytes)?;
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
                if !self.indexes()?.map_or(false, |x| x.contains(name)) {
                    return Ok(None)
                }

                let main = {
                    self.inner.cf_handle(name).expect("cf not found");
                    MainIndex(InnerRawIndex::new(self.inner.clone(), Arc::from(name)))
                };

                let words = {
                    let cf_name = format!("{}-words", name);
                    self.inner.cf_handle(&cf_name).expect("cf not found");
                    WordsIndex(InnerRawIndex::new(self.inner.clone(), Arc::from(cf_name)))
                };

                let docs_words = {
                    let cf_name = format!("{}-docs-words", name);
                    self.inner.cf_handle(&cf_name).expect("cf not found");
                    DocsWordsIndex(InnerRawIndex::new(self.inner.clone(), Arc::from(cf_name)))
                };

                let documents = {
                    let cf_name = format!("{}-documents", name);
                    self.inner.cf_handle(&cf_name).expect("cf not found");
                    DocumentsIndex(InnerRawIndex::new(self.inner.clone(), Arc::from(cf_name)))
                };

                let custom = {
                    let cf_name = format!("{}-custom", name);
                    self.inner.cf_handle(&cf_name).expect("cf not found");
                    CustomSettings(InnerRawIndex::new(self.inner.clone(), Arc::from(cf_name)))
                };

                let raw_index = RawIndex { main, words, docs_words, documents, custom };
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
                    self.inner.create_cf(name, &rocksdb::Options::default())?;
                    MainIndex(InnerRawIndex::new(self.inner.clone(), Arc::from(name)))
                };

                if let Some(prev_schema) = main.schema()? {
                    if prev_schema != schema {
                        return Err(Error::SchemaDiffer)
                    }
                }

                main.set_schema(&schema)?;

                let words = {
                    let cf_name = format!("{}-words", name);
                    self.inner.create_cf(&cf_name, &rocksdb::Options::default())?;
                    WordsIndex(InnerRawIndex::new(self.inner.clone(), Arc::from(cf_name)))
                };

                let docs_words = {
                    let cf_name = format!("{}-docs-words", name);
                    self.inner.create_cf(&cf_name, &rocksdb::Options::default())?;
                    DocsWordsIndex(InnerRawIndex::new(self.inner.clone(), Arc::from(cf_name)))
                };

                let documents = {
                    let cf_name = format!("{}-documents", name);
                    self.inner.create_cf(&cf_name, &rocksdb::Options::default())?;
                    DocumentsIndex(InnerRawIndex::new(self.inner.clone(), Arc::from(cf_name)))
                };

                let custom = {
                    let cf_name = format!("{}-custom", name);
                    self.inner.create_cf(&cf_name, &rocksdb::Options::default())?;
                    CustomSettings(InnerRawIndex::new(self.inner.clone(), Arc::from(cf_name)))
                };

                let mut indexes = self.indexes()?.unwrap_or_else(HashSet::new);
                indexes.insert(name.to_string());
                self.set_indexes(&indexes)?;

                let raw_index = RawIndex { main, words, docs_words, documents, custom };
                let index = Index::from_raw(raw_index)?;

                vacant.insert(Arc::new(index)).clone()
            },
        };

        Ok(index)
    }
}

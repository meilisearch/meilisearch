use std::collections::hash_map::Entry;
use std::collections::{HashSet, HashMap};
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::Schema;

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
use self::raw_index::RawIndex;
use self::words_index::WordsIndex;

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

    pub fn start_with_compression<P: AsRef<Path>>(path: P, factor: i32) -> Result<Database, Error> {
        let config = sled::ConfigBuilder::default()
            .use_compression(true)
            .compression_factor(factor)
            .path(path)
            .build();

        let cache = RwLock::new(HashMap::new());
        let inner = sled::Db::start(config)?;
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

    fn set_indexes(&self, value: &HashSet<String>) -> Result<(), Error> {
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
                if !self.indexes()?.map_or(false, |x| x.contains(name)) {
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

                let docs_words = {
                    let tree_name = format!("{}-docs-words", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    DocsWordsIndex(tree)
                };

                let documents = {
                    let tree_name = format!("{}-documents", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    DocumentsIndex(tree)
                };

                let custom = {
                    let tree_name = format!("{}-custom", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    CustomSettings(tree)
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

                let docs_words = {
                    let tree_name = format!("{}-docs-words", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    DocsWordsIndex(tree)
                };

                let documents = {
                    let tree_name = format!("{}-documents", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    DocumentsIndex(tree)
                };

                let custom = {
                    let tree_name = format!("{}-custom", name);
                    let tree = self.inner.open_tree(tree_name)?;
                    CustomSettings(tree)
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

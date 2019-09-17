use std::collections::hash_map::Entry;
use std::collections::{HashSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;
use meilidb_schema::Schema;

mod error;
mod index;
mod update;

use crate::CfTree;

pub use self::error::Error;
pub use self::index::{Index, CustomSettingsIndex, CommonIndex};

pub use self::update::DocumentsAddition;
pub use self::update::DocumentsDeletion;
pub use self::update::SynonymsAddition;
pub use self::update::SynonymsDeletion;

use self::update::apply_documents_addition;
use self::update::apply_documents_deletion;
use self::update::apply_synonyms_addition;
use self::update::apply_synonyms_deletion;

const INDEXES_KEY: &str = "indexes";
const COMMON_KEY: &str = "common-index";

fn load_indexes(tree: &rocksdb::DB) -> Result<HashSet<String>, Error> {
    match tree.get(INDEXES_KEY)? {
        Some(bytes) => Ok(bincode::deserialize(&bytes)?),
        None => Ok(HashSet::new())
    }
}

pub struct Database {
    cache: RwLock<HashMap<String, Index>>,
    inner: Arc<rocksdb::DB>,
    common: Arc<CommonIndex>,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Database, Error> {
        let cache = RwLock::new(HashMap::new());

        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        let cfs = rocksdb::DB::list_cf(&options, &path).unwrap_or_default();
        let inner = Arc::new(rocksdb::DB::open_cf(&options, path, cfs)?);
        let common_tree = CfTree::create(inner.clone(), COMMON_KEY.to_owned())?;
        let common = Arc::new(CommonIndex(common_tree));
        let indexes = load_indexes(&inner)?;
        let database = Database { cache, inner, common };

        for index in indexes {
            database.open_index(&index)?;
        }

        Ok(database)
    }

    pub fn indexes(&self) -> Result<HashSet<String>, Error> {
        load_indexes(&self.inner)
    }

    fn set_indexes(&self, value: &HashSet<String>) -> Result<(), Error> {
        let bytes = bincode::serialize(value)?;
        self.inner.put(INDEXES_KEY, bytes)?;
        Ok(())
    }

    pub fn open_index(&self, name: &str) -> Result<Option<Index>, Error> {
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
                if !self.indexes()?.contains(name) {
                    return Ok(None)
                }

                let index = Index::new(self.inner.clone(), name)?;
                vacant.insert(index).clone()
            },
        };

        Ok(Some(index))
    }

    pub fn create_index(&self, name: &str, schema: Schema) -> Result<Index, Error> {
        let mut cache = self.cache.write().unwrap();

        let index = match cache.entry(name.to_string()) {
            Entry::Occupied(occupied) => {
                occupied.get().clone()
            },
            Entry::Vacant(vacant) => {
                let index = Index::with_schema(self.inner.clone(), name, schema)?;

                let mut indexes = self.indexes()?;
                indexes.insert(name.to_string());
                self.set_indexes(&indexes)?;

                vacant.insert(index).clone()
            },
        };

        Ok(index)
    }

    pub fn delete_index(&self, name: &str) -> Result<(), Error> {
        let mut cache = self.cache.write().unwrap();

        self.inner.drop_cf(name)?;
        let _ = self.inner.drop_cf(&format!("{}-synonyms", name));
        let _ = self.inner.drop_cf(&format!("{}-words", name));
        let _ = self.inner.drop_cf(&format!("{}-docs-words", name));
        let _ = self.inner.drop_cf(&format!("{}-documents", name));
        let _ = self.inner.drop_cf(&format!("{}-custom", name));
        let _ = self.inner.drop_cf(&format!("{}-updates", name));
        let _ = self.inner.drop_cf(&format!("{}-updates-results", name));
        cache.remove(name);

        if let Ok(mut index_list) = self.indexes() {
            index_list.remove(name);
            let _ = self.set_indexes(&index_list);
        }
        Ok(())
    }

    pub fn common_index(&self) -> Arc<CommonIndex> {
        self.common.clone()
    }

    pub fn checkpoint_to<P>(&self, path: P) -> Result<(), Error>
    where P: AsRef<Path>,
    {
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.inner)?;
        Ok(checkpoint.create_checkpoint(path)?)
    }
}

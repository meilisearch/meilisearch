use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::ops::Deref;

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use heed::types::{Str, SerdeBincode};
use heed::{EnvOpenOptions, Env, Database};
use milli::Index;
use serde::{Serialize, Deserialize};

use crate::data::{SearchQuery, SearchResult};

const CONTROLLER_META_FILENAME: &str = "index_controller_meta";
const INDEXES_CONTROLLER_FILENAME: &str = "indexes_db";
const INDEXES_DB_NAME: &str = "indexes_db";

trait UpdateStore {}

pub struct IndexController<U> {
    update_store: U,
    env: Env,
    indexes_db: Database<Str, SerdeBincode<IndexMetadata>>,
    indexes: DashMap<String, Index>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexControllerMeta {
    open_options: EnvOpenOptions,
    created_at: DateTime<Utc>,
}

impl IndexControllerMeta {
    fn from_path(path: impl AsRef<Path>) -> Result<Option<IndexControllerMeta>> {
        let path = path.as_ref().to_path_buf().push(CONTROLLER_META_FILENAME);
        if path.exists() {
            let mut file = File::open(path)?;
            let mut buffer = Vec::new();
            let n = file.read_to_end(&mut buffer)?;
            let meta: IndexControllerMeta = serde_json::from_slice(&buffer[..n])?;
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    fn to_path(self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref().to_path_buf().push(CONTROLLER_META_FILENAME);
        if path.exists() {
            Err(anyhow::anyhow!("Index controller metadata already exists"))
        } else {
            let mut file = File::create(path)?;
            let json = serde_json::to_vec(&self)?;
            file.write_all(&json)?;
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexMetadata {
    created_at: DateTime<Utc>,
    open_options: EnvOpenOptions,
    id: String,
}

impl IndexMetadata {
    fn open_index(&self) -> Result<Self> {
        todo!()
    }
}

struct IndexView<'a, U> {
    txn: heed::RoTxn<'a>,
    index: &'a Index,
    update_store: &'a U,
}

struct IndexViewMut<'a, U> {
    txn: heed::RwTxn<'a>,
    index: &'a Index,
    update_store: &'a U,
}

impl<'a, U> Deref for IndexViewMut<'a, U> {
    type Target = IndexView<'a, U>;

    fn deref(&self) -> &Self::Target {
        IndexView {
            txn: *self.txn,
            index: self.index,
            update_store: self.update_store,
        }
    }
}

impl<'a, U: UpdateStore> IndexView<'a, U> {
    fn search(&self, search_query: SearchQuery) -> Result<SearchResult> {
        let mut search = self.index.search(self.txn);
        if let Some(query) = &search_query.q {
            search.query(query);
        }

        if let Some(offset) = search_query.offset {
            search.offset(offset);
        }

        let limit = search_query.limit;
        search.limit(limit);

        Ok(search.execute()?)
    }
}

impl<U: UpdateStore> IndexController<U> {
    /// Open the index controller from meta found at path, and create a new one if no meta is
    /// found.
    pub fn new(path: impl AsRef<Path>, update_store: U) -> Result<Self> {
        // If index controller metadata is present, we return the env, otherwise, we create a new
        // metadata from scratch before returning a new env.
        let env = match IndexControllerMeta::from_path(path)? {
            Some(meta) =>  meta.open_options.open(INDEXES_CONTROLLER_FILENAME)?,
            None => {
                let open_options = EnvOpenOptions::new()
                    .map_size(page_size::get() * 1000);
                let env = open_options.open(INDEXES_CONTROLLER_FILENAME)?;
                let created_at = Utc::now();
                let meta = IndexControllerMeta { open_options, created_at };
                meta.to_path(path)?;
                env
            }
        };
        let indexes = DashMap::new();
        let indexes_db = match env.open_database(INDEXES_DB_NAME)? {
            Some(indexes_db) => indexes_db,
            None => env.create_database(INDEXES_DB_NAME)?,
        };

        Ok(Self { env, indexes, indexes_db, update_store })
    }

    pub fn get_or_create<S: AsRef<str>>(&mut self, name: S) -> Result<IndexViewMut<'_, U>> {
        todo!()
    }

    /// Get an index with read access to the db. The index are lazily loaded, meaning that we first
    /// check for its exixtence in the indexes map, and if it doesn't exist, the index db is check
    /// for metadata to launch the index.
    pub fn get<S: AsRef<str>>(&self, name: S) -> Result<Option<IndexView<'_, U>>> {
        match self.indexes.get(name.as_ref()) {
            Some(index) => {
               let txn = index.read_txn()?;
               let update_store = &self.update_store;
               Ok(Some(IndexView { index, update_store, txn }))
            }
            None => {
                let txn = self.env.read_txn()?;
                match self.indexes_db.get(&txn, name.as_ref())? {
                    Some(meta) => {
                        let index = meta.open_index()?;
                        self.indexes.insert(name.as_ref().to_owned(), index);
                        Ok(self.indexes.get(name.as_ref()))
                    }
                    None => Ok(None)
                }
            }
        }
    }

    pub fn get_mut<S: AsRef<str>>(&self, name: S) -> Result<Option<IndexViewMut<'_, U>>> {
        todo!()
    }

    pub async fn delete_index<S: AsRef<str>>(&self, name:S) -> Result<()> {
        todo!()
    }

    pub async fn list_indices(&self) -> Result<Vec<(String, IndexMetadata)>> {
        todo!()
    }

    pub async fn rename_index(&self, old: &str, new: &str) -> Result<()> {
        todo!()
    }
}

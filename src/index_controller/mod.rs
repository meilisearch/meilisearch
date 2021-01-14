use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use heed::types::{Str, SerdeBincode};
use heed::{EnvOpenOptions, Env, Database};
use milli::{Index, FieldsIdsMap, SearchResult, FieldId};
use serde::{Serialize, Deserialize};

use crate::data::SearchQuery;

const CONTROLLER_META_FILENAME: &str = "index_controller_meta";
const INDEXES_CONTROLLER_FILENAME: &str = "indexes_db";
const INDEXES_DB_NAME: &str = "indexes_db";

pub trait UpdateStore {}

pub struct IndexController<U> {
    path: PathBuf,
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
        let mut path = path.as_ref().to_path_buf();
        path.push(CONTROLLER_META_FILENAME);
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
        let mut path = path.as_ref().to_path_buf();
        path.push(CONTROLLER_META_FILENAME);
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
    fn open_index(&self, path: impl AsRef<Path>) -> Result<Index> {
        // create a path in the form "db_path/indexes/index_id"
        let mut path = path.as_ref().to_path_buf();
        path.push("indexes");
        path.push(&self.id);
        Ok(Index::new(self.open_options, path)?)
    }
}

struct IndexView<'a, U> {
    txn: heed::RoTxn<'a>,
    index: Ref<'a, String, Index>,
    update_store: &'a U,
}

impl<'a, U: UpdateStore> IndexView<'a, U> {
    pub fn search(&self, search_query: SearchQuery) -> Result<SearchResult> {
        let mut search = self.index.search(&self.txn);
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

    pub fn fields_ids_map(&self) -> Result<FieldsIdsMap> {
        Ok(self.index.fields_ids_map(&self.txn)?)
    }

    pub fn fields_displayed_fields_ids(&self) -> Result<Option<Vec<FieldId>>> {
        Ok(self.index.displayed_fields_ids(&self.txn)?)
    }

    pub fn documents(&self, ids: Vec<u32>) -> Result<Vec<(u32, obkv::KvReader<'_>)>> {
        Ok(self.index.documents(&self.txn, ids)?)
    }
}

impl<U: UpdateStore> IndexController<U> {
    /// Open the index controller from meta found at path, and create a new one if no meta is
    /// found.
    pub fn new(path: impl AsRef<Path>, update_store: U) -> Result<Self> {
        // If index controller metadata is present, we return the env, otherwise, we create a new
        // metadata from scratch before returning a new env.
        let path = path.as_ref().to_path_buf();
        let env = match IndexControllerMeta::from_path(&path)? {
            Some(meta) =>  meta.open_options.open(INDEXES_CONTROLLER_FILENAME)?,
            None => {
                let open_options = EnvOpenOptions::new()
                    .map_size(page_size::get() * 1000);
                let env = open_options.open(INDEXES_CONTROLLER_FILENAME)?;
                let created_at = Utc::now();
                let meta = IndexControllerMeta { open_options: open_options.clone(), created_at };
                meta.to_path(path)?;
                env
            }
        };
        let indexes = DashMap::new();
        let indexes_db = match env.open_database(Some(INDEXES_DB_NAME))? {
            Some(indexes_db) => indexes_db,
            None => env.create_database(Some(INDEXES_DB_NAME))?,
        };

        Ok(Self { env, indexes, indexes_db, update_store, path })
    }

    pub fn get_or_create<S: AsRef<str>>(&mut self, name: S) -> Result<IndexView<'_, U>> {
        todo!()
    }

    /// Get an index with read access to the db. The index are lazily loaded, meaning that we first
    /// check for its exixtence in the indexes map, and if it doesn't exist, the index db is check
    /// for metadata to launch the index.
    pub fn get<S: AsRef<str>>(&self, name: S) -> Result<Option<IndexView<'_, U>>> {
        let update_store = &self.update_store;
        match self.indexes.get(name.as_ref()) {
            Some(index) => {
               let txn = index.read_txn()?;
               Ok(Some(IndexView { index, update_store, txn }))
            }
            None => {
                let txn = self.env.read_txn()?;
                match self.indexes_db.get(&txn, name.as_ref())? {
                    Some(meta) => {
                        let index = meta.open_index(self.path)?;
                        self.indexes.insert(name.as_ref().to_owned(), index);
                        // TODO: create index view
                        match self.indexes.get(name.as_ref()) {
                            Some(index) => {
                                let txn = index.read_txn()?;
                                Ok(Some(IndexView { index, txn, update_store }))
                            }
                            None => Ok(None)
                        }
                    }
                    None => Ok(None)
                }
            }
        }
    }

    pub fn get_mut<S: AsRef<str>>(&self, name: S) -> Result<Option<IndexView<'_, U>>> {
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

use std::fs::{create_dir_all, remove_dir_all};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::bail;
use chrono::{DateTime, Utc};
use dashmap::{DashMap, mapref::entry::Entry};
use heed::{Env, EnvOpenOptions, Database, types::{Str, SerdeJson, ByteSlice}, RoTxn, RwTxn};
use log::error;
use milli::Index;
use rayon::ThreadPool;
use serde::{Serialize, Deserialize};
use uuid::Uuid;

use crate::option::IndexerOpts;
use super::update_handler::UpdateHandler;
use super::{UpdateMeta, UpdateResult};

type UpdateStore = super::update_store::UpdateStore<UpdateMeta, UpdateResult, String>;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct IndexMeta {
    update_store_size: u64,
    index_store_size: u64,
    pub uuid: Uuid,
    pub created_at: DateTime<Utc>,
}

impl IndexMeta {
    fn open(
        &self,
        path: impl AsRef<Path>,
        thread_pool: Arc<ThreadPool>,
        indexer_options: &IndexerOpts,
    ) -> anyhow::Result<(Arc<Index>, Arc<UpdateStore>)> {
        let update_path = make_update_db_path(&path, &self.uuid);
        let index_path = make_index_db_path(&path, &self.uuid);

        create_dir_all(&update_path)?;
        create_dir_all(&index_path)?;

        let mut options = EnvOpenOptions::new();
        options.map_size(self.index_store_size as usize);
        let index = Arc::new(Index::new(options, index_path)?);

        let mut options = EnvOpenOptions::new();
        options.map_size(self.update_store_size as usize);
        let handler = UpdateHandler::new(indexer_options, index.clone(), thread_pool)?;
        let update_store = UpdateStore::open(options, update_path, handler)?;

        Ok((index, update_store))
    }
}

pub struct IndexStore {
    env: Env,
    name_to_uuid: Database<Str, ByteSlice>,
    uuid_to_index: DashMap<Uuid, (Arc<Index>, Arc<UpdateStore>)>,
    uuid_to_index_meta: Database<ByteSlice, SerdeJson<IndexMeta>>,

    thread_pool: Arc<ThreadPool>,
    indexer_options: IndexerOpts,
}

impl IndexStore {
    pub fn new(path: impl AsRef<Path>, indexer_options: IndexerOpts) -> anyhow::Result<Self> {
        let env = EnvOpenOptions::new()
            .map_size(4096 * 100)
            .max_dbs(2)
            .open(path)?;

        let uuid_to_index = DashMap::new();
        let name_to_uuid = open_or_create_database(&env, Some("name_to_uid"))?;
        let uuid_to_index_meta = open_or_create_database(&env, Some("uid_to_index_db"))?;

        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(indexer_options.indexing_jobs.unwrap_or(0))
            .build()?;
        let thread_pool = Arc::new(thread_pool);

        Ok(Self {
            env,
            name_to_uuid,
            uuid_to_index,
            uuid_to_index_meta,

            thread_pool,
            indexer_options,
        })
    }

    fn index_uuid(&self, txn: &RoTxn, name: impl AsRef<str>) -> anyhow::Result<Option<Uuid>> {
        match self.name_to_uuid.get(txn, name.as_ref())? {
            Some(bytes) => {
                let uuid = Uuid::from_slice(bytes)?;
                Ok(Some(uuid))
            }
            None => Ok(None)
        }
    }

    fn retrieve_index(&self, txn: &RoTxn, uid: Uuid) -> anyhow::Result<Option<(Arc<Index>, Arc<UpdateStore>)>> {
        match self.uuid_to_index.entry(uid.clone()) {
            Entry::Vacant(entry) => {
                match self.uuid_to_index_meta.get(txn, uid.as_bytes())? {
                    Some(meta) => {
                        let path = self.env.path();
                        let (index, updates) = meta.open(path, self.thread_pool.clone(), &self.indexer_options)?;
                        entry.insert((index.clone(), updates.clone()));
                        Ok(Some((index, updates)))
                    },
                    None => Ok(None)
                }
            }
            Entry::Occupied(entry) => {
                let (index, updates) = entry.get();
                Ok(Some((index.clone(), updates.clone())))
            }
        }
    }

    fn get_index_txn(&self, txn: &RoTxn, name: impl AsRef<str>) -> anyhow::Result<Option<(Arc<Index>, Arc<UpdateStore>)>> {
        match self.index_uuid(&txn, name)? {
            Some(uid) => self.retrieve_index(&txn, uid),
            None => Ok(None),
        }
    }

    pub fn index(&self, name: impl AsRef<str>) -> anyhow::Result<Option<(Arc<Index>, Arc<UpdateStore>)>> {
        let txn = self.env.read_txn()?;
        self.get_index_txn(&txn, name)
    }

    pub fn get_or_create_index(
        &self,
        name: impl AsRef<str>,
        update_size: u64,
        index_size: u64,
    ) -> anyhow::Result<(Arc<Index>, Arc<UpdateStore>)> {
        let mut txn = self.env.write_txn()?;
        match self.get_index_txn(&txn, name.as_ref())? {
            Some(res) => Ok(res),
            None => {
                let uuid = Uuid::new_v4();
                let result = self.create_index_txn(&mut txn, uuid, name, update_size, index_size)?;
                // If we fail to commit the transaction, we must delete the database from the
                // file-system.
                if let Err(e) = txn.commit() {
                    self.clean_db(uuid);
                    return Err(e)?;
                }
                Ok(result)
            },
        }
    }

    // Remove all the files and data associated with a db uuid.
    fn clean_db(&self, uuid: Uuid) {
        let update_db_path = make_update_db_path(self.env.path(), &uuid);
        let index_db_path = make_index_db_path(self.env.path(), &uuid);

        remove_dir_all(update_db_path).expect("Failed to clean database");
        remove_dir_all(index_db_path).expect("Failed to clean database");

        self.uuid_to_index.remove(&uuid);
    }

    fn create_index_txn( &self,
        txn: &mut RwTxn,
        uuid: Uuid,
        name: impl AsRef<str>,
        update_store_size: u64,
        index_store_size: u64,
    ) -> anyhow::Result<(Arc<Index>, Arc<UpdateStore>)> {
        let created_at = Utc::now();
        let meta = IndexMeta { update_store_size, index_store_size, uuid: uuid.clone(), created_at };

        self.name_to_uuid.put(txn, name.as_ref(), uuid.as_bytes())?;
        self.uuid_to_index_meta.put(txn, uuid.as_bytes(), &meta)?;

        let path = self.env.path();
        let (index, update_store) = match meta.open(path, self.thread_pool.clone(), &self.indexer_options) {
            Ok(res) => res,
            Err(e) => {
                self.clean_db(uuid);
                return Err(e)
            }
        };

        self.uuid_to_index.insert(uuid, (index.clone(), update_store.clone()));

        Ok((index, update_store))
    }

    /// Same a get or create, but returns an error if the index already exists.
    pub fn create_index(
        &self,
        name: impl AsRef<str>,
        update_size: u64,
        index_size: u64,
    ) -> anyhow::Result<(Arc<Index>, Arc<UpdateStore>)> {
        let uuid = Uuid::new_v4();
        let mut txn = self.env.write_txn()?;

        if self.name_to_uuid.get(&txn, name.as_ref())?.is_some() {
            bail!("cannot create index {:?}: an index with this name already exists.")
        }

        let result = self.create_index_txn(&mut txn, uuid, name, update_size, index_size)?;
        // If we fail to commit the transaction, we must delete the database from the
        // file-system.
        if let Err(e) = txn.commit() {
            self.clean_db(uuid);
            return Err(e)?;
        }
        Ok(result)
    }

    /// Returns each index associated with it's metadata;
    pub fn list_indexes(&self) -> anyhow::Result<Vec<(String, IndexMeta)>> {
        let txn = self.env.read_txn()?;
        let indexes = self.name_to_uuid
            .iter(&txn)?
            .filter_map(|entry| entry
                .map_err(|e| {
                    error!("error decoding entry while listing indexes: {}", e);
                    e
                })
                .ok())
            .map(|(name, uuid)| {
                let meta = self.uuid_to_index_meta
                    .get(&txn, &uuid)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| panic!("corrupted database, index {} should exist.", name));
                (name.to_owned(), meta)
            })
            .collect();
        Ok(indexes)
    }
}

fn open_or_create_database<K: 'static, V: 'static>(env: &Env, name: Option<&str>) -> anyhow::Result<Database<K, V>> {
    match env.open_database::<K, V>(name)? {
        Some(db) => Ok(db),
        None => Ok(env.create_database::<K, V>(name)?),
    }
}

fn make_update_db_path(path: impl AsRef<Path>, uuid: &Uuid) -> PathBuf {
    let mut path = path.as_ref().to_path_buf();
    path.push(format!("update{}", uuid));
    path
}

fn make_index_db_path(path: impl AsRef<Path>, uuid: &Uuid) -> PathBuf {
    let mut path = path.as_ref().to_path_buf();
    path.push(format!("index{}", uuid));
    path
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_make_update_db_path() {
        let uuid = Uuid::new_v4();
        assert_eq!(
            make_update_db_path("/home", &uuid),
            PathBuf::from(format!("/home/update{}", uuid))
        );
    }

    #[test]
    fn test_make_index_db_path() {
        let uuid = Uuid::new_v4();
        assert_eq!(
            make_index_db_path("/home", &uuid),
            PathBuf::from(format!("/home/index{}", uuid))
        );
    }

    mod index_store {
        use super::*;

        #[test]
        fn test_index_uuid() {
            let temp = tempfile::tempdir().unwrap();
            let store = IndexStore::new(temp, IndexerOpts::default()).unwrap();

            let name = "foobar";
            let txn = store.env.read_txn().unwrap();
            // name is not found if the uuid in not present in the db
            assert!(store.index_uuid(&txn, &name).unwrap().is_none());
            drop(txn);

            // insert an uuid in the the name_to_uuid_db:
            let uuid = Uuid::new_v4();
            let mut txn = store.env.write_txn().unwrap();
            store.name_to_uuid.put(&mut txn, &name, uuid.as_bytes()).unwrap();
            txn.commit().unwrap();

            // check that the uuid is there
            let txn = store.env.read_txn().unwrap();
            assert_eq!(store.index_uuid(&txn, &name).unwrap(), Some(uuid));
        }

        #[test]
        fn test_retrieve_index() {
            let temp = tempfile::tempdir().unwrap();
            let store = IndexStore::new(temp, IndexerOpts::default()).unwrap();
            let uuid = Uuid::new_v4();

            let txn = store.env.read_txn().unwrap();
            assert!(store.retrieve_index(&txn, uuid).unwrap().is_none());

            let meta = IndexMeta {
                update_store_size: 4096 * 100,
                index_store_size: 4096 * 100,
                uuid: uuid.clone(),
                created_at: Utc::now(),
            };
            let mut txn = store.env.write_txn().unwrap();
            store.uuid_to_index_meta.put(&mut txn, uuid.as_bytes(), &meta).unwrap();
            txn.commit().unwrap();

            // the index cache should be empty
            assert!(store.uuid_to_index.is_empty());

            let txn = store.env.read_txn().unwrap();
            assert!(store.retrieve_index(&txn, uuid).unwrap().is_some());
            assert_eq!(store.uuid_to_index.len(), 1);
        }

        #[test]
        fn test_index() {
            let temp = tempfile::tempdir().unwrap();
            let store = IndexStore::new(temp, IndexerOpts::default()).unwrap();
            let name = "foobar";

            assert!(store.index(&name).unwrap().is_none());

            let uuid = Uuid::new_v4();
            let meta = IndexMeta {
                update_store_size: 4096 * 100,
                index_store_size: 4096 * 100,
                uuid: uuid.clone(),
                created_at: Utc::now(),
            };
            let mut txn = store.env.write_txn().unwrap();
            store.name_to_uuid.put(&mut txn, &name, uuid.as_bytes()).unwrap();
            store.uuid_to_index_meta.put(&mut txn, uuid.as_bytes(), &meta).unwrap();
            txn.commit().unwrap();

            assert!(store.index(&name).unwrap().is_some());
        }

        #[test]
        fn test_get_or_create_index() {
            let temp = tempfile::tempdir().unwrap();
            let store = IndexStore::new(temp, IndexerOpts::default()).unwrap();
            let name = "foobar";

            let update_store_size = 4096 * 100;
            let index_store_size = 4096 * 100;
            store.get_or_create_index(&name, update_store_size, index_store_size).unwrap();
            let txn = store.env.read_txn().unwrap();
            let  uuid = store.name_to_uuid.get(&txn, &name).unwrap();
            assert_eq!(store.uuid_to_index.len(), 1);
            assert!(uuid.is_some());
            let uuid = Uuid::from_slice(uuid.unwrap()).unwrap();
            let meta = store.uuid_to_index_meta.get(&txn, uuid.as_bytes()).unwrap().unwrap();
            assert_eq!(meta.update_store_size, update_store_size);
            assert_eq!(meta.index_store_size, index_store_size);
            assert_eq!(meta.uuid, uuid);
        }

        #[test]
        fn test_create_index() {
            let temp = tempfile::tempdir().unwrap();
            let store = IndexStore::new(temp, IndexerOpts::default()).unwrap();
            let name = "foobar";

            let update_store_size = 4096 * 100;
            let index_store_size = 4096 * 100;
            let uuid = Uuid::new_v4();
            let mut txn = store.env.write_txn().unwrap();
            store.create_index_txn(&mut txn, uuid, name, update_store_size, index_store_size).unwrap();
            let uuid = store.name_to_uuid.get(&txn, &name).unwrap();
            assert_eq!(store.uuid_to_index.len(), 1);
            assert!(uuid.is_some());
            let uuid = Uuid::from_slice(uuid.unwrap()).unwrap();
            let meta = store.uuid_to_index_meta.get(&txn, uuid.as_bytes()).unwrap().unwrap();
            assert_eq!(meta.update_store_size, update_store_size);
            assert_eq!(meta.index_store_size, index_store_size);
            assert_eq!(meta.uuid, uuid);
        }
    }
}

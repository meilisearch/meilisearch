use std::fs::{create_dir_all, remove_dir_all};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use dashmap::{mapref::entry::Entry, DashMap};
use heed::{
    types::{ByteSlice, SerdeJson, Str},
    Database, Env, EnvOpenOptions, RoTxn, RwTxn,
};
use log::{error, info};
use milli::Index;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::update_handler::UpdateHandler;
use super::{UpdateMeta, UpdateResult};
use crate::option::IndexerOpts;

type UpdateStore = super::update_store::UpdateStore<UpdateMeta, UpdateResult, String>;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct IndexMeta {
    update_store_size: u64,
    index_store_size: u64,
    pub uuid: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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

    pub fn delete(&self, index_uid: impl AsRef<str>) -> anyhow::Result<()> {
        // we remove the references to the index from the index map so it is not accessible anymore
        let mut txn = self.env.write_txn()?;
        let uuid = self
            .index_uuid(&txn, &index_uid)?
            .with_context(|| format!("Index {:?} doesn't exist", index_uid.as_ref()))?;
        self.name_to_uuid.delete(&mut txn, index_uid.as_ref())?;
        self.uuid_to_index_meta.delete(&mut txn, uuid.as_bytes())?;
        txn.commit()?;
        // If the index was loaded (i.e it is present in the uuid_to_index map), then we need to
        // close it. The process goes as follow:
        //
        // 1) We want to remove any pending updates from the store.
        // 2) We try to get ownership on the update store so we can close it. It may take a
        // couple of tries, but since the update store event loop only has a weak reference to
        // itself, and we are the only other function holding a reference to it otherwise, we will
        // get it eventually.
        // 3) We request a closing of the update store.
        // 4) We can take ownership on the index, and close it.
        // 5) We remove all the files from the file system.
        let index_uid = index_uid.as_ref().to_string();
        let path = self.env.path().to_owned();
        if let Some((_, (index, updates))) = self.uuid_to_index.remove(&uuid) {
            std::thread::spawn(move || {
                info!("Preparing for {:?} deletion.", index_uid);
                // this error is non fatal, but may delay the deletion.
                if let Err(e) = updates.abort_pendings() {
                    error!(
                        "error aborting pending updates when deleting index {:?}: {}",
                        index_uid, e
                    );
                }
                let updates = get_arc_ownership_blocking(updates);
                let close_event = updates.prepare_for_closing();
                close_event.wait();
                info!("closed update store for {:?}", index_uid);

                let index = get_arc_ownership_blocking(index);
                let close_event = index.prepare_for_closing();
                close_event.wait();

                let update_path = make_update_db_path(&path, &uuid);
                let index_path = make_index_db_path(&path, &uuid);

                if let Err(e) = remove_dir_all(index_path) {
                    error!("error removing index {:?}: {}", index_uid, e);
                }

                if let Err(e) = remove_dir_all(update_path) {
                    error!("error removing index {:?}: {}", index_uid, e);
                }

                info!("index {:?} deleted.", index_uid);
            });
        }

        Ok(())
    }

    fn index_uuid(&self, txn: &RoTxn, name: impl AsRef<str>) -> anyhow::Result<Option<Uuid>> {
        match self.name_to_uuid.get(txn, name.as_ref())? {
            Some(bytes) => {
                let uuid = Uuid::from_slice(bytes)?;
                Ok(Some(uuid))
            }
            None => Ok(None),
        }
    }

    fn retrieve_index(
        &self,
        txn: &RoTxn,
        uid: Uuid,
    ) -> anyhow::Result<Option<(Arc<Index>, Arc<UpdateStore>)>> {
        match self.uuid_to_index.entry(uid.clone()) {
            Entry::Vacant(entry) => match self.uuid_to_index_meta.get(txn, uid.as_bytes())? {
                Some(meta) => {
                    let path = self.env.path();
                    let (index, updates) =
                        meta.open(path, self.thread_pool.clone(), &self.indexer_options)?;
                    entry.insert((index.clone(), updates.clone()));
                    Ok(Some((index, updates)))
                }
                None => Ok(None),
            },
            Entry::Occupied(entry) => {
                let (index, updates) = entry.get();
                Ok(Some((index.clone(), updates.clone())))
            }
        }
    }

    fn get_index_txn(
        &self,
        txn: &RoTxn,
        name: impl AsRef<str>,
    ) -> anyhow::Result<Option<(Arc<Index>, Arc<UpdateStore>)>> {
        match self.index_uuid(&txn, name)? {
            Some(uid) => self.retrieve_index(&txn, uid),
            None => Ok(None),
        }
    }

    pub fn index(
        &self,
        name: impl AsRef<str>,
    ) -> anyhow::Result<Option<(Arc<Index>, Arc<UpdateStore>)>> {
        let txn = self.env.read_txn()?;
        self.get_index_txn(&txn, name)
    }

    /// Use this function to perform an update on an index.
    /// This function also puts a lock on what index is allowed to perform an update.
    pub fn update_index<F, T>(&self, name: impl AsRef<str>, f: F) -> anyhow::Result<(T, IndexMeta)>
    where
        F: FnOnce(&Index) -> anyhow::Result<T>,
    {
        let mut txn = self.env.write_txn()?;
        let (index, _) = self
            .get_index_txn(&txn, &name)?
            .with_context(|| format!("Index {:?} doesn't exist", name.as_ref()))?;
        let result = f(index.as_ref());
        match result {
            Ok(ret) => {
                let meta = self.update_meta(&mut txn, name, |meta| meta.updated_at = Utc::now())?;
                txn.commit()?;
                Ok((ret, meta))
            }
            Err(e) => Err(e),
        }
    }

    pub fn index_with_meta(
        &self,
        name: impl AsRef<str>,
    ) -> anyhow::Result<Option<(Arc<Index>, IndexMeta)>> {
        let txn = self.env.read_txn()?;
        let uuid = self.index_uuid(&txn, &name)?;
        match uuid {
            Some(uuid) => {
                let meta = self
                    .uuid_to_index_meta
                    .get(&txn, uuid.as_bytes())?
                    .with_context(|| {
                        format!("unable to retrieve metadata for index {:?}", name.as_ref())
                    })?;
                let (index, _) = self
                    .retrieve_index(&txn, uuid)?
                    .with_context(|| format!("unable to retrieve index {:?}", name.as_ref()))?;
                Ok(Some((index, meta)))
            }
            None => Ok(None),
        }
    }

    fn update_meta<F>(
        &self,
        txn: &mut RwTxn,
        name: impl AsRef<str>,
        f: F,
    ) -> anyhow::Result<IndexMeta>
    where
        F: FnOnce(&mut IndexMeta),
    {
        let uuid = self
            .index_uuid(txn, &name)?
            .with_context(|| format!("Index {:?} doesn't exist", name.as_ref()))?;
        let mut meta = self
            .uuid_to_index_meta
            .get(txn, uuid.as_bytes())?
            .with_context(|| format!("couldn't retrieve metadata for index {:?}", name.as_ref()))?;
        f(&mut meta);
        self.uuid_to_index_meta.put(txn, uuid.as_bytes(), &meta)?;
        Ok(meta)
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
                let (index, updates, _) =
                    self.create_index_txn(&mut txn, uuid, name, update_size, index_size)?;
                // If we fail to commit the transaction, we must delete the database from the
                // file-system.
                if let Err(e) = txn.commit() {
                    self.clean_db(uuid);
                    return Err(e)?;
                }
                Ok((index, updates))
            }
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

    fn create_index_txn(
        &self,
        txn: &mut RwTxn,
        uuid: Uuid,
        name: impl AsRef<str>,
        update_store_size: u64,
        index_store_size: u64,
    ) -> anyhow::Result<(Arc<Index>, Arc<UpdateStore>, IndexMeta)> {
        let created_at = Utc::now();
        let updated_at = created_at;
        let meta = IndexMeta {
            update_store_size,
            index_store_size,
            uuid: uuid.clone(),
            created_at,
            updated_at,
        };

        self.name_to_uuid.put(txn, name.as_ref(), uuid.as_bytes())?;
        self.uuid_to_index_meta.put(txn, uuid.as_bytes(), &meta)?;

        let path = self.env.path();
        let (index, update_store) =
            match meta.open(path, self.thread_pool.clone(), &self.indexer_options) {
                Ok(res) => res,
                Err(e) => {
                    self.clean_db(uuid);
                    return Err(e);
                }
            };

        self.uuid_to_index
            .insert(uuid, (index.clone(), update_store.clone()));

        Ok((index, update_store, meta))
    }

    /// Same as `get_or_create`, but returns an error if the index already exists.
    pub fn create_index(
        &self,
        name: impl AsRef<str>,
        update_size: u64,
        index_size: u64,
    ) -> anyhow::Result<(Arc<Index>, Arc<UpdateStore>, IndexMeta)> {
        let uuid = Uuid::new_v4();
        let mut txn = self.env.write_txn()?;

        if self.name_to_uuid.get(&txn, name.as_ref())?.is_some() {
            bail!("index {:?} already exists", name.as_ref())
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

    /// Returns each index associated with its metadata:
    /// (index_name, IndexMeta, primary_key)
    /// This method will force all the indexes to be loaded.
    pub fn list_indexes(&self) -> anyhow::Result<Vec<(String, IndexMeta, Option<String>)>> {
        let txn = self.env.read_txn()?;
        let metas = self.name_to_uuid.iter(&txn)?.filter_map(|entry| {
            entry
                .map_err(|e| {
                    error!("error decoding entry while listing indexes: {}", e);
                    e
                })
                .ok()
        });
        let mut indexes = Vec::new();
        for (name, uuid) in metas {
            // get index to retrieve primary key
            let (index, _) = self
                .get_index_txn(&txn, name)?
                .with_context(|| format!("could not load index {:?}", name))?;
            let primary_key = index.primary_key(&index.read_txn()?)?.map(String::from);
            // retieve meta
            let meta = self
                .uuid_to_index_meta
                .get(&txn, &uuid)?
                .with_context(|| format!("could not retieve meta for index {:?}", name))?;
            indexes.push((name.to_owned(), meta, primary_key));
        }
        Ok(indexes)
    }
}

// Loops on an arc to get ownership on the wrapped value. This method sleeps 100ms before retrying.
fn get_arc_ownership_blocking<T>(mut item: Arc<T>) -> T {
    loop {
        match Arc::try_unwrap(item) {
            Ok(item) => return item,
            Err(item_arc) => {
                item = item_arc;
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        }
    }
}

fn open_or_create_database<K: 'static, V: 'static>(
    env: &Env,
    name: Option<&str>,
) -> anyhow::Result<Database<K, V>> {
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
            store
                .name_to_uuid
                .put(&mut txn, &name, uuid.as_bytes())
                .unwrap();
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

            let created_at = Utc::now();
            let updated_at = created_at;

            let meta = IndexMeta {
                update_store_size: 4096 * 100,
                index_store_size: 4096 * 100,
                uuid: uuid.clone(),
                created_at,
                updated_at,
            };
            let mut txn = store.env.write_txn().unwrap();
            store
                .uuid_to_index_meta
                .put(&mut txn, uuid.as_bytes(), &meta)
                .unwrap();
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

            let created_at = Utc::now();
            let updated_at = created_at;

            let uuid = Uuid::new_v4();
            let meta = IndexMeta {
                update_store_size: 4096 * 100,
                index_store_size: 4096 * 100,
                uuid: uuid.clone(),
                created_at,
                updated_at,
            };
            let mut txn = store.env.write_txn().unwrap();
            store
                .name_to_uuid
                .put(&mut txn, &name, uuid.as_bytes())
                .unwrap();
            store
                .uuid_to_index_meta
                .put(&mut txn, uuid.as_bytes(), &meta)
                .unwrap();
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
            store
                .get_or_create_index(&name, update_store_size, index_store_size)
                .unwrap();
            let txn = store.env.read_txn().unwrap();
            let uuid = store.name_to_uuid.get(&txn, &name).unwrap();
            assert_eq!(store.uuid_to_index.len(), 1);
            assert!(uuid.is_some());
            let uuid = Uuid::from_slice(uuid.unwrap()).unwrap();
            let meta = store
                .uuid_to_index_meta
                .get(&txn, uuid.as_bytes())
                .unwrap()
                .unwrap();
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
            store
                .create_index_txn(&mut txn, uuid, name, update_store_size, index_store_size)
                .unwrap();
            let uuid = store.name_to_uuid.get(&txn, &name).unwrap();
            assert_eq!(store.uuid_to_index.len(), 1);
            assert!(uuid.is_some());
            let uuid = Uuid::from_slice(uuid.unwrap()).unwrap();
            let meta = store
                .uuid_to_index_meta
                .get(&txn, uuid.as_bytes())
                .unwrap()
                .unwrap();
            assert_eq!(meta.update_store_size, update_store_size);
            assert_eq!(meta.index_store_size, index_store_size);
            assert_eq!(meta.uuid, uuid);
        }
    }
}

use std::path::{Path, PathBuf};
use std::fs::create_dir_all;
use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use heed::{Env, EnvOpenOptions, Database, types::{Str, SerdeJson, ByteSlice}, RoTxn, RwTxn};
use milli::Index;
use rayon::ThreadPool;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use log::warn;

use crate::option::IndexerOpts;
use super::update_handler::UpdateHandler;
use super::{UpdateMeta, UpdateResult};

type UpdateStore = super::update_store::UpdateStore<UpdateMeta, UpdateResult, String>;

#[derive(Serialize, Deserialize, Debug)]
struct IndexMeta {
    update_size: u64,
    index_size: u64,
    uid: Uuid,
}

impl IndexMeta {
    fn open(
        &self,
        path: impl AsRef<Path>,
        thread_pool: Arc<ThreadPool>,
        opt: &IndexerOpts,
    ) -> anyhow::Result<(Arc<Index>, Arc<UpdateStore>)> {
        let update_path = make_update_db_path(&path, &self.uid);
        let index_path = make_index_db_path(&path, &self.uid);

        create_dir_all(&update_path)?;
        create_dir_all(&index_path)?;

        let mut options = EnvOpenOptions::new();
        options.map_size(self.index_size as usize);
        let index = Arc::new(Index::new(options, index_path)?);

        let mut options = EnvOpenOptions::new();
        options.map_size(self.update_size as usize);
        let handler = UpdateHandler::new(opt, index.clone(), thread_pool)?;
        let update_store = UpdateStore::open(options, update_path, handler)?;
        Ok((index, update_store))
    }
}

pub struct IndexStore {
    env: Env,
    name_to_uid: DashMap<String, Uuid>,
    name_to_uid_db: Database<Str, ByteSlice>,
    uid_to_index: DashMap<Uuid, (Arc<Index>, Arc<UpdateStore>)>,
    uid_to_index_db: Database<ByteSlice, SerdeJson<IndexMeta>>,

    thread_pool: Arc<ThreadPool>,
    opt: IndexerOpts,
}

impl IndexStore {
    pub fn new(path: impl AsRef<Path>, opt: IndexerOpts) -> anyhow::Result<Self> {
        let env = EnvOpenOptions::new()
            .map_size(4096 * 100)
            .max_dbs(2)
            .open(path)?;

        let name_to_uid = DashMap::new();
        let uid_to_index = DashMap::new();
        let name_to_uid_db = open_or_create_database(&env, Some("name_to_uid"))?;
        let uid_to_index_db = open_or_create_database(&env, Some("uid_to_index_db"))?;

        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(opt.indexing_jobs.unwrap_or(0))
            .build()?;
        let thread_pool = Arc::new(thread_pool);

        Ok(Self {
            env,
            name_to_uid,
            name_to_uid_db,
            uid_to_index,
            uid_to_index_db,

            thread_pool,
            opt,
        })
    }

    fn index_uid(&self, txn: &RoTxn, name: impl AsRef<str>) -> anyhow::Result<Option<Uuid>> {
        match self.name_to_uid.entry(name.as_ref().to_string()) {
            Entry::Vacant(entry) => {
                match self.name_to_uid_db.get(txn, name.as_ref())? {
                    Some(bytes) => {
                        let uuid = Uuid::from_slice(bytes)?;
                        entry.insert(uuid);
                        Ok(Some(uuid))
                    }
                    None => Ok(None)
                }
            }
            Entry::Occupied(entry) => Ok(Some(entry.get().clone())),
        }
    }

    fn retrieve_index(&self, txn: &RoTxn, uid: Uuid) -> anyhow::Result<Option<(Arc<Index>, Arc<UpdateStore>)>> {
        match self.uid_to_index.entry(uid.clone()) {
            Entry::Vacant(entry) => {
                match self.uid_to_index_db.get(txn, uid.as_bytes())? {
                    Some(meta) => {
                        let path = self.env.path();
                        let (index, updates) = meta.open(path, self.thread_pool.clone(), &self.opt)?;
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

    fn _get_index(&self, txn: &RoTxn, name: impl AsRef<str>) -> anyhow::Result<Option<(Arc<Index>, Arc<UpdateStore>)>> {
        match self.index_uid(&txn, name)? {
            Some(uid) => self.retrieve_index(&txn, uid),
            None => Ok(None),
        }
    }

    pub fn index(&self, name: impl AsRef<str>) -> anyhow::Result<Option<(Arc<Index>, Arc<UpdateStore>)>> {
        let txn = self.env.read_txn()?;
        self._get_index(&txn, name)
    }

    pub fn get_or_create_index(
        &self, name: impl AsRef<str>,
        update_size: u64,
        index_size: u64,
        ) -> anyhow::Result<(Arc<Index>, Arc<UpdateStore>)> {
        let mut txn = self.env.write_txn()?;
        match self._get_index(&txn, name.as_ref())? {
            Some(res) => Ok(res),
            None => {
                let uid = Uuid::new_v4();
                // TODO: clean in case of error
                let result = self.create_index(&mut txn, uid, name, update_size, index_size);
                match result {
                    Ok((index, update_store)) => {
                        match txn.commit() {
                            Ok(_) => Ok((index, update_store)),
                            Err(e) => {
                                self.clean_uid(&uid);
                                Err(anyhow::anyhow!("error creating index: {}", e))
                            }
                        }
                    }
                    Err(e) => {
                        self.clean_uid(&uid);
                        Err(e)
                    }
                }
            },
        }
    }

    /// removes all data acociated with an index Uuid. This is called when index creation failed
    /// and outstanding files and data need to be cleaned.
    fn clean_uid(&self, _uid: &Uuid) {
        // TODO!
        warn!("creating cleanup is not yet implemented");
    }

    fn create_index( &self,
        txn: &mut RwTxn,
        uid: Uuid,
        name: impl AsRef<str>,
        update_size: u64,
        index_size: u64,
    ) -> anyhow::Result<(Arc<Index>, Arc<UpdateStore>)> {
        let meta = IndexMeta { update_size, index_size, uid: uid.clone() };

        self.name_to_uid_db.put(txn, name.as_ref(), uid.as_bytes())?;
        self.uid_to_index_db.put(txn, uid.as_bytes(), &meta)?;

        let path = self.env.path();
        let (index, update_store) = meta.open(path, self.thread_pool.clone(), &self.opt)?;

        self.name_to_uid.insert(name.as_ref().to_string(), uid);
        self.uid_to_index.insert(uid, (index.clone(), update_store.clone()));

        Ok((index, update_store))
    }
}

fn open_or_create_database<K: 'static, V: 'static>(env: &Env, name: Option<&str>) -> anyhow::Result<Database<K, V>> {
    match env.open_database::<K, V>(name)? {
        Some(db) => Ok(db),
        None => Ok(env.create_database::<K, V>(name)?),
    }
}

fn make_update_db_path(path: impl AsRef<Path>, uid: &Uuid) -> PathBuf {
    let mut path = path.as_ref().to_path_buf();
    path.push(format!("update{}", uid));
    path
}

fn make_index_db_path(path: impl AsRef<Path>, uid: &Uuid) -> PathBuf {
    let mut path = path.as_ref().to_path_buf();
    path.push(format!("index{}", uid));
    path
}

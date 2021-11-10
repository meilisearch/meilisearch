use std::collections::HashSet;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use heed::types::{SerdeBincode, Str};
use heed::{CompactionOption, Database, Env, EnvOpenOptions};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::error::{IndexResolverError, Result};
use crate::EnvSizer;
use crate::tasks::task::TaskId;

const UUID_STORE_SIZE: usize = 1_073_741_824; //1GiB

#[derive(Serialize, Deserialize)]
struct DumpEntry {
    uuid: Uuid,
    uid: String,
}

const UUIDS_DB_PATH: &str = "index_uuids";

#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait UuidStore: Sized {
    // Create a new entry for `name`. Return an error if `err` and the entry already exists, return
    // the uuid otherwise.
    async fn get_uuid(&self, uid: String) -> Result<(String, Option<Uuid>)>;
    async fn delete(&self, uid: String) -> Result<Option<Uuid>>;
    async fn list(&self) -> Result<Vec<(String, Uuid)>>;
    async fn insert(&self, name: String, uuid: Uuid, task_id: TaskId) -> Result<()>;
    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
    async fn get_size(&self) -> Result<u64>;
    async fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
    async fn get_index_creation_task_id(&self, index_uid: String) -> Result<TaskId>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct IndexMeta {
    uuid: Uuid,
    index_creation_task_id: TaskId,
}

#[derive(Clone)]
pub struct HeedUuidStore {
    env: Env,
    db: Database<Str, SerdeBincode<IndexMeta>>,
}

impl HeedUuidStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().join(UUIDS_DB_PATH);
        create_dir_all(&path)?;
        let mut options = EnvOpenOptions::new();
        options.map_size(UUID_STORE_SIZE); // 1GB
        options.max_dbs(1);
        let env = options.open(path)?;
        let db = env.create_database(Some("uuids"))?;
        Ok(Self { env, db })
    }

    fn get_uuid(&self, name: &str) -> Result<Option<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        let txn = env.read_txn()?;
        match db.get(&txn, name)? {
            Some(IndexMeta { uuid, .. }) => Ok(Some(uuid)),
            None => Ok(None),
        }
    }

    fn delete(&self, uid: String) -> Result<Option<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        let mut txn = env.write_txn()?;
        match db.get(&txn, &uid)? {
            Some(IndexMeta { uuid, .. }) => {
                db.delete(&mut txn, &uid)?;
                txn.commit()?;
                Ok(Some(uuid))
            }
            None => Ok(None),
        }
    }

    fn list(&self) -> Result<Vec<(String, Uuid)>> {
        let env = self.env.clone();
        let db = self.db;
        let txn = env.read_txn()?;
        let mut entries = Vec::new();
        for entry in db.iter(&txn)? {
            let (name, IndexMeta { uuid, .. }) = entry?;
            entries.push((name.to_owned(), uuid))
        }
        Ok(entries)
    }

    pub(crate) fn insert(&self, name: String, uuid: Uuid, task_id: TaskId) -> Result<()> {
        let env = self.env.clone();
        let db = self.db;
        let mut txn = env.write_txn()?;

        if db.get(&txn, &name)?.is_some() {
            return Err(IndexResolverError::IndexAlreadyExists(name));
        }

        let meta = IndexMeta {
            uuid,
            index_creation_task_id: task_id,
        };
        db.put(&mut txn, &name, &meta)?;
        txn.commit()?;
        Ok(())
    }

    fn snapshot(&self, mut path: PathBuf) -> Result<HashSet<Uuid>> {
        // Write transaction to acquire a lock on the database.
        let txn = self.env.write_txn()?;
        let mut entries = HashSet::new();
        for entry in self.db.iter(&txn)? {
            let (_, IndexMeta { uuid, .. }) = entry?;
            entries.insert(uuid);
        }

        // only perform snapshot if there are indexes
        if !entries.is_empty() {
            path.push(UUIDS_DB_PATH);
            create_dir_all(&path).unwrap();
            path.push("data.mdb");
            self.env.copy_to_path(path, CompactionOption::Enabled)?;
        }
        Ok(entries)
    }

    fn get_index_creation_task_id(&self, index_uid: String) -> Result<TaskId> {
        let txn = self.env.read_txn()?;

        match self.db.get(&txn, &index_uid)? {
            Some(IndexMeta {index_creation_task_id, .. }) => Ok(index_creation_task_id),
            None => Err(IndexResolverError::UnexistingIndex(index_uid))
        }
    }

    fn get_size(&self) -> Result<u64> {
        Ok(self.env.size())
    }

    // pub fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
    //     let dump_path = path.join(UUIDS_DB_PATH);
    //     create_dir_all(&dump_path)?;
    //     let dump_file_path = dump_path.join("data.jsonl");
    //     let mut dump_file = File::create(&dump_file_path)?;
    //     let mut uuids = HashSet::new();

    //     let txn = self.env.read_txn()?;
    //     for entry in self.db.iter(&txn)? {
    //         let (uid, uuid) = entry?;
    //         let uid = uid.to_string();
    //         let uuid = Uuid::from_slice(uuid)?;

    //         let entry = DumpEntry { uuid, uid };
    //         serde_json::to_writer(&mut dump_file, &entry)?;
    //         dump_file.write_all(b"\n").unwrap();

    //         uuids.insert(uuid);
    //     }

    //     Ok(uuids)
    // }

    // pub fn load_dump(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
    //     let uuid_resolver_path = dst.as_ref().join(UUIDS_DB_PATH);
    //     std::fs::create_dir_all(&uuid_resolver_path)?;

    //     let src_indexes = src.as_ref().join(UUIDS_DB_PATH).join("data.jsonl");
    //     let indexes = File::open(&src_indexes)?;
    //     let mut indexes = BufReader::new(indexes);
    //     let mut line = String::new();

    //     let db = Self::new(dst)?;
    //     let mut txn = db.env.write_txn()?;

    //     loop {
    //         match indexes.read_line(&mut line) {
    //             Ok(0) => break,
    //             Ok(_) => {
    //                 let DumpEntry { uuid, uid } = serde_json::from_str(&line)?;
    //                 db.db.put(&mut txn, &uid, uuid.as_bytes())?;
    //             }
    //             Err(e) => return Err(e.into()),
    //         }

    //         line.clear();
    //     }
    //     txn.commit()?;

    //     db.env.prepare_for_closing().wait();

    //     Ok(())
    // }
}

#[async_trait::async_trait]
impl UuidStore for HeedUuidStore {
    async fn get_uuid(&self, name: String) -> Result<(String, Option<Uuid>)> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.get_uuid(&name).map(|res| (name, res))).await?
    }

    async fn delete(&self, uid: String) -> Result<Option<Uuid>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.delete(uid)).await?
    }

    async fn list(&self) -> Result<Vec<(String, Uuid)>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.list()).await?
    }

    async fn insert(&self, name: String, uuid: Uuid, task_id: TaskId) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.insert(name, uuid, task_id)).await?
    }

    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.snapshot(path)).await?
    }

    async fn get_size(&self) -> Result<u64> {
        self.get_size()
    }

    async fn dump(&self, _path: PathBuf) -> Result<HashSet<Uuid>> {
        todo!()
        // let this = self.clone();
        // tokio::task::spawn_blocking(move || this.dump(path)).await?
    }

    async fn get_index_creation_task_id(&self, index_uid: String) -> Result<TaskId> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.get_index_creation_task_id(index_uid)).await?
    }
}

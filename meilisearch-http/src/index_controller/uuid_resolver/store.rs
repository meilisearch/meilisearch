use std::collections::HashSet;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use heed::{
    types::{ByteSlice, Str},
    CompactionOption, Database, Env, EnvOpenOptions,
};
use uuid::Uuid;

use super::{Result, UuidError, UUID_STORE_SIZE};
use crate::helpers::EnvSizer;

#[async_trait::async_trait]
pub trait UuidStore {
    // Create a new entry for `name`. Return an error if `err` and the entry already exists, return
    // the uuid otherwise.
    async fn create_uuid(&self, uid: String, err: bool) -> Result<Uuid>;
    async fn get_uuid(&self, uid: String) -> Result<Option<Uuid>>;
    async fn delete(&self, uid: String) -> Result<Option<Uuid>>;
    async fn list(&self) -> Result<Vec<(String, Uuid)>>;
    async fn insert(&self, name: String, uuid: Uuid) -> Result<()>;
    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
    async fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
    async fn get_size(&self) -> Result<u64>;
}

#[derive(Clone)]
pub struct HeedUuidStore {
    env: Env,
    db: Database<Str, ByteSlice>,
}

impl HeedUuidStore {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().join("index_uuids");
        create_dir_all(&path)?;
        let mut options = EnvOpenOptions::new();
        options.map_size(UUID_STORE_SIZE); // 1GB
        let env = options.open(path)?;
        let db = env.create_database(None)?;
        Ok(Self { env, db })
    }

    pub fn create_uuid(&self, name: String, err: bool) -> Result<Uuid> {
        let env = self.env.clone();
        let db = self.db;
        let mut txn = env.write_txn()?;
        match db.get(&txn, &name)? {
            Some(uuid) => {
                if err {
                    Err(UuidError::NameAlreadyExist)
                } else {
                    let uuid = Uuid::from_slice(uuid)?;
                    Ok(uuid)
                }
            }
            None => {
                let uuid = Uuid::new_v4();
                db.put(&mut txn, &name, uuid.as_bytes())?;
                txn.commit()?;
                Ok(uuid)
            }
        }
    }

    pub fn get_uuid(&self, name: String) -> Result<Option<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        let txn = env.read_txn()?;
        match db.get(&txn, &name)? {
            Some(uuid) => {
                let uuid = Uuid::from_slice(uuid)?;
                Ok(Some(uuid))
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, uid: String) -> Result<Option<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        let mut txn = env.write_txn()?;
        match db.get(&txn, &uid)? {
            Some(uuid) => {
                let uuid = Uuid::from_slice(uuid)?;
                db.delete(&mut txn, &uid)?;
                txn.commit()?;
                Ok(Some(uuid))
            }
            None => Ok(None),
        }
    }

    pub fn list(&self) -> Result<Vec<(String, Uuid)>> {
        let env = self.env.clone();
        let db = self.db;
        let txn = env.read_txn()?;
        let mut entries = Vec::new();
        for entry in db.iter(&txn)? {
            let (name, uuid) = entry?;
            let uuid = Uuid::from_slice(uuid)?;
            entries.push((name.to_owned(), uuid))
        }
        Ok(entries)
    }

    pub fn insert(&self, name: String, uuid: Uuid) -> Result<()> {
        let env = self.env.clone();
        let db = self.db;
        let mut txn = env.write_txn()?;
        db.put(&mut txn, &name, uuid.as_bytes())?;
        txn.commit()?;
        Ok(())
    }

    // TODO: we should merge this function and the following function for the dump. it's exactly
    // the same code
    pub fn snapshot(&self, mut path: PathBuf) -> Result<HashSet<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        // Write transaction to acquire a lock on the database.
        let txn = env.write_txn()?;
        let mut entries = HashSet::new();
        for entry in db.iter(&txn)? {
            let (_, uuid) = entry?;
            let uuid = Uuid::from_slice(uuid)?;
            entries.insert(uuid);
        }

        // only perform snapshot if there are indexes
        if !entries.is_empty() {
            path.push("index_uuids");
            create_dir_all(&path).unwrap();
            path.push("data.mdb");
            env.copy_to_path(path, CompactionOption::Enabled)?;
        }
        Ok(entries)
    }

    pub fn dump(&self, mut path: PathBuf) -> Result<HashSet<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        // Write transaction to acquire a lock on the database.
        let txn = env.write_txn()?;
        let mut entries = HashSet::new();
        for entry in db.iter(&txn)? {
            let (_, uuid) = entry?;
            let uuid = Uuid::from_slice(uuid)?;
            entries.insert(uuid);
        }

        // only perform dump if there are indexes
        if !entries.is_empty() {
            path.push("index_uuids");
            create_dir_all(&path).unwrap();
            path.push("data.mdb");
            env.copy_to_path(path, CompactionOption::Enabled)?;
        }
        Ok(entries)
    }

    pub fn get_size(&self) -> Result<u64> {
        Ok(self.env.size())
    }
}

#[async_trait::async_trait]
impl UuidStore for HeedUuidStore {
    async fn create_uuid(&self, name: String, err: bool) -> Result<Uuid> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.create_uuid(name, err)).await?
    }

    async fn get_uuid(&self, name: String) -> Result<Option<Uuid>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.get_uuid(name)).await?
    }

    async fn delete(&self, uid: String) -> Result<Option<Uuid>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.delete(uid)).await?
    }

    async fn list(&self) -> Result<Vec<(String, Uuid)>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.list()).await?
    }

    async fn insert(&self, name: String, uuid: Uuid) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.insert(name, uuid)).await?
    }

    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.snapshot(path)).await?
    }

    async fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.dump(path)).await?
    }

    async fn get_size(&self) -> Result<u64> {
        self.get_size()
    }
}

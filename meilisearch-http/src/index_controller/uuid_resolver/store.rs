use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use heed::{
    types::{ByteSlice, Str},
    Database, Env, EnvOpenOptions,CompactionOption
};
use uuid::Uuid;

use super::{UUID_STORE_SIZE, UuidError, Result};

#[async_trait::async_trait]
pub trait UuidStore {
    // Create a new entry for `name`. Return an error if `err` and the entry already exists, return
    // the uuid otherwise.
    async fn create_uuid(&self, uid: String, err: bool) -> Result<Uuid>;
    async fn get_uuid(&self, uid: String) -> Result<Option<Uuid>>;
    async fn delete(&self, uid: String) -> Result<Option<Uuid>>;
    async fn list(&self) -> Result<Vec<(String, Uuid)>>;
    async fn snapshot(&self, path: PathBuf) -> Result<Vec<Uuid>>;
}

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
}

#[async_trait::async_trait]
impl UuidStore for HeedUuidStore {
    async fn create_uuid(&self, name: String, err: bool) -> Result<Uuid> {
        let env = self.env.clone();
        let db = self.db;
        tokio::task::spawn_blocking(move || {
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
        })
        .await?
    }
    async fn get_uuid(&self, name: String) -> Result<Option<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        tokio::task::spawn_blocking(move || {
            let txn = env.read_txn()?;
            match db.get(&txn, &name)? {
                Some(uuid) => {
                    let uuid = Uuid::from_slice(uuid)?;
                    Ok(Some(uuid))
                }
                None => Ok(None),
            }
        })
        .await?
    }

    async fn delete(&self, uid: String) -> Result<Option<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        tokio::task::spawn_blocking(move || {
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
        })
        .await?
    }

    async fn list(&self) -> Result<Vec<(String, Uuid)>> {
        let env = self.env.clone();
        let db = self.db;
        tokio::task::spawn_blocking(move || {
            let txn = env.read_txn()?;
            let mut entries = Vec::new();
            for entry in db.iter(&txn)? {
                let (name, uuid) = entry?;
                let uuid = Uuid::from_slice(uuid)?;
                entries.push((name.to_owned(), uuid))
            }
            Ok(entries)
        })
        .await?
    }

    async fn snapshot(&self, mut path: PathBuf) -> Result<Vec<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        tokio::task::spawn_blocking(move || {
            // Write transaction to acquire a lock on the database.
            let txn = env.write_txn()?;
            let mut entries = Vec::new();
            for entry in db.iter(&txn)? {
                let (_, uuid) = entry?;
                let uuid = Uuid::from_slice(uuid)?;
                entries.push(uuid)
            }

            // only perform snapshot if there are indexes
            if !entries.is_empty() {
                path.push("index_uuids");
                create_dir_all(&path).unwrap();
                path.push("data.mdb");
                env.copy_to_path(path, CompactionOption::Enabled)?;
            }
            Ok(entries)
        })
        .await?
    }
}

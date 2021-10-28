use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use heed::types::{ByteSlice, Str};
use heed::{CompactionOption, Database, Env, EnvOpenOptions};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::error::{IndexResolverError, Result};
use crate::EnvSizer;

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
    async fn insert(&self, name: String, uuid: Uuid) -> Result<()>;
    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
    async fn get_size(&self) -> Result<u64>;
    async fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
}

#[derive(Clone)]
pub struct HeedUuidStore {
    env: Env,
    db: Database<Str, ByteSlice>,
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

    pub fn get_uuid(&self, name: &str) -> Result<Option<Uuid>> {
        let env = self.env.clone();
        let db = self.db;
        let txn = env.read_txn()?;
        match db.get(&txn, name)? {
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

        if db.get(&txn, &name)?.is_some() {
            return Err(IndexResolverError::IndexAlreadyExists(name));
        }

        db.put(&mut txn, &name, uuid.as_bytes())?;
        txn.commit()?;
        Ok(())
    }

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
            path.push(UUIDS_DB_PATH);
            create_dir_all(&path).unwrap();
            path.push("data.mdb");
            env.copy_to_path(path, CompactionOption::Enabled)?;
        }
        Ok(entries)
    }

    pub fn get_size(&self) -> Result<u64> {
        Ok(self.env.size())
    }

    pub fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        let dump_path = path.join(UUIDS_DB_PATH);
        create_dir_all(&dump_path)?;
        let dump_file_path = dump_path.join("data.jsonl");
        let mut dump_file = File::create(&dump_file_path)?;
        let mut uuids = HashSet::new();

        let txn = self.env.read_txn()?;
        for entry in self.db.iter(&txn)? {
            let (uid, uuid) = entry?;
            let uid = uid.to_string();
            let uuid = Uuid::from_slice(uuid)?;

            let entry = DumpEntry { uuid, uid };
            serde_json::to_writer(&mut dump_file, &entry)?;
            dump_file.write_all(b"\n").unwrap();

            uuids.insert(uuid);
        }

        Ok(uuids)
    }

    pub fn load_dump(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
        let uuid_resolver_path = dst.as_ref().join(UUIDS_DB_PATH);
        std::fs::create_dir_all(&uuid_resolver_path)?;

        let src_indexes = src.as_ref().join(UUIDS_DB_PATH).join("data.jsonl");
        let indexes = File::open(&src_indexes)?;
        let mut indexes = BufReader::new(indexes);
        let mut line = String::new();

        let db = Self::new(dst)?;
        let mut txn = db.env.write_txn()?;

        loop {
            match indexes.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let DumpEntry { uuid, uid } = serde_json::from_str(&line)?;
                    db.db.put(&mut txn, &uid, uuid.as_bytes())?;
                }
                Err(e) => return Err(e.into()),
            }

            line.clear();
        }
        txn.commit()?;

        db.env.prepare_for_closing().wait();

        Ok(())
    }
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

    async fn insert(&self, name: String, uuid: Uuid) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.insert(name, uuid)).await?
    }

    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.snapshot(path)).await?
    }

    async fn get_size(&self) -> Result<u64> {
        self.get_size()
    }

    async fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.dump(path)).await?
    }
}

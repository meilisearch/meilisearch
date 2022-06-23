use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use walkdir::WalkDir;

use milli::heed::types::{SerdeBincode, Str};
use milli::heed::{CompactionOption, Database, Env};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::error::{IndexResolverError, Result};
use crate::tasks::task::TaskId;

#[derive(Serialize, Deserialize)]
pub struct DumpEntry {
    pub uid: String,
    pub index_meta: IndexMeta,
}

const UUIDS_DB_PATH: &str = "index_uuids";

#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait IndexMetaStore: Sized {
    // Create a new entry for `name`. Return an error if `err` and the entry already exists, return
    // the uuid otherwise.
    async fn get(&self, uid: String) -> Result<(String, Option<IndexMeta>)>;
    async fn delete(&self, uid: String) -> Result<Option<IndexMeta>>;
    async fn list(&self) -> Result<Vec<(String, IndexMeta)>>;
    async fn insert(&self, name: String, meta: IndexMeta) -> Result<()>;
    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
    async fn get_size(&self) -> Result<u64>;
    async fn dump(&self, path: PathBuf) -> Result<()>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexMeta {
    pub uuid: Uuid,
    pub creation_task_id: TaskId,
}

#[derive(Clone)]
pub struct HeedMetaStore {
    env: Arc<Env>,
    db: Database<Str, SerdeBincode<IndexMeta>>,
}

impl Drop for HeedMetaStore {
    fn drop(&mut self) {
        if Arc::strong_count(&self.env) == 1 {
            self.env.as_ref().clone().prepare_for_closing();
        }
    }
}

impl HeedMetaStore {
    pub fn new(env: Arc<milli::heed::Env>) -> Result<Self> {
        let db = env.create_database(Some("uuids"))?;
        Ok(Self { env, db })
    }

    fn get(&self, name: &str) -> Result<Option<IndexMeta>> {
        let env = self.env.clone();
        let db = self.db;
        let txn = env.read_txn()?;
        match db.get(&txn, name)? {
            Some(meta) => Ok(Some(meta)),
            None => Ok(None),
        }
    }

    fn delete(&self, uid: String) -> Result<Option<IndexMeta>> {
        let env = self.env.clone();
        let db = self.db;
        let mut txn = env.write_txn()?;
        match db.get(&txn, &uid)? {
            Some(meta) => {
                db.delete(&mut txn, &uid)?;
                txn.commit()?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    fn list(&self) -> Result<Vec<(String, IndexMeta)>> {
        let env = self.env.clone();
        let db = self.db;
        let txn = env.read_txn()?;
        let mut entries = Vec::new();
        for entry in db.iter(&txn)? {
            let (name, meta) = entry?;
            entries.push((name.to_string(), meta))
        }
        Ok(entries)
    }

    pub(crate) fn insert(&self, name: String, meta: IndexMeta) -> Result<()> {
        let env = self.env.clone();
        let db = self.db;
        let mut txn = env.write_txn()?;

        if db.get(&txn, &name)?.is_some() {
            return Err(IndexResolverError::IndexAlreadyExists(name));
        }

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

    fn get_size(&self) -> Result<u64> {
        Ok(WalkDir::new(self.env.path())
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.metadata().ok())
            .filter(|metadata| metadata.is_file())
            .fold(0, |acc, m| acc + m.len()))
    }

    pub fn dump(&self, path: PathBuf) -> Result<()> {
        let dump_path = path.join(UUIDS_DB_PATH);
        create_dir_all(&dump_path)?;
        let dump_file_path = dump_path.join("data.jsonl");
        let mut dump_file = File::create(&dump_file_path)?;

        let txn = self.env.read_txn()?;
        for entry in self.db.iter(&txn)? {
            let (uid, index_meta) = entry?;
            let uid = uid.to_string();

            let entry = DumpEntry { uid, index_meta };
            serde_json::to_writer(&mut dump_file, &entry)?;
            dump_file.write_all(b"\n").unwrap();
        }

        Ok(())
    }

    pub fn load_dump(src: impl AsRef<Path>, env: Arc<milli::heed::Env>) -> Result<()> {
        let src_indexes = src.as_ref().join(UUIDS_DB_PATH).join("data.jsonl");
        let indexes = File::open(&src_indexes)?;
        let mut indexes = BufReader::new(indexes);
        let mut line = String::new();

        let db = Self::new(env)?;
        let mut txn = db.env.write_txn()?;

        loop {
            match indexes.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let DumpEntry { uid, index_meta } = serde_json::from_str(&line)?;
                    db.db.put(&mut txn, &uid, &index_meta)?;
                }
                Err(e) => return Err(e.into()),
            }

            line.clear();
        }
        txn.commit()?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl IndexMetaStore for HeedMetaStore {
    async fn get(&self, name: String) -> Result<(String, Option<IndexMeta>)> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.get(&name).map(|res| (name, res))).await?
    }

    async fn delete(&self, uid: String) -> Result<Option<IndexMeta>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.delete(uid)).await?
    }

    async fn list(&self) -> Result<Vec<(String, IndexMeta)>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.list()).await?
    }

    async fn insert(&self, name: String, meta: IndexMeta) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.insert(name, meta)).await?
    }

    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.snapshot(path)).await?
    }

    async fn get_size(&self) -> Result<u64> {
        self.get_size()
    }

    async fn dump(&self, path: PathBuf) -> Result<()> {
        let this = self.clone();
        Ok(tokio::task::spawn_blocking(move || this.dump(path)).await??)
    }
}

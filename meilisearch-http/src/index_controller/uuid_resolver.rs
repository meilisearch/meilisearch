use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use heed::{
    types::{ByteSlice, Str},
    Database, Env, EnvOpenOptions,CompactionOption
};
use log::{info, warn};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::helpers::compression;

const UUID_STORE_SIZE: usize = 1_073_741_824; //1GiB

pub type Result<T> = std::result::Result<T, UuidError>;

#[derive(Debug)]
enum UuidResolveMsg {
    Resolve {
        uid: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
    GetOrCreate {
        uid: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
    Create {
        uid: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
    Delete {
        uid: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
    List {
        ret: oneshot::Sender<Result<Vec<(String, Uuid)>>>,
    },
    SnapshotRequest {
        path: PathBuf,
        ret: oneshot::Sender<Result<Vec<Uuid>>>,
    },
}

struct UuidResolverActor<S> {
    inbox: mpsc::Receiver<UuidResolveMsg>,
    store: S,
}

impl<S: UuidStore> UuidResolverActor<S> {
    fn new(inbox: mpsc::Receiver<UuidResolveMsg>, store: S) -> Self {
        Self { inbox, store }
    }

    async fn run(mut self) {
        use UuidResolveMsg::*;

        info!("uuid resolver started");

        loop {
            match self.inbox.recv().await {
                Some(Create { uid: name, ret }) => {
                    let _ = ret.send(self.handle_create(name).await);
                }
                Some(GetOrCreate { uid: name, ret }) => {
                    let _ = ret.send(self.handle_get_or_create(name).await);
                }
                Some(Resolve { uid: name, ret }) => {
                    let _ = ret.send(self.handle_resolve(name).await);
                }
                Some(Delete { uid: name, ret }) => {
                    let _ = ret.send(self.handle_delete(name).await);
                }
                Some(List { ret }) => {
                    let _ = ret.send(self.handle_list().await);
                }
                Some(SnapshotRequest { path, ret }) => {
                    let _ = ret.send(self.handle_snapshot(path).await);
                }
                // all senders have been dropped, need to quit.
                None => break,
            }
        }

        warn!("exiting uuid resolver loop");
    }

    async fn handle_create(&self, uid: String) -> Result<Uuid> {
        if !is_index_uid_valid(&uid) {
            return Err(UuidError::BadlyFormatted(uid));
        }
        self.store.create_uuid(uid, true).await
    }

    async fn handle_get_or_create(&self, uid: String) -> Result<Uuid> {
        if !is_index_uid_valid(&uid) {
            return Err(UuidError::BadlyFormatted(uid));
        }
        self.store.create_uuid(uid, false).await
    }

    async fn handle_resolve(&self, uid: String) -> Result<Uuid> {
        self.store
            .get_uuid(uid.clone())
            .await?
            .ok_or(UuidError::UnexistingIndex(uid))
    }

    async fn handle_delete(&self, uid: String) -> Result<Uuid> {
        self.store
            .delete(uid.clone())
            .await?
            .ok_or(UuidError::UnexistingIndex(uid))
    }

    async fn handle_list(&self) -> Result<Vec<(String, Uuid)>> {
        let result = self.store.list().await?;
        Ok(result)
    }

    async fn handle_snapshot(&self, path: PathBuf) -> Result<Vec<Uuid>> {
        self.store.snapshot(path).await
    }
}

fn is_index_uid_valid(uid: &str) -> bool {
    uid.chars()
        .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
}

#[derive(Clone)]
pub struct UuidResolverHandle {
    sender: mpsc::Sender<UuidResolveMsg>,
}

impl UuidResolverHandle {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let (sender, reveiver) = mpsc::channel(100);
        let store = HeedUuidStore::new(path)?;
        let actor = UuidResolverActor::new(reveiver, store);
        tokio::spawn(actor.run());
        Ok(Self { sender })
    }

    pub fn from_snapshot(
        db_path: impl AsRef<Path>,
        snapshot_path: impl AsRef<Path>
    ) -> anyhow::Result<Self> {
        let (sender, reveiver) = mpsc::channel(100);
        let store = HeedUuidStore::from_snapshot(snapshot_path, db_path)?;
        let actor = UuidResolverActor::new(reveiver, store);
        tokio::spawn(actor.run());
        Ok(Self { sender })
    }

    pub async fn resolve(&self, name: String) -> anyhow::Result<Uuid> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::Resolve { uid: name, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    pub async fn get_or_create(&self, name: String) -> Result<Uuid> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::GetOrCreate { uid: name, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    pub async fn create(&self, name: String) -> anyhow::Result<Uuid> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::Create { uid: name, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    pub async fn delete(&self, name: String) -> anyhow::Result<Uuid> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::Delete { uid: name, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    pub async fn list(&self) -> anyhow::Result<Vec<(String, Uuid)>> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::List { ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    pub async fn snapshot(&self, path: PathBuf) -> Result<Vec<Uuid>> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::SnapshotRequest { path, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }
}

#[derive(Debug, Error)]
pub enum UuidError {
    #[error("Name already exist.")]
    NameAlreadyExist,
    #[error("Index \"{0}\" doesn't exist.")]
    UnexistingIndex(String),
    #[error("Error performing task: {0}")]
    TokioTask(#[from] tokio::task::JoinError),
    #[error("Database error: {0}")]
    Heed(#[from] heed::Error),
    #[error("Uuid error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("Badly formatted index uid: {0}")]
    BadlyFormatted(String),
}

#[async_trait::async_trait]
trait UuidStore {
    // Create a new entry for `name`. Return an error if `err` and the entry already exists, return
    // the uuid otherwise.
    async fn create_uuid(&self, uid: String, err: bool) -> Result<Uuid>;
    async fn get_uuid(&self, uid: String) -> Result<Option<Uuid>>;
    async fn delete(&self, uid: String) -> Result<Option<Uuid>>;
    async fn list(&self) -> Result<Vec<(String, Uuid)>>;
    async fn snapshot(&self, path: PathBuf) -> Result<Vec<Uuid>>;
}

struct HeedUuidStore {
    env: Env,
    db: Database<Str, ByteSlice>,
}

impl HeedUuidStore {
    fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().join("index_uuids");
        create_dir_all(&path)?;
        let mut options = EnvOpenOptions::new();
        options.map_size(UUID_STORE_SIZE); // 1GB
        let env = options.open(path)?;
        let db = env.create_database(None)?;
        Ok(Self { env, db })
    }

    fn from_snapshot(snapshot: impl AsRef<Path>, path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let src = snapshot.as_ref().join("uuids");
        let dst = path.as_ref().join("uuids");
        compression::from_tar_gz(src, dst)?;
        Self::new(path)
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
            path.push("index_uuids");
            create_dir_all(&path).unwrap();
            path.push("data.mdb");
            env.copy_to_path(path, CompactionOption::Enabled)?;
            Ok(entries)
        })
        .await?
    }
}

use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_stream::stream;
use chrono::{DateTime, Utc};
use futures::pin_mut;
use futures::stream::StreamExt;
use heed::{CompactionOption, EnvOpenOptions};
use log::debug;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::remove_dir_all;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::spawn_blocking;
use uuid::Uuid;

use super::update_handler::UpdateHandler;
use super::{get_arc_ownership_blocking, IndexSettings};
use crate::index::UpdateResult as UResult;
use crate::index::{Document, Index, SearchQuery, SearchResult, Settings};
use crate::index_controller::{
    updates::{Failed, Processed, Processing},
    UpdateMeta,
};
use crate::option::IndexerOpts;
use crate::helpers::compression;

pub type Result<T> = std::result::Result<T, IndexError>;
type AsyncMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
type UpdateResult = std::result::Result<Processed<UpdateMeta, UResult>, Failed<UpdateMeta, String>>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMeta {
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

impl IndexMeta {
    fn new(index: &Index) -> Result<Self> {
        let txn = index.read_txn()?;
        Self::new_txn(index, &txn)
    }

    fn new_txn(index: &Index, txn: &heed::RoTxn) -> Result<Self> {
        let created_at = index.created_at(&txn)?;
        let updated_at = index.updated_at(&txn)?;
        let primary_key = index.primary_key(&txn)?.map(String::from);
        Ok(Self {
            primary_key,
            updated_at,
            created_at,
        })
    }
}

enum IndexMsg {
    CreateIndex {
        uuid: Uuid,
        primary_key: Option<String>,
        ret: oneshot::Sender<Result<IndexMeta>>,
    },
    Update {
        meta: Processing<UpdateMeta>,
        data: std::fs::File,
        ret: oneshot::Sender<Result<UpdateResult>>,
    },
    Search {
        uuid: Uuid,
        query: SearchQuery,
        ret: oneshot::Sender<anyhow::Result<SearchResult>>,
    },
    Settings {
        uuid: Uuid,
        ret: oneshot::Sender<Result<Settings>>,
    },
    Documents {
        uuid: Uuid,
        attributes_to_retrieve: Option<Vec<String>>,
        offset: usize,
        limit: usize,
        ret: oneshot::Sender<Result<Vec<Document>>>,
    },
    Document {
        uuid: Uuid,
        attributes_to_retrieve: Option<Vec<String>>,
        doc_id: String,
        ret: oneshot::Sender<Result<Document>>,
    },
    Delete {
        uuid: Uuid,
        ret: oneshot::Sender<Result<()>>,
    },
    GetMeta {
        uuid: Uuid,
        ret: oneshot::Sender<Result<IndexMeta>>,
    },
    UpdateIndex {
        uuid: Uuid,
        index_settings: IndexSettings,
        ret: oneshot::Sender<Result<IndexMeta>>,
    },
    Snapshot {
        uuid: Uuid,
        path: PathBuf,
        ret: oneshot::Sender<Result<()>>,
    },
}

struct IndexActor<S> {
    read_receiver: Option<mpsc::Receiver<IndexMsg>>,
    write_receiver: Option<mpsc::Receiver<IndexMsg>>,
    update_handler: Arc<UpdateHandler>,
    store: S,
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("error with index: {0}")]
    Error(#[from] anyhow::Error),
    #[error("index already exists")]
    IndexAlreadyExists,
    #[error("Index doesn't exists")]
    UnexistingIndex,
    #[error("Heed error: {0}")]
    HeedError(#[from] heed::Error),
    #[error("Existing primary key")]
    ExistingPrimaryKey,
}

#[async_trait::async_trait]
trait IndexStore {
    async fn create(&self, uuid: Uuid, primary_key: Option<String>) -> Result<Index>;
    async fn get(&self, uuid: Uuid) -> Result<Option<Index>>;
    async fn delete(&self, uuid: Uuid) -> Result<Option<Index>>;
}

impl<S: IndexStore + Sync + Send> IndexActor<S> {
    fn new(
        read_receiver: mpsc::Receiver<IndexMsg>,
        write_receiver: mpsc::Receiver<IndexMsg>,
        store: S,
    ) -> Result<Self> {
        let options = IndexerOpts::default();
        let update_handler = UpdateHandler::new(&options).map_err(IndexError::Error)?;
        let update_handler = Arc::new(update_handler);
        let read_receiver = Some(read_receiver);
        let write_receiver = Some(write_receiver);
        Ok(Self {
            read_receiver,
            write_receiver,
            store,
            update_handler,
        })
    }

    /// `run` poll the write_receiver and read_receiver concurrently, but while messages send
    /// through the read channel are processed concurrently, the messages sent through the write
    /// channel are processed one at a time.
    async fn run(mut self) {
        let mut read_receiver = self
            .read_receiver
            .take()
            .expect("Index Actor must have a inbox at this point.");

        let read_stream = stream! {
            loop {
                match read_receiver.recv().await {
                    Some(msg) => yield msg,
                    None => break,
                }
            }
        };

        let mut write_receiver = self
            .write_receiver
            .take()
            .expect("Index Actor must have a inbox at this point.");

        let write_stream = stream! {
            loop {
                match write_receiver.recv().await {
                    Some(msg) => yield msg,
                    None => break,
                }
            }
        };

        pin_mut!(write_stream);
        pin_mut!(read_stream);

        let fut1 = read_stream.for_each_concurrent(Some(10), |msg| self.handle_message(msg));
        let fut2 = write_stream.for_each_concurrent(Some(1), |msg| self.handle_message(msg));

        let fut1: Box<dyn Future<Output = ()> + Unpin + Send> = Box::new(fut1);
        let fut2: Box<dyn Future<Output = ()> + Unpin + Send> = Box::new(fut2);

        tokio::join!(fut1, fut2);
    }

    async fn handle_message(&self, msg: IndexMsg) {
        use IndexMsg::*;
        match msg {
            CreateIndex {
                uuid,
                primary_key,
                ret,
            } => {
                let _ = ret.send(self.handle_create_index(uuid, primary_key).await);
            }
            Update { ret, meta, data } => {
                let _ = ret.send(self.handle_update(meta, data).await);
            }
            Search { ret, query, uuid } => {
                let _ = ret.send(self.handle_search(uuid, query).await);
            }
            Settings { ret, uuid } => {
                let _ = ret.send(self.handle_settings(uuid).await);
            }
            Documents {
                ret,
                uuid,
                attributes_to_retrieve,
                offset,
                limit,
            } => {
                let _ = ret.send(
                    self.handle_fetch_documents(uuid, offset, limit, attributes_to_retrieve)
                        .await,
                );
            }
            Document {
                uuid,
                attributes_to_retrieve,
                doc_id,
                ret,
            } => {
                let _ = ret.send(
                    self.handle_fetch_document(uuid, doc_id, attributes_to_retrieve)
                        .await,
                );
            }
            Delete { uuid, ret } => {
                let _ = ret.send(self.handle_delete(uuid).await);
            }
            GetMeta { uuid, ret } => {
                let _ = ret.send(self.handle_get_meta(uuid).await);
            }
            UpdateIndex {
                uuid,
                index_settings,
                ret,
            } => {
                let _ = ret.send(self.handle_update_index(uuid, index_settings).await);
            }
            Snapshot { uuid, path, ret } => {
                let _ = ret.send(self.handle_snapshot(uuid, path).await);
            }
        }
    }

    async fn handle_search(&self, uuid: Uuid, query: SearchQuery) -> anyhow::Result<SearchResult> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexError::UnexistingIndex)?;
        spawn_blocking(move || index.perform_search(query)).await?
    }

    async fn handle_create_index(
        &self,
        uuid: Uuid,
        primary_key: Option<String>,
    ) -> Result<IndexMeta> {
        let index = self.store.create(uuid, primary_key).await?;
        let meta = spawn_blocking(move || IndexMeta::new(&index))
            .await
            .map_err(|e| IndexError::Error(e.into()))??;
        Ok(meta)
    }

    async fn handle_update(
        &self,
        meta: Processing<UpdateMeta>,
        data: File,
    ) -> Result<UpdateResult> {
        log::info!("Processing update {}", meta.id());
        let uuid = meta.index_uuid();
        let update_handler = self.update_handler.clone();
        let index = match self.store.get(*uuid).await? {
            Some(index) => index,
            None => self.store.create(*uuid, None).await?,
        };
        spawn_blocking(move || update_handler.handle_update(meta, data, index))
            .await
            .map_err(|e| IndexError::Error(e.into()))
    }

    async fn handle_settings(&self, uuid: Uuid) -> Result<Settings> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexError::UnexistingIndex)?;
        spawn_blocking(move || index.settings().map_err(IndexError::Error))
            .await
            .map_err(|e| IndexError::Error(e.into()))?
    }

    async fn handle_fetch_documents(
        &self,
        uuid: Uuid,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Vec<Document>> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexError::UnexistingIndex)?;
        spawn_blocking(move || {
            index
                .retrieve_documents(offset, limit, attributes_to_retrieve)
                .map_err(IndexError::Error)
        })
        .await
        .map_err(|e| IndexError::Error(e.into()))?
    }

    async fn handle_fetch_document(
        &self,
        uuid: Uuid,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Document> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexError::UnexistingIndex)?;
        spawn_blocking(move || {
            index
                .retrieve_document(doc_id, attributes_to_retrieve)
                .map_err(IndexError::Error)
        })
        .await
        .map_err(|e| IndexError::Error(e.into()))?
    }

    async fn handle_delete(&self, uuid: Uuid) -> Result<()> {
        let index = self.store.delete(uuid).await?;

        if let Some(index) = index {
            tokio::task::spawn(async move {
                let index = index.0;
                let store = get_arc_ownership_blocking(index).await;
                spawn_blocking(move || {
                    store.prepare_for_closing().wait();
                    debug!("Index closed");
                });
            });
        }

        Ok(())
    }

    async fn handle_get_meta(&self, uuid: Uuid) -> Result<IndexMeta> {
        match self.store.get(uuid).await? {
            Some(index) => {
                let meta = spawn_blocking(move || IndexMeta::new(&index))
                    .await
                    .map_err(|e| IndexError::Error(e.into()))??;
                Ok(meta)
            }
            None => Err(IndexError::UnexistingIndex),
        }
    }

    async fn handle_update_index(
        &self,
        uuid: Uuid,
        index_settings: IndexSettings,
    ) -> Result<IndexMeta> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexError::UnexistingIndex)?;

        spawn_blocking(move || match index_settings.primary_key {
            Some(ref primary_key) => {
                let mut txn = index.write_txn()?;
                if index.primary_key(&txn)?.is_some() {
                    return Err(IndexError::ExistingPrimaryKey);
                }
                index.put_primary_key(&mut txn, primary_key)?;
                let meta = IndexMeta::new_txn(&index, &txn)?;
                txn.commit()?;
                Ok(meta)
            }
            None => {
                let meta = IndexMeta::new(&index)?;
                Ok(meta)
            }
        })
        .await
        .map_err(|e| IndexError::Error(e.into()))?
    }

    async fn handle_snapshot(&self, uuid: Uuid, mut path: PathBuf) -> Result<()> {
        use tokio::fs::create_dir_all;

        path.push("indexes");
        println!("performing index snapshot in {:?}", path);
        create_dir_all(&path)
            .await
            .map_err(|e| IndexError::Error(e.into()))?;

        if let Some(index) = self.store.get(uuid).await? {
            let mut index_path = path.join(format!("index-{}", uuid));
            create_dir_all(&index_path)
                .await
                .map_err(|e| IndexError::Error(e.into()))?;
            index_path.push("data.mdb");
            spawn_blocking(move || -> anyhow::Result<()> {
                // Get write txn to wait for ongoing write transaction before snapshot.
                let _txn = index.write_txn()?;
                index
                    .env
                    .copy_to_path(index_path, CompactionOption::Enabled)?;
                Ok(())
            });
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct IndexActorHandle {
    read_sender: mpsc::Sender<IndexMsg>,
    write_sender: mpsc::Sender<IndexMsg>,
}

impl IndexActorHandle {
    pub fn new(path: impl AsRef<Path>, index_size: usize) -> anyhow::Result<Self> {
        let (read_sender, read_receiver) = mpsc::channel(100);
        let (write_sender, write_receiver) = mpsc::channel(100);

        let store = HeedIndexStore::new(path, index_size);
        let actor = IndexActor::new(read_receiver, write_receiver, store)?;
        tokio::task::spawn(actor.run());
        Ok(Self {
            read_sender,
            write_sender,
        })
    }

    pub async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMeta> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::CreateIndex {
            ret,
            uuid,
            primary_key,
        };
        let _ = self.read_sender.send(msg).await;
        receiver.await.expect("IndexActor has been killed")
    }

    pub async fn update(
        &self,
        meta: Processing<UpdateMeta>,
        data: std::fs::File,
    ) -> anyhow::Result<UpdateResult> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Update { ret, meta, data };
        let _ = self.write_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    pub async fn search(&self, uuid: Uuid, query: SearchQuery) -> Result<SearchResult> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Search { uuid, query, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    pub async fn settings(&self, uuid: Uuid) -> Result<Settings> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Settings { uuid, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    pub async fn documents(
        &self,
        uuid: Uuid,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Vec<Document>> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Documents {
            uuid,
            ret,
            offset,
            attributes_to_retrieve,
            limit,
        };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    pub async fn document(
        &self,
        uuid: Uuid,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Document> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Document {
            uuid,
            ret,
            doc_id,
            attributes_to_retrieve,
        };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    pub async fn delete(&self, uuid: Uuid) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Delete { uuid, ret };
        let _ = self.write_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    pub async fn get_index_meta(&self, uuid: Uuid) -> Result<IndexMeta> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::GetMeta { uuid, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    pub async fn update_index(
        &self,
        uuid: Uuid,
        index_settings: IndexSettings,
    ) -> Result<IndexMeta> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::UpdateIndex {
            uuid,
            index_settings,
            ret,
        };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    pub async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Snapshot { uuid, path, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }
}

struct HeedIndexStore {
    index_store: AsyncMap<Uuid, Index>,
    path: PathBuf,
    index_size: usize,
}

impl HeedIndexStore {
    fn new(path: impl AsRef<Path>, index_size: usize) -> Self {
        let path = path.as_ref().join("indexes/");
        let index_store = Arc::new(RwLock::new(HashMap::new()));
        Self {
            index_store,
            path,
            index_size,
        }
    }
}

#[async_trait::async_trait]
impl IndexStore for HeedIndexStore {
    async fn create(&self, uuid: Uuid, primary_key: Option<String>) -> Result<Index> {
        let path = self.path.join(format!("index-{}", uuid));
        if path.exists() {
            return Err(IndexError::IndexAlreadyExists);
        }

        let index_size = self.index_size;
        let index = spawn_blocking(move || -> Result<Index> {
            let index = open_index(&path, index_size)?;
            if let Some(primary_key) = primary_key {
                let mut txn = index.write_txn()?;
                index.put_primary_key(&mut txn, &primary_key)?;
                txn.commit()?;
            }
            Ok(index)
        })
        .await
        .map_err(|e| IndexError::Error(e.into()))??;

        self.index_store.write().await.insert(uuid, index.clone());

        Ok(index)
    }

    async fn get(&self, uuid: Uuid) -> Result<Option<Index>> {
        let guard = self.index_store.read().await;
        match guard.get(&uuid) {
            Some(index) => Ok(Some(index.clone())),
            None => {
                // drop the guard here so we can perform the write after without deadlocking;
                drop(guard);
                let path = self.path.join(format!("index-{}", uuid));
                if !path.exists() {
                    return Ok(None);
                }

                let index_size = self.index_size;
                let index = spawn_blocking(move || open_index(path, index_size))
                    .await
                    .map_err(|e| IndexError::Error(e.into()))??;
                self.index_store.write().await.insert(uuid, index.clone());
                Ok(Some(index))
            }
        }
    }

    async fn delete(&self, uuid: Uuid) -> Result<Option<Index>> {
        let db_path = self.path.join(format!("index-{}", uuid));
        remove_dir_all(db_path)
            .await
            .map_err(|e| IndexError::Error(e.into()))?;
        let index = self.index_store.write().await.remove(&uuid);
        Ok(index)
    }
}

fn open_index(path: impl AsRef<Path>, size: usize) -> Result<Index> {
    create_dir_all(&path).map_err(|e| IndexError::Error(e.into()))?;
    let mut options = EnvOpenOptions::new();
    options.map_size(size);
    let index = milli::Index::new(options, &path).map_err(IndexError::Error)?;
    Ok(Index(Arc::new(index)))
}

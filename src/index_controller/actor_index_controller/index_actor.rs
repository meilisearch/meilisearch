use std::fs::{File, create_dir_all};
use std::path::{PathBuf, Path};
use std::sync::Arc;

use chrono::Utc;
use heed::EnvOpenOptions;
use milli::Index;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;
use log::info;

use super::update_handler::UpdateHandler;
use crate::index_controller::{IndexMetadata, UpdateMeta, updates::{Processed, Failed, Processing}, UpdateResult as UResult};
use crate::option::IndexerOpts;

pub type Result<T> = std::result::Result<T, IndexError>;
type AsyncMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
type UpdateResult = std::result::Result<Processed<UpdateMeta, UResult>, Failed<UpdateMeta, String>>;

enum IndexMsg {
    CreateIndex { uuid: Uuid, primary_key: Option<String>, ret: oneshot::Sender<Result<IndexMetadata>> },
    Update { meta: Processing<UpdateMeta>, data: std::fs::File, ret:  oneshot::Sender<UpdateResult>},
}

struct IndexActor<S> {
    inbox: mpsc::Receiver<IndexMsg>,
    update_handler: Arc<UpdateHandler>,
    store: S,
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("error with index: {0}")]
    Error(#[from] anyhow::Error),
    #[error("index already exists")]
    IndexAlreadyExists,
}

#[async_trait::async_trait]
trait IndexStore {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMetadata>;
    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<Index>>;
}

impl<S: IndexStore + Sync + Send> IndexActor<S> {
    fn new(inbox: mpsc::Receiver<IndexMsg>, store: S) -> Self {
        let options = IndexerOpts::default();
        let update_handler = UpdateHandler::new(&options).unwrap();
        let update_handler = Arc::new(update_handler);
        Self { inbox, store, update_handler }
    }

    async fn run(mut self) {
        loop {
            match self.inbox.recv().await {
                Some(IndexMsg::CreateIndex { uuid, primary_key, ret }) => self.handle_create_index(uuid, primary_key, ret).await,
                Some(IndexMsg::Update { ret, meta, data }) => self.handle_update(meta, data, ret).await,
                None => break,
            }
        }
    }

    async fn handle_create_index(&self, uuid: Uuid, primary_key: Option<String>, ret: oneshot::Sender<Result<IndexMetadata>>) {
        let result = self.store.create_index(uuid, primary_key).await;
        let _ = ret.send(result);
    }

    async fn handle_update(&self, meta: Processing<UpdateMeta>, data: File, ret: oneshot::Sender<UpdateResult>) {
        info!("processing update");
        let uuid = meta.index_uuid().clone();
        let index = self.store.get_or_create(uuid).await.unwrap();
        let update_handler = self.update_handler.clone();
        let result = tokio::task::spawn_blocking(move || update_handler.handle_update(meta, data, index.as_ref())).await;
        let result = result.unwrap();
        let _ = ret.send(result);
    }
}

#[derive(Clone)]
pub struct IndexActorHandle {
    sender: mpsc::Sender<IndexMsg>,
}

impl IndexActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);

        let store = MapIndexStore::new("data.ms");
        let actor = IndexActor::new(receiver, store);
        tokio::task::spawn(actor.run());
        Self { sender }
    }

    pub async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMetadata> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::CreateIndex { ret, uuid, primary_key };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("IndexActor has been killed")
    }

    pub async fn update(&self, meta: Processing<UpdateMeta>, data: std::fs::File) -> UpdateResult {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Update { ret, meta, data };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("IndexActor has been killed")
    }
}

struct MapIndexStore {
    root: PathBuf,
    meta_store: AsyncMap<Uuid, IndexMetadata>,
    index_store: AsyncMap<Uuid, Arc<Index>>,
}

#[async_trait::async_trait]
impl IndexStore for MapIndexStore {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMetadata> {
        let meta = match self.meta_store.write().await.entry(uuid.clone()) {
            Entry::Vacant(entry) => {
                let meta = IndexMetadata {
                    uuid,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    primary_key,
                };
                entry.insert(meta).clone()
            }
            Entry::Occupied(_) => return Err(IndexError::IndexAlreadyExists),
        };

        let db_path = self.root.join(format!("index-{}", meta.uuid));

        let index: Result<Index> = tokio::task::spawn_blocking(move || {
            create_dir_all(&db_path).expect("can't create db");
            let mut options = EnvOpenOptions::new();
            options.map_size(4096 * 100_000);
            let index = Index::new(options, &db_path)
                .map_err(|e| IndexError::Error(e))?;
            Ok(index)
        }).await.expect("thread died");

        self.index_store.write().await.insert(meta.uuid.clone(), Arc::new(index?));

        Ok(meta)
    }

    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<Index>> {
        match self.index_store.write().await.entry(uuid.clone()) {
            Entry::Vacant(entry) => {
                match self.meta_store.write().await.entry(uuid.clone()) {
                    Entry::Vacant(_) => {
                        todo!()
                    }
                    Entry::Occupied(entry) => {
                        todo!()
                    }
                }
            }
            Entry::Occupied(entry) => Ok(entry.get().clone()),
        }
    }
}

impl MapIndexStore {
    fn new(root: impl AsRef<Path>) -> Self {
        let mut root = root.as_ref().to_owned();
        root.push("indexes/");
        let meta_store = Arc::new(RwLock::new(HashMap::new()));
        let index_store = Arc::new(RwLock::new(HashMap::new()));
        Self { meta_store, index_store, root }
    }
}

use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::collections::{HashMap, hash_map::Entry};

use super::index_actor::IndexActorHandle;
use log::info;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;

use crate::index::UpdateResult;
use crate::index_controller::{UpdateMeta, UpdateStatus};

pub type Result<T> = std::result::Result<T, UpdateError>;
type UpdateStore = super::update_store::UpdateStore<UpdateMeta, UpdateResult, String>;
type PayloadData<D> = std::result::Result<D, Box<dyn std::error::Error + Sync + Send + 'static>>;

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("error with update: {0}")]
    Error(Box<dyn std::error::Error + Sync + Send + 'static>),
}

enum UpdateMsg<D> {
    Update {
        uuid: Uuid,
        meta: UpdateMeta,
        data: mpsc::Receiver<PayloadData<D>>,
        ret: oneshot::Sender<Result<UpdateStatus>>,
    },
    ListUpdates {
        uuid: Uuid,
        ret: oneshot::Sender<Result<Vec<UpdateStatus>>>,
    },
}

struct UpdateActor<D, S> {
    path: PathBuf,
    store: S,
    inbox: mpsc::Receiver<UpdateMsg<D>>,
}

#[async_trait::async_trait]
trait UpdateStoreStore {
    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<UpdateStore>>;
}

impl<D, S> UpdateActor<D, S>
where
    D: AsRef<[u8]> + Sized + 'static,
    S: UpdateStoreStore,
{
    fn new(
        store: S,
        inbox: mpsc::Receiver<UpdateMsg<D>>,
        path: impl AsRef<Path>,
    ) -> Self {
        let path = path.as_ref().to_owned().join("update_files");
        create_dir_all(&path).unwrap();
        Self {
            store,
            inbox,
            path,
        }
    }

    async fn run(mut self) {
        use UpdateMsg::*;

        info!("started update actor.");

        loop {
            match self.inbox.recv().await {
                Some(Update {
                    uuid,
                    meta,
                    data,
                    ret,
                }) => self.handle_update(uuid, meta, data, ret).await,
                Some(ListUpdates { uuid, ret }) => self.handle_list_updates(uuid, ret).await,
                None => {}
            }
        }
    }

    async fn handle_update(
        &self,
        uuid: Uuid,
        meta: UpdateMeta,
        mut payload: mpsc::Receiver<PayloadData<D>>,
        ret: oneshot::Sender<Result<UpdateStatus>>,
    ) {
        let update_store = self.store.get_or_create(uuid).await.unwrap();
        let update_file_id = uuid::Uuid::new_v4();
        let path = self.path.join(format!("update_{}", update_file_id));
        let mut file = File::create(&path).await.unwrap();

        while let Some(bytes) = payload.recv().await {
            match bytes {
                Ok(bytes) => {
                    file.write_all(bytes.as_ref()).await;
                }
                Err(e) => {
                    ret.send(Err(UpdateError::Error(e)));
                    return;
                }
            }
        }

        file.flush().await;

        let file = file.into_std().await;

        let result = tokio::task::spawn_blocking(move || {
            let result = update_store
                .register_update(meta, path, uuid)
                .map(|pending| UpdateStatus::Pending(pending))
                .map_err(|e| UpdateError::Error(Box::new(e)));
            let _ = ret.send(result);
        })
        .await;
    }

    async fn handle_list_updates(
        &self,
        uuid: Uuid,
        ret: oneshot::Sender<Result<Vec<UpdateStatus>>>,
    ) {
        todo!()
    }
}

#[derive(Clone)]
pub struct UpdateActorHandle<D> {
    sender: mpsc::Sender<UpdateMsg<D>>,
}

impl<D> UpdateActorHandle<D>
where
    D: AsRef<[u8]> + Sized + 'static + Sync + Send,
{
    pub fn new(index_handle: IndexActorHandle, path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_owned().join("updates");
        let (sender, receiver) = mpsc::channel(100);
        let store = MapUpdateStoreStore::new(index_handle, &path);
        let actor = UpdateActor::new(store, receiver, path);

        tokio::task::spawn(actor.run());

        Self { sender }
    }

    pub async fn update(
        &self,
        meta: UpdateMeta,
        data: mpsc::Receiver<PayloadData<D>>,
        uuid: Uuid,
    ) -> Result<UpdateStatus> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Update {
            uuid,
            data,
            meta,
            ret,
        };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("update actor killed.")
    }
}

struct MapUpdateStoreStore {
    db: Arc<RwLock<HashMap<Uuid, Arc<UpdateStore>>>>,
    index_handle: IndexActorHandle,
    path: PathBuf,
}

impl MapUpdateStoreStore {
    fn new(index_handle: IndexActorHandle, path: impl AsRef<Path>) -> Self {
        let db = Arc::new(RwLock::new(HashMap::new()));
        let path = path.as_ref().to_owned();
        Self { db, index_handle, path }
    }
}

#[async_trait::async_trait]
impl UpdateStoreStore for MapUpdateStoreStore {
    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<UpdateStore>> {
        match self.db.write().await.entry(uuid) {
            Entry::Vacant(e) => {
                let mut options = heed::EnvOpenOptions::new();
                options.map_size(4096 * 100_000);
                let path = self.path.clone().join(format!("updates-{}", e.key()));
                create_dir_all(&path).unwrap();
                let index_handle = self.index_handle.clone();
                let store = UpdateStore::open(options, &path, move |meta, file| {
                    futures::executor::block_on(index_handle.update(meta, file))
                }).unwrap();
                let store = e.insert(store);
                Ok(store.clone())
            }
            Entry::Occupied(e) => {
                Ok(e.get().clone())
            }
        }
    }
}

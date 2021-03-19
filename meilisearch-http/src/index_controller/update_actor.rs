use std::collections::{hash_map::Entry, HashMap};
use std::io::SeekFrom;
use std::fs::{create_dir_all, remove_dir_all};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::info;
use oxidized_json_checker::JsonChecker;
use super::index_actor::IndexActorHandle;
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, AsyncSeekExt};
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;

use super::get_arc_ownership_blocking;
use crate::index::UpdateResult;
use crate::index_controller::{UpdateMeta, UpdateStatus};

pub type Result<T> = std::result::Result<T, UpdateError>;
type UpdateStore = super::update_store::UpdateStore<UpdateMeta, UpdateResult, String>;
type PayloadData<D> = std::result::Result<D, Box<dyn std::error::Error + Sync + Send + 'static>>;

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("error with update: {0}")]
    Error(Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Index {0} doesn't exist.")]
    UnexistingIndex(Uuid),
    #[error("Update {0} doesn't exist.")]
    UnexistingUpdate(u64),
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
    GetUpdate {
        uuid: Uuid,
        ret: oneshot::Sender<Result<UpdateStatus>>,
        id: u64,
    },
    Delete {
        uuid: Uuid,
        ret: oneshot::Sender<Result<()>>,
    },
    Create {
        uuid: Uuid,
        ret: oneshot::Sender<Result<()>>,
    },
    Snapshot {
        uuids: Vec<Uuid>,
        path: PathBuf,
        ret: oneshot::Sender<Result<()>>,
    }
}

struct UpdateActor<D, S> {
    path: PathBuf,
    store: S,
    inbox: mpsc::Receiver<UpdateMsg<D>>,
}

#[async_trait::async_trait]
trait UpdateStoreStore {
    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<UpdateStore>>;
    async fn delete(&self, uuid: Uuid) -> Result<Option<Arc<UpdateStore>>>;
    async fn get(&self, uuid: Uuid) -> Result<Option<Arc<UpdateStore>>>;
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
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_owned().join("update_files");
        create_dir_all(&path)?;
        assert!(path.exists());
        Ok(Self { store, inbox, path })
    }

    async fn run(mut self) {
        use UpdateMsg::*;

        info!("Started update actor.");

        loop {
            match self.inbox.recv().await {
                Some(Update {
                    uuid,
                    meta,
                    data,
                    ret,
                }) => {
                    let _ = ret.send(self.handle_update(uuid, meta, data).await);
                }
                Some(ListUpdates { uuid, ret }) => {
                    let _ = ret.send(self.handle_list_updates(uuid).await);
                }
                Some(GetUpdate { uuid, ret, id }) => {
                    let _ = ret.send(self.handle_get_update(uuid, id).await);
                }
                Some(Delete { uuid, ret }) => {
                    let _ = ret.send(self.handle_delete(uuid).await);
                }
                Some(Create { uuid, ret }) => {
                    let _ = ret.send(self.handle_create(uuid).await);
                }
                Some(Snapshot { uuids, path, ret }) => {
                    let _ = ret.send(self.handle_snapshot(uuids, path).await);
                }
                None => break,
            }
        }
    }

    async fn handle_update(
        &self,
        uuid: Uuid,
        meta: UpdateMeta,
        mut payload: mpsc::Receiver<PayloadData<D>>,
    ) -> Result<UpdateStatus> {
        let update_store = self.store.get_or_create(uuid).await?;
        let update_file_id = uuid::Uuid::new_v4();
        let path = self.path.join(format!("update_{}", update_file_id));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .map_err(|e| UpdateError::Error(Box::new(e)))?;

        while let Some(bytes) = payload.recv().await {
            match bytes {
                Ok(bytes) => {
                    file.write_all(bytes.as_ref())
                        .await
                        .map_err(|e| UpdateError::Error(Box::new(e)))?;
                }
                Err(e) => {
                    return Err(UpdateError::Error(e));
                }
            }
        }

        file.flush()
            .await
            .map_err(|e| UpdateError::Error(Box::new(e)))?;

        file.seek(SeekFrom::Start(0))
            .await
            .map_err(|e| UpdateError::Error(Box::new(e)))?;

        let mut file = file.into_std().await;

        tokio::task::spawn_blocking(move || {
            use std::io::{BufReader, sink, copy, Seek};

            // If the payload is empty, ignore the check.
            if file.metadata().map_err(|e| UpdateError::Error(Box::new(e)))?.len() > 0 {
                // Check that the json payload is valid:
                let reader = BufReader::new(&mut file);
                let mut checker = JsonChecker::new(reader);

                if copy(&mut checker, &mut sink()).is_err() || checker.finish().is_err() {
                    // The json file is invalid, we use Serde to get a nice error message:
                    file.seek(SeekFrom::Start(0))
                        .map_err(|e| UpdateError::Error(Box::new(e)))?;
                    let _: serde_json::Value = serde_json::from_reader(file)
                        .map_err(|e| UpdateError::Error(Box::new(e)))?;
                }
            }

            // The payload is valid, we can register it to the update store.
            update_store
                .register_update(meta, path, uuid)
                .map(UpdateStatus::Pending)
                .map_err(|e| UpdateError::Error(Box::new(e)))
        })
        .await
        .map_err(|e| UpdateError::Error(Box::new(e)))?
    }

    async fn handle_list_updates(&self, uuid: Uuid) -> Result<Vec<UpdateStatus>> {
        let update_store = self.store.get(uuid).await?;
        tokio::task::spawn_blocking(move || {
            let result = update_store
                .ok_or(UpdateError::UnexistingIndex(uuid))?
                .list()
                .map_err(|e| UpdateError::Error(e.into()))?;
            Ok(result)
        })
        .await
        .map_err(|e| UpdateError::Error(Box::new(e)))?
    }

    async fn handle_get_update(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus> {
        let store = self
            .store
            .get(uuid)
            .await?
            .ok_or(UpdateError::UnexistingIndex(uuid))?;
        let result = store
            .meta(id)
            .map_err(|e| UpdateError::Error(Box::new(e)))?
            .ok_or(UpdateError::UnexistingUpdate(id))?;
        Ok(result)
    }

    async fn handle_delete(&self, uuid: Uuid) -> Result<()> {
        let store = self.store.delete(uuid).await?;

        if let Some(store) = store {
            tokio::task::spawn(async move {
                let store = get_arc_ownership_blocking(store).await;
                tokio::task::spawn_blocking(move || {
                    store.prepare_for_closing().wait();
                    info!("Update store {} was closed.", uuid);
                });
            });
        }

        Ok(())
    }

    async fn handle_create(&self, uuid: Uuid) -> Result<()> {
        let _ = self.store.get_or_create(uuid).await?;
        Ok(())
    }

    async fn handle_snapshot(&self, uuids: Vec<Uuid>, path: PathBuf) -> Result<()> {
        use tokio::time;
        use std::time::Duration;

        println!("performing update snapshot");
        time::sleep(Duration::from_secs(2)).await;
        println!("Update snapshot done");
        Ok(())
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
    pub fn new(
        index_handle: IndexActorHandle,
        path: impl AsRef<Path>,
        update_store_size: usize,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_owned().join("updates");
        let (sender, receiver) = mpsc::channel(100);
        let store = MapUpdateStoreStore::new(index_handle, &path, update_store_size);
        let actor = UpdateActor::new(store, receiver, path)?;

        tokio::task::spawn(actor.run());

        Ok(Self { sender })
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

impl<D> UpdateActorHandle<D> {
    pub async fn get_all_updates_status(&self, uuid: Uuid) -> Result<Vec<UpdateStatus>> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::ListUpdates { uuid, ret };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("update actor killed.")
    }

    pub async fn update_status(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::GetUpdate { uuid, id, ret };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("update actor killed.")
    }

    pub async fn delete(&self, uuid: Uuid) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Delete { uuid, ret };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("update actor killed.")
    }

    pub async fn create(&self, uuid: Uuid) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Create { uuid, ret };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("update actor killed.")
    }

    pub async fn snapshot(&self, uuids: Vec<Uuid>, path: PathBuf) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Snapshot { uuids, path, ret };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("update actor killed.")
    }
}

struct MapUpdateStoreStore {
    db: Arc<RwLock<HashMap<Uuid, Arc<UpdateStore>>>>,
    index_handle: IndexActorHandle,
    path: PathBuf,
    update_store_size: usize,
}

impl MapUpdateStoreStore {
    fn new(
        index_handle: IndexActorHandle,
        path: impl AsRef<Path>,
        update_store_size: usize,
    ) -> Self {
        let db = Arc::new(RwLock::new(HashMap::new()));
        let path = path.as_ref().to_owned();
        Self {
            db,
            index_handle,
            path,
            update_store_size,
        }
    }
}

#[async_trait::async_trait]
impl UpdateStoreStore for MapUpdateStoreStore {
    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<UpdateStore>> {
        match self.db.write().await.entry(uuid) {
            Entry::Vacant(e) => {
                let mut options = heed::EnvOpenOptions::new();
                let update_store_size = self.update_store_size;
                options.map_size(update_store_size);
                let path = self.path.clone().join(format!("updates-{}", e.key()));
                create_dir_all(&path).unwrap();
                let index_handle = self.index_handle.clone();
                let store = UpdateStore::open(options, &path, move |meta, file| {
                    futures::executor::block_on(index_handle.update(meta, file))
                })
                .map_err(|e| UpdateError::Error(e.into()))?;
                let store = e.insert(store);
                Ok(store.clone())
            }
            Entry::Occupied(e) => Ok(e.get().clone()),
        }
    }

    async fn get(&self, uuid: Uuid) -> Result<Option<Arc<UpdateStore>>> {
        let guard = self.db.read().await;
        match guard.get(&uuid) {
            Some(uuid) => Ok(Some(uuid.clone())),
            None => {
                // The index is not found in the found in the loaded indexes, so we attempt to load
                // it from disk. We need to acquire a write lock **before** attempting to open the
                // index, because someone could be trying to open it at the same time as us.
                drop(guard);
                let path = self.path.clone().join(format!("updates-{}", uuid));
                if path.exists() {
                    let mut guard = self.db.write().await;
                    match guard.entry(uuid) {
                        Entry::Vacant(entry) => {
                            // We can safely load the index
                            let index_handle = self.index_handle.clone();
                            let mut options = heed::EnvOpenOptions::new();
                            let update_store_size = self.update_store_size;
                            options.map_size(update_store_size);
                            let store = UpdateStore::open(options, &path, move |meta, file| {
                                futures::executor::block_on(index_handle.update(meta, file))
                            })
                            .map_err(|e| UpdateError::Error(e.into()))?;
                            let store = entry.insert(store);
                            Ok(Some(store.clone()))
                        }
                        Entry::Occupied(entry) => {
                            // The index was loaded while we attempted to to iter
                            Ok(Some(entry.get().clone()))
                        }
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn delete(&self, uuid: Uuid) -> Result<Option<Arc<UpdateStore>>> {
        let store = self.db.write().await.remove(&uuid);
        let path = self.path.clone().join(format!("updates-{}", uuid));
        if store.is_some() || path.exists() {
            remove_dir_all(path).unwrap();
        }
        Ok(store)
    }
}

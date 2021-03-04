use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::info;
use super::index_actor::IndexActorHandle;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::index_controller::{UpdateMeta, UpdateStatus};
use crate::index::UpdateResult;

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
        ret: oneshot::Sender<Result<UpdateStatus>>
    },
    ListUpdates {
        uuid: Uuid,
        ret: oneshot::Sender<Result<Vec<UpdateStatus>>>,
    }
}

struct UpdateActor<D> {
    path: PathBuf,
    store: Arc<UpdateStore>,
    inbox: mpsc::Receiver<UpdateMsg<D>>,
    index_handle: IndexActorHandle,
}

impl<D> UpdateActor<D>
where D: AsRef<[u8]> + Sized + 'static,
{
    fn new(
        store: Arc<UpdateStore>,
        inbox: mpsc::Receiver<UpdateMsg<D>>,
        index_handle: IndexActorHandle,
        path: impl AsRef<Path>,
        ) -> Self {
        let path = path.as_ref().to_owned().join("update_files");
        create_dir_all(&path).unwrap();
        Self { store, inbox, index_handle, path }
    }

    async fn run(mut self) {
        use UpdateMsg::*;

        info!("started update actor.");

        loop {
            match self.inbox.recv().await {
                Some(Update { uuid, meta, data, ret }) => self.handle_update(uuid, meta, data, ret).await,
                Some(ListUpdates { uuid, ret }) => self.handle_list_updates(uuid, ret).await,
                None => {}
            }
        }
    }

    async fn handle_update(&self, uuid: Uuid, meta: UpdateMeta, mut payload: mpsc::Receiver<PayloadData<D>>, ret: oneshot::Sender<Result<UpdateStatus>>) {
        let store = self.store.clone();
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
                    return
                }
            }
        }

        file.flush().await;

        let file = file.into_std().await;

        let result = tokio::task::spawn_blocking(move || {
            let result = store
                .register_update(meta, path, uuid)
                .map(|pending| UpdateStatus::Pending(pending))
                .map_err(|e| UpdateError::Error(Box::new(e)));
            let _ = ret.send(result);
        }).await;
    }

    async fn handle_list_updates(&self, uuid: Uuid, ret: oneshot::Sender<Result<Vec<UpdateStatus>>>) {
        todo!()
    }
}

#[derive(Clone)]
pub struct UpdateActorHandle<D> {
    sender: mpsc::Sender<UpdateMsg<D>>,
}

impl<D> UpdateActorHandle<D>
where D: AsRef<[u8]> + Sized + 'static,
{
    pub fn new(index_handle: IndexActorHandle, path: impl AsRef<Path>) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let mut options = heed::EnvOpenOptions::new();
        options.map_size(4096 * 100_000);

        let path = path
            .as_ref()
            .to_owned()
            .join("updates");

        create_dir_all(&path).unwrap();
        let index_handle_clone = index_handle.clone();
        let store = UpdateStore::open(options, &path, move |meta, file| {
            futures::executor::block_on(index_handle_clone.update(meta, file))
        }).unwrap();
        let actor = UpdateActor::new(store, receiver, index_handle, path);
        tokio::task::spawn_local(actor.run());
        Self { sender }
    }

    pub async fn update(&self, meta: UpdateMeta, data: mpsc::Receiver<PayloadData<D>>, uuid: Uuid) -> Result<UpdateStatus> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Update {
            uuid,
            data,
            meta,
            ret,
        };
        let _ =  self.sender.send(msg).await;
        receiver.await.expect("update actor killed.")
    }
}

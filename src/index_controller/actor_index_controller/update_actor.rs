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

use crate::index_controller::{UpdateMeta, UpdateStatus, UpdateResult, updates::Pending};

pub type Result<T> = std::result::Result<T, UpdateError>;
type UpdateStore = super::update_store::UpdateStore<UpdateMeta, UpdateResult, String>;

#[derive(Debug, Error)]
pub enum UpdateError {}

enum UpdateMsg<D> {
    CreateIndex{
        uuid: Uuid,
        ret: oneshot::Sender<Result<()>>,
    },
    Update {
        uuid: Uuid,
        meta: UpdateMeta,
        data: mpsc::Receiver<D>,
        ret: oneshot::Sender<Result<UpdateStatus>>
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
        info!("started update actor.");

        loop {
            match self.inbox.recv().await {
                Some(UpdateMsg::Update { uuid, meta, data, ret }) => self.handle_update(uuid, meta, data, ret).await,
                Some(_) => {}
                None => {}
            }
        }
    }

    async fn handle_update(&self, uuid: Uuid, meta: UpdateMeta, mut payload: mpsc::Receiver<D>, ret: oneshot::Sender<Result<UpdateStatus>>) {
        let store = self.store.clone();
        let update_file_id = uuid::Uuid::new_v4();
        let path = self.path.join(format!("update_{}", update_file_id));
        let mut file = File::create(&path).await.unwrap();

        while let Some(bytes) = payload.recv().await {
            file.write_all(bytes.as_ref()).await;
        }

        file.flush().await;

        let file = file.into_std().await;

        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Pending<UpdateMeta>> {
            Ok(store.register_update(meta, path, uuid)?)
        }).await.unwrap().unwrap();
        let _ = ret.send(Ok(UpdateStatus::Pending(result)));
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
        let mut path = PathBuf::new();
        path.push("data.ms");
        path.push("updates");
        create_dir_all(&path).unwrap();
        let index_handle_clone = index_handle.clone();
        let store = UpdateStore::open(options, &path, move |meta, file| {
            futures::executor::block_on(index_handle_clone.update(meta, file))
        }).unwrap();
        let actor = UpdateActor::new(store, receiver, index_handle, path);
        tokio::task::spawn_local(actor.run());
        Self { sender }
    }

    pub async fn update(&self, meta: UpdateMeta, data: mpsc::Receiver<D>, uuid: Uuid) -> Result<UpdateStatus> {
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

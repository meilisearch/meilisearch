use super::index_actor::IndexActorHandle;
use uuid::Uuid;
use tokio::sync::{mpsc, oneshot};
use crate::index_controller::{UpdateMeta, UpdateStatus, UpdateResult};
use thiserror::Error;
use tokio::io::AsyncReadExt;
use log::info;
use tokio::fs::File;
use std::path::PathBuf;
use std::fs::create_dir_all;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, UpdateError>;
type UpdateStore = super::update_store::UpdateStore<UpdateMeta, UpdateResult, String>;

#[derive(Debug, Error)]
pub enum UpdateError {}

enum UpdateMsg {
    CreateIndex{
        uuid: Uuid,
        ret: oneshot::Sender<Result<()>>,
    },
    Update {
        uuid: Uuid,
        meta: UpdateMeta,
        payload: Option<File>,
        ret: oneshot::Sender<Result<UpdateStatus>>
    }
}

struct UpdateActor {
    store: Arc<UpdateStore>,
    inbox: mpsc::Receiver<UpdateMsg>,
    index_handle: IndexActorHandle,
}

impl UpdateActor {
    fn new(store: Arc<UpdateStore>, inbox: mpsc::Receiver<UpdateMsg>, index_handle: IndexActorHandle) -> Self {
        Self { store, inbox, index_handle }
    }

    async fn run(mut self) {

        info!("started update actor.");

        loop {
            match self.inbox.recv().await {
                Some(UpdateMsg::Update { uuid, meta, payload, ret }) => self.handle_update(uuid, meta, payload, ret).await,
                Some(_) => {}
                None => {}
            }
        }
    }

    async fn handle_update(&self, _uuid: Uuid, meta: UpdateMeta, payload: Option<File>, ret: oneshot::Sender<Result<UpdateStatus>>) {
        let mut buf = Vec::new();
        let mut payload = payload.unwrap();
        payload.read_to_end(&mut buf).await.unwrap();
        let result = self.store.register_update(meta, &buf).unwrap();
        let _ = ret.send(Ok(UpdateStatus::Pending(result)));
    }
}

#[derive(Clone)]
pub struct UpdateActorHandle {
    sender: mpsc::Sender<UpdateMsg>,
}

impl UpdateActorHandle {
    pub fn new(index_handle: IndexActorHandle) -> Self {
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
        let actor = UpdateActor::new(store, receiver, index_handle);
        tokio::task::spawn_local(actor.run());
        Self { sender }
    }

    pub async fn update(&self, meta: UpdateMeta, payload: Option<File>, uuid: Uuid) -> Result<UpdateStatus> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Update {
            uuid,
            payload,
            meta,
            ret,
        };
        let _ =  self.sender.send(msg).await;
        receiver.await.expect("update actor killed.")
    }
}

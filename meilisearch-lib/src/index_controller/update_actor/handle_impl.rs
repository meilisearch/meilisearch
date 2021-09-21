use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::index_controller::{IndexActorHandle, Update, UpdateStatus};

use super::error::Result;
use super::{UpdateActor, UpdateActorHandle, UpdateMsg, UpdateStoreInfo};

#[derive(Clone)]
pub struct UpdateActorHandleImpl {
    sender: mpsc::Sender<UpdateMsg>,
}

impl UpdateActorHandleImpl {
    pub fn new<I>(
        index_handle: I,
        path: impl AsRef<Path>,
        update_store_size: usize,
    ) -> anyhow::Result<Self>
    where
        I: IndexActorHandle + Clone + Sync + Send +'static,
    {
        let path = path.as_ref().to_owned();
        let (sender, receiver) = mpsc::channel(100);
        let actor = UpdateActor::new(update_store_size, receiver, path, index_handle)?;

        tokio::task::spawn_local(actor.run());

        Ok(Self { sender })
    }
}

#[async_trait::async_trait]
impl UpdateActorHandle for UpdateActorHandleImpl {
    async fn get_all_updates_status(&self, uuid: Uuid) -> Result<Vec<UpdateStatus>> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::ListUpdates { uuid, ret };
        self.sender.send(msg).await?;
        receiver.await?
    }

    async fn update_status(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::GetUpdate { uuid, id, ret };
        self.sender.send(msg).await?;
        receiver.await?
    }

    async fn delete(&self, uuid: Uuid) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Delete { uuid, ret };
        self.sender.send(msg).await?;
        receiver.await?
    }

    async fn snapshot(&self, uuids: HashSet<Uuid>, path: PathBuf) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Snapshot { uuids, path, ret };
        self.sender.send(msg).await?;
        receiver.await?
    }

    async fn dump(&self, uuids: HashSet<Uuid>, path: PathBuf) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Dump { uuids, path, ret };
        self.sender.send(msg).await?;
        receiver.await?
    }

    async fn get_info(&self) -> Result<UpdateStoreInfo> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::GetInfo { ret };
        self.sender.send(msg).await?;
        receiver.await?
    }

    async fn update(
        &self,
        uuid: Uuid,
        update: Update,
    ) -> Result<UpdateStatus> {
        let (ret, receiver) = oneshot::channel();
        let msg = UpdateMsg::Update {
            uuid,
            update,
            ret,
        };
        self.sender.send(msg).await?;
        receiver.await?
    }
}

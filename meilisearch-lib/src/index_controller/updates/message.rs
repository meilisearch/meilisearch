use std::path::PathBuf;

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::index::Index;

use super::error::Result;
use super::{Update, UpdateStatus, UpdateStoreInfo};

#[derive(Debug)]
pub enum UpdateMsg {
    Update {
        uuid: Uuid,
        update: Update,
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
    DeleteIndex {
        uuid: Uuid,
        ret: oneshot::Sender<Result<()>>,
    },
    Snapshot {
        indexes: Vec<Index>,
        path: PathBuf,
        ret: oneshot::Sender<Result<()>>,
    },
    Dump {
        indexes: Vec<Index>,
        path: PathBuf,
        ret: oneshot::Sender<Result<()>>,
    },
    GetInfo {
        ret: oneshot::Sender<Result<UpdateStoreInfo>>,
    },
}

impl UpdateMsg {
    pub async fn snapshot(
        sender: &mpsc::Sender<Self>,
        path: PathBuf,
        indexes: Vec<Index>,
    ) -> Result<()> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Snapshot { path, indexes, ret };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn dump(
        sender: &mpsc::Sender<Self>,
        indexes: Vec<Index>,
        path: PathBuf,
    ) -> Result<()> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Dump { path, indexes, ret };
        sender.send(msg).await?;
        rcv.await?
    }
    pub async fn update(
        sender: &mpsc::Sender<Self>,
        uuid: Uuid,
        update: Update,
    ) -> Result<UpdateStatus> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Update { uuid, update, ret };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn get_update(
        sender: &mpsc::Sender<Self>,
        uuid: Uuid,
        id: u64,
    ) -> Result<UpdateStatus> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::GetUpdate { uuid, id, ret };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn list_updates(
        sender: &mpsc::Sender<Self>,
        uuid: Uuid,
    ) -> Result<Vec<UpdateStatus>> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::ListUpdates { uuid, ret };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn get_info(sender: &mpsc::Sender<Self>) -> Result<UpdateStoreInfo> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::GetInfo { ret };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn delete(sender: &mpsc::Sender<Self>, uuid: Uuid) -> Result<()> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::DeleteIndex { ret, uuid };
        sender.send(msg).await?;
        rcv.await?
    }
}

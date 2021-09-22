use std::collections::HashSet;
use std::path::PathBuf;

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::error::Result;

#[derive(Debug)]
pub enum UuidResolverMsg {
    Get {
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
    Insert {
        uuid: Uuid,
        name: String,
        ret: oneshot::Sender<Result<()>>,
    },
    SnapshotRequest {
        path: PathBuf,
        ret: oneshot::Sender<Result<HashSet<Uuid>>>,
    },
    GetSize {
        ret: oneshot::Sender<Result<u64>>,
    },
    DumpRequest {
        path: PathBuf,
        ret: oneshot::Sender<Result<HashSet<Uuid>>>,
    },
}

impl UuidResolverMsg {
    pub async fn get(channel: &mpsc::Sender<Self>, uid: String) -> Result<Uuid> {
        let (ret, recv) = oneshot::channel();
        let msg = Self::Get { uid, ret };
        channel.send(msg).await?;
        recv.await?
    }

    pub async fn insert(channel: &mpsc::Sender<Self>, uuid: Uuid, name: String) -> Result<()> {
        let (ret, recv) = oneshot::channel();
        let msg = Self::Insert { name, uuid, ret };
        channel.send(msg).await?;
        recv.await?
    }

    pub async fn list(channel: &mpsc::Sender<Self>) -> Result<Vec<(String, Uuid)>> {
        let (ret, recv) = oneshot::channel();
        let msg = Self::List { ret };
        channel.send(msg).await?;
        recv.await?
    }

    pub async fn get_size(channel: &mpsc::Sender<Self>) -> Result<u64> {
        let (ret, recv) = oneshot::channel();
        let msg = Self::GetSize { ret };
        channel.send(msg).await?;
        recv.await?
    }

    pub async fn dump(channel: &mpsc::Sender<Self>, path: PathBuf) -> Result<HashSet<Uuid>> {
        let (ret, recv) = oneshot::channel();
        let msg = Self::DumpRequest { ret, path };
        channel.send(msg).await?;
        recv.await?
    }

    pub async fn snapshot(channel: &mpsc::Sender<Self>, path: PathBuf) -> Result<HashSet<Uuid>> {
        let (ret, recv) = oneshot::channel();
        let msg = Self::SnapshotRequest { ret, path };
        channel.send(msg).await?;
        recv.await?
    }

    pub async fn delete(channel: &mpsc::Sender<Self>, uid: String) -> Result<Uuid> {
        let (ret, recv) = oneshot::channel();
        let msg = Self::Delete { ret, uid };
        channel.send(msg).await?;
        recv.await?
    }
}

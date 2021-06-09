use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::{HeedUuidStore, Result, UuidResolveMsg, UuidResolverActor, UuidResolverHandle};

#[derive(Clone)]
pub struct UuidResolverHandleImpl {
    sender: mpsc::Sender<UuidResolveMsg>,
}

impl UuidResolverHandleImpl {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let (sender, reveiver) = mpsc::channel(100);
        let store = HeedUuidStore::new(path)?;
        let actor = UuidResolverActor::new(reveiver, store);
        tokio::spawn(actor.run());
        Ok(Self { sender })
    }
}

#[async_trait::async_trait]
impl UuidResolverHandle for UuidResolverHandleImpl {
    async fn get(&self, name: String) -> Result<Uuid> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::Get { uid: name, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    async fn delete(&self, name: String) -> anyhow::Result<Uuid> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::Delete { uid: name, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    async fn list(&self) -> anyhow::Result<Vec<(String, Uuid)>> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::List { ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    async fn insert(&self, name: String, uuid: Uuid) -> anyhow::Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::Insert { ret, name, uuid };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::SnapshotRequest { path, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }

    async fn get_size(&self) -> Result<u64> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::GetSize { ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }
    async fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::DumpRequest { ret, path };
        let _ = self.sender.send(msg).await;
        Ok(receiver
            .await
            .expect("Uuid resolver actor has been killed")?)
    }
}

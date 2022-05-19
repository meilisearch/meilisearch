use tokio::sync::{mpsc, oneshot};

use super::error::Result;
use super::{DumpActorHandle, DumpMsg};

#[derive(Clone)]
pub struct DumpActorHandleImpl {
    pub sender: mpsc::Sender<DumpMsg>,
}

#[async_trait::async_trait]
impl DumpActorHandle for DumpActorHandleImpl {
    async fn create_dump(&self) -> Result<DumpInfo> {
        let (ret, receiver) = oneshot::channel();
        let msg = DumpMsg::CreateDump { ret };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("IndexActor has been killed")
    }

    async fn dump_info(&self, uid: String) -> Result<DumpInfo> {
        let (ret, receiver) = oneshot::channel();
        let msg = DumpMsg::DumpInfo { ret, uid };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("IndexActor has been killed")
    }
}

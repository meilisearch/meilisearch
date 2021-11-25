use std::path::Path;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::index_resolver::HardStateIndexResolver;
use crate::tasks::task_store::TaskStore;
use crate::MeiliSearch;

use super::error::Result;
use super::{DumpActor, DumpActorHandle, DumpInfo, DumpMsg};

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

impl DumpActorHandleImpl {
    pub fn new(
        path: impl AsRef<Path>,
        analytics_path: impl AsRef<Path>,
        index_resolver: Arc<HardStateIndexResolver>,
        task_store: TaskStore,
        index_db_size: usize,
        update_db_size: usize,
    ) -> anyhow::Result<Self> {
        let (sender, receiver) = mpsc::channel(10);
        let actor = DumpActor::new(
            receiver,
            index_resolver,
            task_store,
            path,
            analytics_path,
            index_db_size,
            update_db_size,
        );

        tokio::task::spawn(actor.run());

        Ok(Self { sender })
    }
}

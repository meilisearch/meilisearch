use std::path::Path;

use actix_web::web::Bytes;
use tokio::sync::{mpsc, oneshot};

use super::error::Result;
use super::{DumpActor, DumpActorHandle, DumpInfo, DumpMsg};

#[derive(Clone)]
pub struct DumpActorHandleImpl {
    sender: mpsc::Sender<DumpMsg>,
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
        uuid_resolver: crate::index_controller::uuid_resolver::UuidResolverHandleImpl,
        update: crate::index_controller::update_actor::UpdateActorHandleImpl<Bytes>,
        index_db_size: usize,
        update_db_size: usize,
    ) -> anyhow::Result<Self> {
        let (sender, receiver) = mpsc::channel(10);
        let actor = DumpActor::new(
            receiver,
            uuid_resolver,
            update,
            path,
            index_db_size,
            update_db_size,
        );

        tokio::task::spawn(actor.run());

        Ok(Self { sender })
    }
}

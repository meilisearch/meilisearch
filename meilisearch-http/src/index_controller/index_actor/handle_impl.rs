use std::path::{Path, PathBuf};

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::index::{Document, SearchQuery, SearchResult, Settings};
use crate::index_controller::{updates::Processing, UpdateMeta};
use crate::index_controller::{IndexSettings, IndexStats};

use super::{
    IndexActor, IndexActorHandle, IndexMeta, IndexMsg, MapIndexStore, Result, UpdateResult,
};

#[derive(Clone)]
pub struct IndexActorHandleImpl {
    read_sender: mpsc::Sender<IndexMsg>,
    write_sender: mpsc::Sender<IndexMsg>,
}

#[async_trait::async_trait]
impl IndexActorHandle for IndexActorHandleImpl {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMeta> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::CreateIndex {
            ret,
            uuid,
            primary_key,
        };
        let _ = self.read_sender.send(msg).await;
        receiver.await.expect("IndexActor has been killed")
    }

    async fn update(
        &self,
        uuid: Uuid,
        meta: Processing<UpdateMeta>,
        data: std::fs::File,
    ) -> anyhow::Result<UpdateResult> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Update { ret, meta, data, uuid };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn search(&self, uuid: Uuid, query: SearchQuery) -> Result<SearchResult> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Search { uuid, query, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn settings(&self, uuid: Uuid) -> Result<Settings> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Settings { uuid, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn documents(
        &self,
        uuid: Uuid,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Vec<Document>> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Documents {
            uuid,
            ret,
            offset,
            attributes_to_retrieve,
            limit,
        };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn document(
        &self,
        uuid: Uuid,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Document> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Document {
            uuid,
            ret,
            doc_id,
            attributes_to_retrieve,
        };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn delete(&self, uuid: Uuid) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Delete { uuid, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn get_index_meta(&self, uuid: Uuid) -> Result<IndexMeta> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::GetMeta { uuid, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn update_index(&self, uuid: Uuid, index_settings: IndexSettings) -> Result<IndexMeta> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::UpdateIndex {
            uuid,
            index_settings,
            ret,
        };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Snapshot { uuid, path, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn get_index_stats(&self, uuid: Uuid) -> Result<IndexStats> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::GetStats { uuid, ret };
        let _ = self.read_sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }
}

impl IndexActorHandleImpl {
    pub fn new(path: impl AsRef<Path>, index_size: usize) -> anyhow::Result<Self> {
        let (read_sender, read_receiver) = mpsc::channel(100);
        let (write_sender, write_receiver) = mpsc::channel(100);

        let store = MapIndexStore::new(path, index_size);
        let actor = IndexActor::new(read_receiver, write_receiver, store)?;
        tokio::task::spawn(actor.run());
        Ok(Self {
            read_sender,
            write_sender,
        })
    }
}

use crate::option::IndexerOpts;
use std::path::{Path, PathBuf};

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::{
    index::Checked,
    index_controller::{IndexSettings, IndexStats, Processing},
};
use crate::{
    index::{Document, SearchQuery, SearchResult, Settings},
    index_controller::{Failed, Processed},
};

use super::error::Result;
use super::{IndexActor, IndexActorHandle, IndexMeta, IndexMsg, MapIndexStore};

#[derive(Clone)]
pub struct IndexActorHandleImpl {
    sender: mpsc::Sender<IndexMsg>,
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
        let _ = self.sender.send(msg).await;
        receiver.await.expect("IndexActor has been killed")
    }

    async fn update(
        &self,
        uuid: Uuid,
        meta: Processing,
        data: Option<std::fs::File>,
    ) -> Result<std::result::Result<Processed, Failed>> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Update {
            ret,
            meta,
            data,
            uuid,
        };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn search(&self, uuid: Uuid, query: SearchQuery) -> Result<SearchResult> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Search { uuid, query, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn settings(&self, uuid: Uuid) -> Result<Settings<Checked>> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Settings { uuid, ret };
        let _ = self.sender.send(msg).await;
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
        let _ = self.sender.send(msg).await;
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
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn delete(&self, uuid: Uuid) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Delete { uuid, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn get_index_meta(&self, uuid: Uuid) -> Result<IndexMeta> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::GetMeta { uuid, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn update_index(&self, uuid: Uuid, index_settings: IndexSettings) -> Result<IndexMeta> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::UpdateIndex {
            uuid,
            index_settings,
            ret,
        };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Snapshot { uuid, path, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn dump(&self, uuid: Uuid, path: PathBuf) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Dump { uuid, path, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }

    async fn get_index_stats(&self, uuid: Uuid) -> Result<IndexStats> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::GetStats { uuid, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }
}

impl IndexActorHandleImpl {
    pub fn new(
        path: impl AsRef<Path>,
        index_size: usize,
        options: &IndexerOpts,
    ) -> anyhow::Result<Self> {
        let (sender, receiver) = mpsc::channel(100);

        let store = MapIndexStore::new(path, index_size);
        let actor = IndexActor::new(receiver, store, options)?;
        tokio::task::spawn(actor.run());
        Ok(Self { sender })
    }
}

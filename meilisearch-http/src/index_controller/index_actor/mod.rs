#[cfg(test)]
use std::sync::Arc;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
#[cfg(test)]
use mockall::automock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use actor::IndexActor;
pub use actor::CONCURRENT_INDEX_MSG;
pub use handle_impl::IndexActorHandleImpl;
use message::IndexMsg;
use store::{IndexStore, MapIndexStore};

use crate::index::UpdateResult as UResult;
use crate::index::{Document, Index, SearchQuery, SearchResult, Settings};
use crate::index_controller::{
    updates::{Failed, Processed, Processing},
    IndexStats, UpdateMeta,
};

use super::IndexSettings;

mod actor;
mod handle_impl;
mod message;
mod store;

pub type Result<T> = std::result::Result<T, IndexError>;
type UpdateResult = std::result::Result<Processed<UpdateMeta, UResult>, Failed<UpdateMeta, String>>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMeta {
    created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

impl IndexMeta {
    fn new(index: &Index) -> Result<Self> {
        let txn = index.read_txn()?;
        Self::new_txn(index, &txn)
    }

    fn new_txn(index: &Index, txn: &heed::RoTxn) -> Result<Self> {
        let created_at = index.created_at(&txn)?;
        let updated_at = index.updated_at(&txn)?;
        let primary_key = index.primary_key(&txn)?.map(String::from);
        Ok(Self {
            primary_key,
            updated_at,
            created_at,
        })
    }
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("error with index: {0}")]
    Error(#[from] anyhow::Error),
    #[error("index already exists")]
    IndexAlreadyExists,
    #[error("Index doesn't exists")]
    UnexistingIndex,
    #[error("Heed error: {0}")]
    HeedError(#[from] heed::Error),
    #[error("Existing primary key")]
    ExistingPrimaryKey,
}

#[cfg(test)]
#[async_trait::async_trait]
impl IndexActorHandle for Arc<MockIndexActorHandle> {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMeta> {
        self.as_ref().create_index(uuid, primary_key).await
    }

    async fn update(
        &self,
        uuid: Uuid,
        meta: Processing<UpdateMeta>,
        data: std::fs::File,
    ) -> anyhow::Result<UpdateResult> {
        self.as_ref().update(uuid, meta, data).await
    }

    async fn search(&self, uuid: Uuid, query: SearchQuery) -> Result<SearchResult> {
        self.as_ref().search(uuid, query).await
    }

    async fn settings(&self, uuid: Uuid) -> Result<Settings> {
        self.as_ref().settings(uuid).await
    }

    async fn documents(
        &self,
        uuid: Uuid,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Vec<Document>> {
        self.as_ref().documents(uuid, offset, limit, attributes_to_retrieve).await
    }

    async fn document(
        &self,
        uuid: Uuid,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Document> {
        self.as_ref().document(uuid, doc_id, attributes_to_retrieve).await
    }

    async fn delete(&self, uuid: Uuid) -> Result<()> {
        self.as_ref().delete(uuid).await
    }

    async fn get_index_meta(&self, uuid: Uuid) -> Result<IndexMeta> {
        self.as_ref().get_index_meta(uuid).await
    }

    async fn update_index(&self, uuid: Uuid, index_settings: IndexSettings) -> Result<IndexMeta> {
        self.as_ref().update_index(uuid, index_settings).await
    }

    async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> Result<()> {
        self.as_ref().snapshot(uuid, path).await
    }

    async fn get_index_stats(&self, uuid: Uuid) -> Result<IndexStats> {
        self.as_ref().get_index_stats(uuid).await
    }
}

#[async_trait::async_trait]
#[cfg_attr(test, automock)]
pub trait IndexActorHandle {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMeta>;
    async fn update(
        &self,
        uuid: Uuid,
        meta: Processing<UpdateMeta>,
        data: std::fs::File,
    ) -> anyhow::Result<UpdateResult>;
    async fn search(&self, uuid: Uuid, query: SearchQuery) -> Result<SearchResult>;
    async fn settings(&self, uuid: Uuid) -> Result<Settings>;

    async fn documents(
        &self,
        uuid: Uuid,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Vec<Document>>;
    async fn document(
        &self,
        uuid: Uuid,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Document>;
    async fn delete(&self, uuid: Uuid) -> Result<()>;
    async fn get_index_meta(&self, uuid: Uuid) -> Result<IndexMeta>;
    async fn update_index(&self, uuid: Uuid, index_settings: IndexSettings) -> Result<IndexMeta>;
    async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> Result<()>;
    async fn get_index_stats(&self, uuid: Uuid) -> Result<IndexStats>;
}

use std::fs::File;
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

use crate::index::{Document, Index, SearchQuery, SearchResult, Settings};
use crate::index_controller::{Failed, Processed, Processing, IndexStats};

use super::IndexSettings;

mod actor;
mod handle_impl;
mod message;
mod store;

pub type IndexResult<T> = std::result::Result<T, IndexError>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMeta {
    created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

impl IndexMeta {
    fn new(index: &Index) -> IndexResult<Self> {
        let txn = index.read_txn()?;
        Self::new_txn(index, &txn)
    }

    fn new_txn(index: &Index, txn: &heed::RoTxn) -> IndexResult<Self> {
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

#[async_trait::async_trait]
#[cfg_attr(test, automock)]
pub trait IndexActorHandle {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>)
        -> IndexResult<IndexMeta>;
    async fn update(
        &self,
        uuid: Uuid,
        meta: Processing,
        data: Option<File>,
    ) -> anyhow::Result<Result<Processed, Failed>>;
    async fn search(&self, uuid: Uuid, query: SearchQuery) -> IndexResult<SearchResult>;
    async fn settings(&self, uuid: Uuid) -> IndexResult<Settings>;

    async fn documents(
        &self,
        uuid: Uuid,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> IndexResult<Vec<Document>>;
    async fn document(
        &self,
        uuid: Uuid,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> IndexResult<Document>;
    async fn delete(&self, uuid: Uuid) -> IndexResult<()>;
    async fn get_index_meta(&self, uuid: Uuid) -> IndexResult<IndexMeta>;
    async fn update_index(
        &self,
        uuid: Uuid,
        index_settings: IndexSettings,
    ) -> IndexResult<IndexMeta>;
    async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> IndexResult<()>;
    async fn get_index_stats(&self, uuid: Uuid) -> IndexResult<IndexStats>;
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[async_trait::async_trait]
    /// Useful for passing around an `Arc<MockIndexActorHandle>` in tests.
    impl IndexActorHandle for Arc<MockIndexActorHandle> {
        async fn create_index(
            &self,
            uuid: Uuid,
            primary_key: Option<String>,
        ) -> IndexResult<IndexMeta> {
            self.as_ref().create_index(uuid, primary_key).await
        }

        async fn update(
            &self,
            uuid: Uuid,
            meta: Processing,
            data: Option<std::fs::File>,
        ) -> anyhow::Result<Result<Processed, Failed>> {
            self.as_ref().update(uuid, meta, data).await
        }

        async fn search(&self, uuid: Uuid, query: SearchQuery) -> IndexResult<SearchResult> {
            self.as_ref().search(uuid, query).await
        }

        async fn settings(&self, uuid: Uuid) -> IndexResult<Settings> {
            self.as_ref().settings(uuid).await
        }

        async fn documents(
            &self,
            uuid: Uuid,
            offset: usize,
            limit: usize,
            attributes_to_retrieve: Option<Vec<String>>,
        ) -> IndexResult<Vec<Document>> {
            self.as_ref()
                .documents(uuid, offset, limit, attributes_to_retrieve)
                .await
        }

        async fn document(
            &self,
            uuid: Uuid,
            doc_id: String,
            attributes_to_retrieve: Option<Vec<String>>,
        ) -> IndexResult<Document> {
            self.as_ref()
                .document(uuid, doc_id, attributes_to_retrieve)
                .await
        }

        async fn delete(&self, uuid: Uuid) -> IndexResult<()> {
            self.as_ref().delete(uuid).await
        }

        async fn get_index_meta(&self, uuid: Uuid) -> IndexResult<IndexMeta> {
            self.as_ref().get_index_meta(uuid).await
        }

        async fn update_index(
            &self,
            uuid: Uuid,
            index_settings: IndexSettings,
        ) -> IndexResult<IndexMeta> {
            self.as_ref().update_index(uuid, index_settings).await
        }

        async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> IndexResult<()> {
            self.as_ref().snapshot(uuid, path).await
        }

        async fn get_index_stats(&self, uuid: Uuid) -> IndexResult<IndexStats> {
            self.as_ref().get_index_stats(uuid).await
        }
    }
}

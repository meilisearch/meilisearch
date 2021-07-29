use std::fs::File;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
#[cfg(test)]
use mockall::automock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use actor::IndexActor;
pub use actor::CONCURRENT_INDEX_MSG;
pub use handle_impl::IndexActorHandleImpl;
use message::IndexMsg;
use store::{IndexStore, MapIndexStore};

use crate::index::update_handler::Hello;
use crate::index::{Checked, Document, Index, SearchQuery, SearchResult, Settings};
use crate::index_controller::{Failed, IndexStats, Processed, Processing};
use error::Result;

use super::IndexSettings;

mod actor;
pub mod error;
mod handle_impl;
mod message;
mod store;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMeta {
    created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub primary_key: Option<String>,
}

impl IndexMeta {
    fn new(index: &Index) -> Result<Self> {
        let txn = index.read_txn()?;
        Self::new_txn(index, &txn)
    }

    fn new_txn(index: &Index, txn: &heed::RoTxn) -> Result<Self> {
        let created_at = index.created_at(txn)?;
        let updated_at = index.updated_at(txn)?;
        let primary_key = index.primary_key(txn)?.map(String::from);
        Ok(Self {
            created_at,
            updated_at,
            primary_key,
        })
    }
}

#[async_trait::async_trait]
#[cfg_attr(test, automock)]
pub trait IndexActorHandle {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMeta>;
    async fn update(
        &self,
        channel: std::sync::mpsc::Sender<(std::sync::mpsc::Sender<Hello>, std::result::Result<Processed, Failed>)>,
        uuid: Uuid,
        meta: Processing,
        data: Option<File>,
    ) -> Result<std::result::Result<Processed, Failed>>;
    async fn search(&self, uuid: Uuid, query: SearchQuery) -> Result<SearchResult>;
    async fn settings(&self, uuid: Uuid) -> Result<Settings<Checked>>;

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
    async fn dump(&self, uuid: Uuid, path: PathBuf) -> Result<()>;
    async fn get_index_stats(&self, uuid: Uuid) -> Result<IndexStats>;
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[async_trait::async_trait]
    /// Useful for passing around an `Arc<MockIndexActorHandle>` in tests.
    impl IndexActorHandle for Arc<MockIndexActorHandle> {
        async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMeta> {
            self.as_ref().create_index(uuid, primary_key).await
        }

        async fn update(
            &self,
            uuid: Uuid,
            meta: Processing,
            data: Option<std::fs::File>,
        ) -> Result<std::result::Result<Processed, Failed>> {
            self.as_ref().update(uuid, meta, data).await
        }

        async fn search(&self, uuid: Uuid, query: SearchQuery) -> Result<SearchResult> {
            self.as_ref().search(uuid, query).await
        }

        async fn settings(&self, uuid: Uuid) -> Result<Settings<Checked>> {
            self.as_ref().settings(uuid).await
        }

        async fn documents(
            &self,
            uuid: Uuid,
            offset: usize,
            limit: usize,
            attributes_to_retrieve: Option<Vec<String>>,
        ) -> Result<Vec<Document>> {
            self.as_ref()
                .documents(uuid, offset, limit, attributes_to_retrieve)
                .await
        }

        async fn document(
            &self,
            uuid: Uuid,
            doc_id: String,
            attributes_to_retrieve: Option<Vec<String>>,
        ) -> Result<Document> {
            self.as_ref()
                .document(uuid, doc_id, attributes_to_retrieve)
                .await
        }

        async fn delete(&self, uuid: Uuid) -> Result<()> {
            self.as_ref().delete(uuid).await
        }

        async fn get_index_meta(&self, uuid: Uuid) -> Result<IndexMeta> {
            self.as_ref().get_index_meta(uuid).await
        }

        async fn update_index(
            &self,
            uuid: Uuid,
            index_settings: IndexSettings,
        ) -> Result<IndexMeta> {
            self.as_ref().update_index(uuid, index_settings).await
        }

        async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> Result<()> {
            self.as_ref().snapshot(uuid, path).await
        }

        async fn dump(&self, uuid: Uuid, path: PathBuf) -> Result<()> {
            self.as_ref().dump(uuid, path).await
        }

        async fn get_index_stats(&self, uuid: Uuid) -> Result<IndexStats> {
            self.as_ref().get_index_stats(uuid).await
        }
    }
}

mod actor;
mod handle_impl;
mod message;
mod store;

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use super::IndexSettings;
use crate::index::UpdateResult as UResult;
use crate::index::{Document, Index, SearchQuery, SearchResult, Settings};
use crate::index_controller::{
    updates::{Failed, Processed, Processing},
    UpdateMeta,
};
use message::IndexMsg;
use store::{IndexStore, MapIndexStore};
use actor::IndexActor;

pub use handle_impl::IndexActorHandleImpl;

#[cfg(test)]
use mockall::automock;

pub type Result<T> = std::result::Result<T, IndexError>;
type UpdateResult = std::result::Result<Processed<UpdateMeta, UResult>, Failed<UpdateMeta, String>>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMeta {
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
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


#[async_trait::async_trait]
#[cfg_attr(test, automock)]
pub trait IndexActorHandle {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMeta>;
    async fn update(
        &self,
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
    async fn update_index(
        &self,
        uuid: Uuid,
        index_settings: IndexSettings,
    ) -> Result<IndexMeta>;
    async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> Result<()>;
}


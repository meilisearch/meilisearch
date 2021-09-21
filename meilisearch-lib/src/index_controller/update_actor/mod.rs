use std::{collections::HashSet, path::PathBuf};

use milli::update::IndexDocumentsMethod;
use uuid::Uuid;
use serde::{Serialize, Deserialize};

use crate::index_controller::UpdateStatus;
use super::Update;

use actor::UpdateActor;
use error::Result;
use message::UpdateMsg;

pub use handle_impl::UpdateActorHandleImpl;
pub use store::{UpdateStore, UpdateStoreInfo};

mod actor;
pub mod error;
mod handle_impl;
mod message;
pub mod store;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegisterUpdate {
    DocumentAddition {
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_uuid: Uuid,
    }
}


#[cfg(test)]
use mockall::automock;

#[async_trait::async_trait]
pub trait UpdateActorHandle {
    async fn get_all_updates_status(&self, uuid: Uuid) -> Result<Vec<UpdateStatus>>;
    async fn update_status(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus>;
    async fn delete(&self, uuid: Uuid) -> Result<()>;
    async fn snapshot(&self, uuid: HashSet<Uuid>, path: PathBuf) -> Result<()>;
    async fn dump(&self, uuids: HashSet<Uuid>, path: PathBuf) -> Result<()>;
    async fn get_info(&self) -> Result<UpdateStoreInfo>;
    async fn update(
        &self,
        uuid: Uuid,
        update: Update,
    ) -> Result<UpdateStatus>;
}

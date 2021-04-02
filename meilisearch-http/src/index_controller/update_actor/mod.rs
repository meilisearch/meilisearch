mod actor;
mod handle_impl;
mod message;
mod store;
mod update_store;

use std::path::PathBuf;

use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::index::UpdateResult;
use crate::index_controller::{UpdateMeta, UpdateStatus};

use actor::UpdateActor;
use message::UpdateMsg;
use store::{MapUpdateStoreStore, UpdateStoreStore};

pub use handle_impl::UpdateActorHandleImpl;

pub type Result<T> = std::result::Result<T, UpdateError>;
type UpdateStore = update_store::UpdateStore<UpdateMeta, UpdateResult, String>;
type PayloadData<D> = std::result::Result<D, Box<dyn std::error::Error + Sync + Send + 'static>>;

#[cfg(test)]
use mockall::automock;

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("error with update: {0}")]
    Error(Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Index {0} doesn't exist.")]
    UnexistingIndex(Uuid),
    #[error("Update {0} doesn't exist.")]
    UnexistingUpdate(u64),
}

#[async_trait::async_trait]
#[cfg_attr(test, automock(type Data=Vec<u8>;))]
pub trait UpdateActorHandle {
    type Data: AsRef<[u8]> + Sized + 'static + Sync + Send;

    async fn get_all_updates_status(&self, uuid: Uuid) -> Result<Vec<UpdateStatus>>;
    async fn update_status(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus>;
    async fn delete(&self, uuid: Uuid) -> Result<()>;
    async fn create(&self, uuid: Uuid) -> Result<()>;
    async fn snapshot(&self, uuid: Uuid, path: PathBuf) -> Result<()>;
    async fn update(
        &self,
        meta: UpdateMeta,
        data: mpsc::Receiver<PayloadData<Self::Data>>,
        uuid: Uuid,
    ) -> Result<UpdateStatus>;
    async fn is_locked(&self, uuid: Uuid) -> Result<bool>;
}

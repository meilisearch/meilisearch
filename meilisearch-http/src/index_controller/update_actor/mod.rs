mod actor;
mod handle_impl;
mod message;
mod update_store;

use std::{collections::HashSet, path::PathBuf};

use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::index_controller::{UpdateMeta, UpdateStatus};

use actor::UpdateActor;
use message::UpdateMsg;
use update_store::UpdateStore;
pub use update_store::UpdateStoreInfo;

pub use handle_impl::UpdateActorHandleImpl;

pub type Result<T> = std::result::Result<T, UpdateError>;
type PayloadData<D> = std::result::Result<D, Box<dyn std::error::Error + Sync + Send + 'static>>;

#[cfg(test)]
use mockall::automock;

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("error with update: {0}")]
    Error(Box<dyn std::error::Error + Sync + Send + 'static>),
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
    async fn snapshot(&self, uuids: HashSet<Uuid>, path: PathBuf) -> Result<()>;
    async fn get_info(&self) -> Result<UpdateStoreInfo>;
    async fn update(
        &self,
        meta: UpdateMeta,
        data: mpsc::Receiver<PayloadData<Self::Data>>,
        uuid: Uuid,
    ) -> Result<UpdateStatus>;
}

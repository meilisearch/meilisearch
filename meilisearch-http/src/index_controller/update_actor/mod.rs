mod actor;
mod handle_impl;
mod message;
mod update_store;

use std::{collections::HashSet, path::PathBuf};

use actix_http::error::PayloadError;
use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::index_controller::{UpdateMeta, UpdateStatus};

use actor::UpdateActor;
use message::UpdateMsg;

pub use handle_impl::UpdateActorHandleImpl;
pub use update_store::{UpdateStore, UpdateStoreInfo};

pub type Result<T> = std::result::Result<T, UpdateError>;
type PayloadData<D> = std::result::Result<D, PayloadError>;

#[cfg(test)]
use mockall::automock;

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("Update {0} doesn't exist.")]
    UnexistingUpdate(u64),
    #[error("Internal error processing update: {0}")]
    Internal(String),
}

macro_rules! internal_error {
    ($($other:path), *) => {
        $(
            impl From<$other> for UpdateError {
                fn from(other: $other) -> Self {
                    Self::Internal(other.to_string())
                }
            }
        )*
    }
}

internal_error!(
    heed::Error,
    std::io::Error,
    serde_json::Error,
    PayloadError,
    tokio::task::JoinError,
    anyhow::Error
);

#[async_trait::async_trait]
#[cfg_attr(test, automock(type Data=Vec<u8>;))]
pub trait UpdateActorHandle {
    type Data: AsRef<[u8]> + Sized + 'static + Sync + Send;

    async fn get_all_updates_status(&self, uuid: Uuid) -> Result<Vec<UpdateStatus>>;
    async fn update_status(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus>;
    async fn delete(&self, uuid: Uuid) -> Result<()>;
    async fn snapshot(&self, uuid: HashSet<Uuid>, path: PathBuf) -> Result<()>;
    async fn dump(&self, uuid: HashSet<(String, Uuid)>, path: PathBuf) -> Result<()>;
    async fn get_info(&self) -> Result<UpdateStoreInfo>;
    async fn update(
        &self,
        meta: UpdateMeta,
        data: mpsc::Receiver<PayloadData<Self::Data>>,
        uuid: Uuid,
    ) -> Result<UpdateStatus>;
}

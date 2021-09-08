use std::{collections::HashSet, path::PathBuf};

use actix_web::error::PayloadError;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::index_controller::{UpdateMeta, UpdateStatus};

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

type PayloadData<D> = std::result::Result<D, PayloadError>;

#[cfg(test)]
use mockall::automock;

#[async_trait::async_trait]
#[cfg_attr(test, automock(type Data=Vec<u8>;))]
pub trait UpdateActorHandle {
    type Data: AsRef<[u8]> + Sized + 'static + Sync + Send;

    async fn get_all_updates_status(&self, uuid: Uuid) -> Result<Vec<UpdateStatus>>;
    async fn update_status(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus>;
    async fn delete(&self, uuid: Uuid) -> Result<()>;
    async fn snapshot(&self, uuid: HashSet<Uuid>, path: PathBuf) -> Result<()>;
    async fn dump(&self, uuids: HashSet<Uuid>, path: PathBuf) -> Result<()>;
    async fn get_info(&self) -> Result<UpdateStoreInfo>;
    async fn update(
        &self,
        meta: UpdateMeta,
        data: mpsc::Receiver<PayloadData<Self::Data>>,
        uuid: Uuid,
    ) -> Result<UpdateStatus>;
}

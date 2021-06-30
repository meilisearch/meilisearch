mod actor;
pub mod error;
mod handle_impl;
mod message;
pub mod store;

use std::collections::HashSet;
use std::path::PathBuf;

use uuid::Uuid;

use actor::UuidResolverActor;
use error::Result;
use message::UuidResolveMsg;
use store::UuidStore;

#[cfg(test)]
use mockall::automock;

pub use handle_impl::UuidResolverHandleImpl;
pub use store::HeedUuidStore;

const UUID_STORE_SIZE: usize = 1_073_741_824; //1GiB

#[async_trait::async_trait]
#[cfg_attr(test, automock)]
pub trait UuidResolverHandle {
    async fn get(&self, name: String) -> Result<Uuid>;
    async fn insert(&self, name: String, uuid: Uuid) -> Result<()>;
    async fn delete(&self, name: String) -> Result<Uuid>;
    async fn list(&self) -> Result<Vec<(String, Uuid)>>;
    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
    async fn get_size(&self) -> Result<u64>;
    async fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
}

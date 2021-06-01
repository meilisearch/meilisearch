mod actor;
mod handle_impl;
mod message;
pub mod store;

use std::collections::HashSet;
use std::path::PathBuf;

use thiserror::Error;
use uuid::Uuid;

use actor::UuidResolverActor;
use message::UuidResolveMsg;
use store::UuidStore;

#[cfg(test)]
use mockall::automock;

pub use handle_impl::UuidResolverHandleImpl;
pub use store::HeedUuidStore;

const UUID_STORE_SIZE: usize = 1_073_741_824; //1GiB

pub type Result<T> = std::result::Result<T, UuidResolverError>;

#[async_trait::async_trait]
#[cfg_attr(test, automock)]
pub trait UuidResolverHandle {
    async fn get(&self, name: String) -> Result<Uuid>;
    async fn insert(&self, name: String, uuid: Uuid) -> anyhow::Result<()>;
    async fn create(&self, name: String) -> anyhow::Result<Uuid>;
    async fn delete(&self, name: String) -> anyhow::Result<Uuid>;
    async fn list(&self) -> anyhow::Result<Vec<(String, Uuid)>>;
    async fn snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
    async fn get_size(&self) -> Result<u64>;
    async fn dump(&self, path: PathBuf) -> Result<HashSet<Uuid>>;
}

#[derive(Debug, Error)]
pub enum UuidResolverError {
    #[error("Name already exist.")]
    NameAlreadyExist,
    #[error("Index \"{0}\" doesn't exist.")]
    UnexistingIndex(String),
    #[error("Badly formatted index uid: {0}")]
    BadlyFormatted(String),
    #[error("Internal error resolving index uid: {0}")]
    Internal(String),
}

macro_rules! internal_error {
    ($($other:path), *) => {
        $(
            impl From<$other> for UuidResolverError {
                fn from(other: $other) -> Self {
                    Self::Internal(other.to_string())
                }
            }
        )*
    }
}

internal_error!(
    heed::Error,
    uuid::Error,
    std::io::Error,
    tokio::task::JoinError,
    serde_json::Error
);

mod actor;
mod handle_impl;
mod message;
pub mod store;

use std::collections::HashSet;
use std::path::PathBuf;

use meilisearch_error::Code;
use meilisearch_error::ErrorCode;
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
    async fn insert(&self, name: String, uuid: Uuid) -> Result<()>;
    async fn delete(&self, name: String) -> Result<Uuid>;
    async fn list(&self) -> Result<Vec<(String, Uuid)>>;
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
    Internal(Box<dyn std::error::Error + Sync + Send + 'static>),
}

macro_rules! internal_error {
    ($($other:path), *) => {
        $(
            impl From<$other> for UuidResolverError {
                fn from(other: $other) -> Self {
                    Self::Internal(Box::new(other))
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

impl ErrorCode for UuidResolverError {
    fn error_code(&self) -> Code {
        match self {
            UuidResolverError::NameAlreadyExist => Code::IndexAlreadyExists,
            UuidResolverError::UnexistingIndex(_) => Code::IndexNotFound,
            UuidResolverError::BadlyFormatted(_) => Code::InvalidIndexUid,
            UuidResolverError::Internal(_) => Code::Internal,
        }
    }
}

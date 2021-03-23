mod actor;
mod handle_impl;
mod message;
mod store;

use std::path::PathBuf;

use thiserror::Error;
use uuid::Uuid;

use actor::UuidResolverActor;
use message::UuidResolveMsg;
use store::{HeedUuidStore, UuidStore};

#[cfg(test)]
use mockall::automock;

pub use handle_impl::UuidResolverHandleImpl;

const UUID_STORE_SIZE: usize = 1_073_741_824; //1GiB

pub type Result<T> = std::result::Result<T, UuidError>;

#[async_trait::async_trait]
#[cfg_attr(test, automock)]
pub trait UuidResolverHandle {
    async fn resolve(&self, name: String) -> anyhow::Result<Uuid>;
    async fn get_or_create(&self, name: String) -> Result<Uuid>;
    async fn create(&self, name: String) -> anyhow::Result<Uuid>;
    async fn delete(&self, name: String) -> anyhow::Result<Uuid>;
    async fn list(&self) -> anyhow::Result<Vec<(String, Uuid)>>;
    async fn snapshot(&self, path: PathBuf) -> Result<Vec<Uuid>>;
}

#[derive(Debug, Error)]
pub enum UuidError {
    #[error("Name already exist.")]
    NameAlreadyExist,
    #[error("Index \"{0}\" doesn't exist.")]
    UnexistingIndex(String),
    #[error("Error performing task: {0}")]
    TokioTask(#[from] tokio::task::JoinError),
    #[error("Database error: {0}")]
    Heed(#[from] heed::Error),
    #[error("Uuid error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("Badly formatted index uid: {0}")]
    BadlyFormatted(String),
}

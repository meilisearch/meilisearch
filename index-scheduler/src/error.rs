use milli::heed;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Index `{0}` not found")]
    IndexNotFound(String),
    #[error("Index `{0}` already exists")]
    IndexAlreadyExists(String),
    #[error("Corrupted task queue.")]
    CorruptedTaskQueue,
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Milli(#[from] milli::Error),
    #[error(transparent)]
    IndexError(#[from] index::error::IndexError),
    #[error(transparent)]
    FileStore(#[from] file_store::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

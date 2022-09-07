use milli::heed;
use thiserror::Error;

use crate::index;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Index `{}` not found", .0)]
    IndexNotFound(String),
    #[error("Index `{}` already exists", .0)]
    IndexAlreadyExists(String),
    #[error("Corrupted task queue.")]
    CorruptedTaskQueue,
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Milli(#[from] milli::Error),
    #[error("{0}")]
    IndexError(#[from] index::error::IndexError),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

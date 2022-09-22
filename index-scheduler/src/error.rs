use meilisearch_types::error::{Code, ErrorCode};
use milli::heed;
use thiserror::Error;

use crate::TaskId;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Index `{0}` not found")]
    IndexNotFound(String),
    #[error("Index `{0}` already exists")]
    IndexAlreadyExists(String),
    #[error("Corrupted task queue.")]
    CorruptedTaskQueue,
    #[error("Task `{0}` not found")]
    TaskNotFound(TaskId),

    // maybe the two next errors are going to move to the frontend
    #[error("`{0}` is not a status. Available status are")]
    InvalidStatus(String),
    #[error("`{0}` is not a type. Available types are")]
    InvalidKind(String),

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

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        match self {
            Error::IndexNotFound(_) => Code::IndexNotFound,
            Error::IndexAlreadyExists(_) => Code::IndexAlreadyExists,
            Error::TaskNotFound(_) => Code::TaskNotFound,
            Error::InvalidStatus(_) => todo!(),
            Error::InvalidKind(_) => todo!(),
            Error::Heed(_) => todo!(),
            Error::Milli(_) => todo!(),
            Error::IndexError(_) => todo!(),
            Error::FileStore(_) => todo!(),
            Error::IoError(_) => todo!(),
            Error::Anyhow(_) => Code::Internal,
            Error::CorruptedTaskQueue => Code::Internal,
        }
    }
}

use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::heed;
use meilisearch_types::milli;
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
    #[error("Corrupted dump.")]
    CorruptedDump,
    #[error("Task `{0}` not found")]
    TaskNotFound(TaskId),
    #[error("Query parameters to filter the tasks to delete are missing. Available query parameters are: `uid`, `indexUid`, `status`, `type`")]
    TaskDeletionWithEmptyQuery,
    #[error("Query parameters to filter the tasks to cancel are missing. Available query parameters are: `uid`, `indexUid`, `status`, `type`")]
    TaskCancelationWithEmptyQuery,
    // maybe the two next errors are going to move to the frontend
    #[error("`{0}` is not a status. Available status are")]
    InvalidStatus(String),
    #[error("`{0}` is not a type. Available types are")]
    InvalidKind(String),

    #[error(transparent)]
    Dump(#[from] dump::Error),
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Milli(#[from] milli::Error),
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
            Error::TaskDeletionWithEmptyQuery => Code::TaskDeletionWithEmptyQuery,
            Error::TaskCancelationWithEmptyQuery => Code::TaskCancelationWithEmptyQuery,
            Error::InvalidStatus(_) => Code::BadRequest,
            Error::InvalidKind(_) => Code::BadRequest,

            Error::Dump(e) => e.error_code(),
            Error::Milli(e) => e.error_code(),
            // TODO: TAMO: are all these errors really internal?
            Error::Heed(_) => Code::Internal,
            Error::FileStore(_) => Code::Internal,
            Error::IoError(_) => Code::Internal,
            Error::Anyhow(_) => Code::Internal,
            Error::CorruptedTaskQueue => Code::Internal,
            Error::CorruptedDump => Code::Internal,
        }
    }
}

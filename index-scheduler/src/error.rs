use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::{heed, milli};
use thiserror::Error;

use crate::TaskId;

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
pub enum Error {
    #[error("Index `{0}` not found.")]
    IndexNotFound(String),
    #[error(
        "Indexes {} not found.",
        .0.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", ")
    )]
    IndexesNotFound(Vec<String>),
    #[error("Index `{0}` already exists.")]
    IndexAlreadyExists(String),
    #[error(
        "Indexes must be declared only once during a swap. `{0}` was specified several times."
    )]
    SwapDuplicateIndexFound(String),
    #[error(
        "Indexes must be declared only once during a swap. {} were specified several times.",
        .0.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", ")
    )]
    SwapDuplicateIndexesFound(Vec<String>),
    #[error("Corrupted dump.")]
    CorruptedDump,
    #[error("Task `{0}` not found.")]
    TaskNotFound(TaskId),
    #[error("Query parameters to filter the tasks to delete are missing. Available query parameters are: `uid`, `indexUid`, `status`, `type`.")]
    TaskDeletionWithEmptyQuery,
    #[error("Query parameters to filter the tasks to cancel are missing. Available query parameters are: `uid`, `indexUid`, `status`, `type`.")]
    TaskCancelationWithEmptyQuery,

    #[error(transparent)]
    Dump(#[from] dump::Error),
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Milli(#[from] milli::Error),
    #[error("An unexpected crash occurred when processing the task.")]
    ProcessBatchPanicked,
    #[error(transparent)]
    FileStore(#[from] file_store::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    Persist(#[from] tempfile::PersistError),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    // Irrecoverable errors:
    #[error(transparent)]
    CreateBatch(Box<Self>),
    #[error("Corrupted task queue.")]
    CorruptedTaskQueue,
    #[error(transparent)]
    TaskDatabaseUpdate(Box<Self>),
    #[error(transparent)]
    HeedTransaction(heed::Error),
}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        match self {
            Error::IndexNotFound(_) => Code::IndexNotFound,
            Error::IndexesNotFound(_) => Code::IndexNotFound,
            Error::IndexAlreadyExists(_) => Code::IndexAlreadyExists,
            Error::SwapDuplicateIndexesFound(_) => Code::BadRequest,
            Error::SwapDuplicateIndexFound(_) => Code::BadRequest,
            Error::TaskNotFound(_) => Code::TaskNotFound,
            Error::TaskDeletionWithEmptyQuery => Code::TaskDeletionWithEmptyQuery,
            Error::TaskCancelationWithEmptyQuery => Code::TaskCancelationWithEmptyQuery,
            Error::Dump(e) => e.error_code(),
            Error::Milli(e) => e.error_code(),
            Error::ProcessBatchPanicked => Code::Internal,
            // TODO: TAMO: are all these errors really internal?
            Error::Heed(_) => Code::Internal,
            Error::FileStore(_) => Code::Internal,
            Error::IoError(_) => Code::Internal,
            Error::Persist(_) => Code::Internal,
            Error::Anyhow(_) => Code::Internal,
            Error::CorruptedTaskQueue => Code::Internal,
            Error::CorruptedDump => Code::Internal,
            Error::TaskDatabaseUpdate(_) => Code::Internal,
            Error::CreateBatch(_) => Code::Internal,
            Error::HeedTransaction(_) => Code::Internal,
        }
    }
}

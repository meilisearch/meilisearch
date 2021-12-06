use meilisearch_error::{internal_error, Code, ErrorCode};
use tokio::task::JoinError;

use crate::update_file_store::UpdateFileStoreError;

use super::task::TaskId;

pub type Result<T> = std::result::Result<T, TaskError>;

#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("Task `{0}` not found.")]
    UnexistingTask(TaskId),
    #[error("Internal error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
}

internal_error!(
    TaskError: heed::Error,
    JoinError,
    std::io::Error,
    serde_json::Error,
    UpdateFileStoreError
);

impl ErrorCode for TaskError {
    fn error_code(&self) -> Code {
        match self {
            TaskError::UnexistingTask(_) => Code::TaskNotFound,
            TaskError::Internal(_) => Code::Internal,
        }
    }
}

use std::error::Error;

use meilisearch_error::Code;
use meilisearch_error::ErrorCode;
use tokio::task::JoinError;

use crate::document_formats::DocumentFormatError;
use crate::index::error::IndexError;
use crate::tasks::error::TaskError;
use super::update_file_store::UpdateFileStoreError;

// use super::dump_actor::error::DumpActorError;
use super::index_resolver::error::IndexResolverError;

pub type Result<T> = std::result::Result<T, IndexControllerError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexControllerError {
    #[error("Index creation must have an uid")]
    MissingUid,
    #[error("{0}")]
    IndexResolver(#[from] IndexResolverError),
    #[error("{0}")]
    IndexError(#[from] IndexError),
    #[error("Internal error: {0}")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
    #[error("{0}")]
    TaskError(#[from] TaskError),
    #[error("{0}")]
    DocumentFormatError(#[from] DocumentFormatError),

}

internal_error!(IndexControllerError:
    JoinError, UpdateFileStoreError
);

impl ErrorCode for IndexControllerError {
    fn error_code(&self) -> Code {
        match self {
            IndexControllerError::MissingUid => Code::BadRequest,
            IndexControllerError::IndexResolver(e) => e.error_code(),
            IndexControllerError::IndexError(e) => e.error_code(),
            IndexControllerError::Internal(_) => Code::Internal,
            IndexControllerError::TaskError(e) => e.error_code(),
            IndexControllerError::DocumentFormatError(e) => e.error_code(),
        }
    }
}

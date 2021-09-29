use std::error::Error;

use meilisearch_error::Code;
use meilisearch_error::ErrorCode;
use tokio::task::JoinError;

use crate::index::error::IndexError;

use super::dump_actor::error::DumpActorError;
use super::index_resolver::error::IndexResolverError;
use super::updates::error::UpdateLoopError;

pub type Result<T> = std::result::Result<T, IndexControllerError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexControllerError {
    #[error("Index creation must have an uid")]
    MissingUid,
    #[error("{0}")]
    IndexResolver(#[from] IndexResolverError),
    #[error("{0}")]
    UpdateLoop(#[from] UpdateLoopError),
    #[error("{0}")]
    DumpActor(#[from] DumpActorError),
    #[error("{0}")]
    IndexError(#[from] IndexError),
    #[error("Internal error: {0}")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
}

internal_error!(IndexControllerError: JoinError);

impl ErrorCode for IndexControllerError {
    fn error_code(&self) -> Code {
        match self {
            IndexControllerError::MissingUid => Code::BadRequest,
            IndexControllerError::IndexResolver(e) => e.error_code(),
            IndexControllerError::UpdateLoop(e) => e.error_code(),
            IndexControllerError::DumpActor(e) => e.error_code(),
            IndexControllerError::IndexError(e) => e.error_code(),
            IndexControllerError::Internal(_) => Code::Internal,
        }
    }
}

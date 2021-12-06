use meilisearch_auth::error::AuthControllerError;
use meilisearch_error::{internal_error, Code, ErrorCode};

use crate::{index_resolver::error::IndexResolverError, tasks::error::TaskError};

pub type Result<T> = std::result::Result<T, DumpActorError>;

#[derive(thiserror::Error, Debug)]
pub enum DumpActorError {
    #[error("A dump is already processing. You must wait until the current process is finished before requesting another dump.")]
    DumpAlreadyRunning,
    #[error("Dump `{0}` not found.")]
    DumpDoesNotExist(String),
    #[error("An internal error has occurred. `{0}`.")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    IndexResolver(#[from] IndexResolverError),
}

internal_error!(
    DumpActorError: heed::Error,
    std::io::Error,
    tokio::task::JoinError,
    tokio::sync::oneshot::error::RecvError,
    serde_json::error::Error,
    tempfile::PersistError,
    fs_extra::error::Error,
    AuthControllerError,
    TaskError
);

impl ErrorCode for DumpActorError {
    fn error_code(&self) -> Code {
        match self {
            DumpActorError::DumpAlreadyRunning => Code::DumpAlreadyInProgress,
            DumpActorError::DumpDoesNotExist(_) => Code::DumpNotFound,
            DumpActorError::Internal(_) => Code::Internal,
            DumpActorError::IndexResolver(e) => e.error_code(),
        }
    }
}

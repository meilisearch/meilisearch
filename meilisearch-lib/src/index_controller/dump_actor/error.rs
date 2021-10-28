use meilisearch_error::{Code, ErrorCode};

use crate::index_controller::index_resolver::error::IndexResolverError;
use crate::index_controller::updates::error::UpdateLoopError;

pub type Result<T> = std::result::Result<T, DumpActorError>;

#[derive(thiserror::Error, Debug)]
pub enum DumpActorError {
    #[error("Another dump is already in progress")]
    DumpAlreadyRunning,
    #[error("Dump `{0}` not found.")]
    DumpDoesNotExist(String),
    #[error("Internal error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    IndexResolver(#[from] IndexResolverError),
    #[error("{0}")]
    UpdateLoop(#[from] UpdateLoopError),
}

macro_rules! internal_error {
    ($($other:path), *) => {
        $(
            impl From<$other> for DumpActorError {
                fn from(other: $other) -> Self {
                    Self::Internal(Box::new(other))
                }
            }
        )*
    }
}

internal_error!(
    heed::Error,
    std::io::Error,
    tokio::task::JoinError,
    serde_json::error::Error,
    tempfile::PersistError
);

impl ErrorCode for DumpActorError {
    fn error_code(&self) -> Code {
        match self {
            DumpActorError::DumpAlreadyRunning => Code::DumpAlreadyInProgress,
            DumpActorError::DumpDoesNotExist(_) => Code::NotFound,
            DumpActorError::Internal(_) => Code::Internal,
            DumpActorError::IndexResolver(e) => e.error_code(),
            DumpActorError::UpdateLoop(e) => e.error_code(),
        }
    }
}

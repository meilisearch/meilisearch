use meilisearch_auth::error::AuthControllerError;
use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::internal_error;

use crate::{index_resolver::error::IndexResolverError, tasks::error::TaskError};

pub type Result<T> = std::result::Result<T, DumpError>;

#[derive(thiserror::Error, Debug)]
pub enum DumpError {
    #[error("An internal error has occurred. `{0}`.")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    IndexResolver(Box<IndexResolverError>),
}

internal_error!(
    DumpError: milli::heed::Error,
    std::io::Error,
    tokio::task::JoinError,
    tokio::sync::oneshot::error::RecvError,
    serde_json::error::Error,
    tempfile::PersistError,
    fs_extra::error::Error,
    AuthControllerError,
    TaskError
);

impl From<IndexResolverError> for DumpError {
    fn from(e: IndexResolverError) -> Self {
        Self::IndexResolver(Box::new(e))
    }
}

impl ErrorCode for DumpError {
    fn error_code(&self) -> Code {
        match self {
            DumpError::Internal(_) => Code::Internal,
            DumpError::IndexResolver(e) => e.error_code(),
        }
    }
}

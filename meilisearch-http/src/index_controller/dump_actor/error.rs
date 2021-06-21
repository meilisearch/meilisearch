use meilisearch_error::{Code, ErrorCode};

use crate::index_controller::update_actor::error::UpdateActorError;
use crate::index_controller::uuid_resolver::error::UuidResolverError;

pub type Result<T> = std::result::Result<T, DumpActorError>;

#[derive(thiserror::Error, Debug)]
pub enum DumpActorError {
    #[error("dump already running")]
    DumpAlreadyRunning,
    #[error("dump `{0}` does not exist")]
    DumpDoesNotExist(String),
    #[error("internal error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("error while dumping uuids: {0}")]
    UuidResolver(#[from] UuidResolverError),
    #[error("error while dumping updates: {0}")]
    UpdateActor(#[from] UpdateActorError),
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
            DumpActorError::DumpDoesNotExist(_) => Code::DocumentNotFound,
            DumpActorError::Internal(_) => Code::Internal,
            DumpActorError::UuidResolver(e) => e.error_code(),
            DumpActorError::UpdateActor(e) => e.error_code(),
        }
    }
}

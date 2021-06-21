use meilisearch_error::Code;
use meilisearch_error::ErrorCode;

use crate::index::error::IndexError;

use super::dump_actor::error::DumpActorError;
use super::index_actor::error::IndexActorError;
use super::update_actor::error::UpdateActorError;
use super::uuid_resolver::error::UuidResolverError;

pub type Result<T> = std::result::Result<T, IndexControllerError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexControllerError {
    #[error("missing index uid")]
    MissingUid,
    #[error("index resolution error: {0}")]
    Uuid(#[from] UuidResolverError),
    #[error("error with index: {0}")]
    IndexActor(#[from] IndexActorError),
    #[error("error with update: {0}")]
    UpdateActor(#[from] UpdateActorError),
    #[error("error with dump: {0}")]
    DumpActor(#[from] DumpActorError),
    #[error("error with index: {0}")]
    IndexError(#[from] IndexError),
}

impl ErrorCode for IndexControllerError {
    fn error_code(&self) -> Code {
        match self {
            IndexControllerError::MissingUid => Code::InvalidIndexUid,
            IndexControllerError::Uuid(e) => e.error_code(),
            IndexControllerError::IndexActor(e) => e.error_code(),
            IndexControllerError::UpdateActor(e) => e.error_code(),
            IndexControllerError::DumpActor(e) => e.error_code(),
            IndexControllerError::IndexError(e) => e.error_code(),
        }
    }
}

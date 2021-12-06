use std::error::Error;

use meilisearch_error::Code;
use meilisearch_error::{internal_error, ErrorCode};
use tokio::task::JoinError;

use super::DocumentAdditionFormat;
use crate::document_formats::DocumentFormatError;
use crate::index::error::IndexError;
use crate::tasks::error::TaskError;
use crate::update_file_store::UpdateFileStoreError;

use super::dump_actor::error::DumpActorError;
use crate::index_resolver::error::IndexResolverError;

pub type Result<T> = std::result::Result<T, IndexControllerError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexControllerError {
    #[error("Index creation must have an uid")]
    MissingUid,
    #[error("{0}")]
    IndexResolver(#[from] IndexResolverError),
    #[error("{0}")]
    IndexError(#[from] IndexError),
    #[error("An internal error has occurred. `{0}`.")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
    #[error("{0}")]
    TaskError(#[from] TaskError),
    #[error("{0}")]
    DumpError(#[from] DumpActorError),
    #[error("{0}")]
    DocumentFormatError(#[from] DocumentFormatError),
    #[error("A {0} payload is missing.")]
    MissingPayload(DocumentAdditionFormat),
    #[error("The provided payload reached the size limit.")]
    PayloadTooLarge,
}

internal_error!(IndexControllerError: JoinError, UpdateFileStoreError);

impl From<actix_web::error::PayloadError> for IndexControllerError {
    fn from(other: actix_web::error::PayloadError) -> Self {
        match other {
            actix_web::error::PayloadError::Overflow => Self::PayloadTooLarge,
            _ => Self::Internal(Box::new(other)),
        }
    }
}

impl ErrorCode for IndexControllerError {
    fn error_code(&self) -> Code {
        match self {
            IndexControllerError::MissingUid => Code::BadRequest,
            IndexControllerError::IndexResolver(e) => e.error_code(),
            IndexControllerError::IndexError(e) => e.error_code(),
            IndexControllerError::Internal(_) => Code::Internal,
            IndexControllerError::TaskError(e) => e.error_code(),
            IndexControllerError::DocumentFormatError(e) => e.error_code(),
            IndexControllerError::MissingPayload(_) => Code::MissingPayload,
            IndexControllerError::PayloadTooLarge => Code::PayloadTooLarge,
            IndexControllerError::DumpError(DumpActorError::DumpAlreadyRunning) => {
                Code::DumpAlreadyInProgress
            }
            IndexControllerError::DumpError(_) => Code::DumpProcessFailed,
        }
    }
}

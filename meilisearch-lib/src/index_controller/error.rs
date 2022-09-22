use std::error::Error;

use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::index_uid::IndexUidFormatError;
use meilisearch_types::internal_error;
use tokio::task::JoinError;

use super::DocumentAdditionFormat;
use crate::document_formats::DocumentFormatError;
// use crate::dump::error::DumpError;
use index::error::IndexError;

pub type Result<T> = std::result::Result<T, IndexControllerError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexControllerError {
    #[error("Index creation must have an uid")]
    MissingUid,
    #[error(transparent)]
    IndexResolver(#[from] index_scheduler::Error),
    #[error(transparent)]
    IndexError(#[from] IndexError),
    #[error("An internal error has occurred. `{0}`.")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
    // #[error("{0}")]
    // DumpError(#[from] DumpError),
    #[error(transparent)]
    DocumentFormatError(#[from] DocumentFormatError),
    #[error("A {0} payload is missing.")]
    MissingPayload(DocumentAdditionFormat),
    #[error("The provided payload reached the size limit.")]
    PayloadTooLarge,
}

internal_error!(IndexControllerError: JoinError, file_store::Error);

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
            IndexControllerError::Internal(_) => Code::Internal,
            IndexControllerError::DocumentFormatError(e) => e.error_code(),
            IndexControllerError::MissingPayload(_) => Code::MissingPayload,
            IndexControllerError::PayloadTooLarge => Code::PayloadTooLarge,
            IndexControllerError::IndexResolver(e) => e.error_code(),
            IndexControllerError::IndexError(e) => e.error_code(),
        }
    }
}

/*
impl From<IndexUidFormatError> for IndexControllerError {
    fn from(err: IndexUidFormatError) -> Self {
        index_scheduler::Error::from(err).into()
    }
}
*/

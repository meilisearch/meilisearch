use std::error::Error;
use std::fmt;

use meilisearch_error::{internal_error, Code, ErrorCode};

use crate::{
    document_formats::DocumentFormatError,
    index::error::IndexError,
    index_controller::{update_file_store::UpdateFileStoreError, DocumentAdditionFormat},
};

pub type Result<T> = std::result::Result<T, UpdateLoopError>;

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum UpdateLoopError {
    #[error("Task `{0}` not found.")]
    UnexistingUpdate(u64),
    #[error("An internal error has occurred. `{0}`.")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
    #[error(
        "update store was shut down due to a fatal error, please check your logs for more info."
    )]
    FatalUpdateStoreError,
    #[error("{0}")]
    DocumentFormatError(#[from] DocumentFormatError),
    #[error("The provided payload reached the size limit.")]
    PayloadTooLarge,
    #[error("A {0} payload is missing.")]
    MissingPayload(DocumentAdditionFormat),
    #[error("{0}")]
    IndexError(#[from] IndexError),
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for UpdateLoopError
where
    T: Sync + Send + 'static + fmt::Debug,
{
    fn from(other: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::Internal(Box::new(other))
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for UpdateLoopError {
    fn from(other: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::Internal(Box::new(other))
    }
}

impl From<actix_web::error::PayloadError> for UpdateLoopError {
    fn from(other: actix_web::error::PayloadError) -> Self {
        match other {
            actix_web::error::PayloadError::Overflow => Self::PayloadTooLarge,
            _ => Self::Internal(Box::new(other)),
        }
    }
}

internal_error!(
    UpdateLoopError: heed::Error,
    std::io::Error,
    serde_json::Error,
    tokio::task::JoinError,
    UpdateFileStoreError
);

impl ErrorCode for UpdateLoopError {
    fn error_code(&self) -> Code {
        match self {
            Self::UnexistingUpdate(_) => Code::TaskNotFound,
            Self::Internal(_) => Code::Internal,
            Self::FatalUpdateStoreError => Code::Internal,
            Self::DocumentFormatError(error) => error.error_code(),
            Self::PayloadTooLarge => Code::PayloadTooLarge,
            Self::MissingPayload(_) => Code::MissingPayload,
            Self::IndexError(e) => e.error_code(),
        }
    }
}

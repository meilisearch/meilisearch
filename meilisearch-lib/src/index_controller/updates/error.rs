use std::error::Error;
use std::fmt;

use meilisearch_error::{Code, ErrorCode};

use crate::{
    document_formats::DocumentFormatError,
    index_controller::update_file_store::UpdateFileStoreError,
};

pub type Result<T> = std::result::Result<T, UpdateLoopError>;

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum UpdateLoopError {
    #[error("Update {0} not found.")]
    UnexistingUpdate(u64),
    #[error("Internal error: {0}")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
    #[error(
        "update store was shut down due to a fatal error, please check your logs for more info."
    )]
    FatalUpdateStoreError,
    #[error("{0}")]
    DocumentFormatError(#[from] DocumentFormatError),
    // TODO: The reference to actix has to go.
    #[error("{0}")]
    PayloadError(#[from] actix_web::error::PayloadError),
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
            Self::UnexistingUpdate(_) => Code::NotFound,
            Self::Internal(_) => Code::Internal,
            //Self::IndexActor(e) => e.error_code(),
            Self::FatalUpdateStoreError => Code::Internal,
            Self::DocumentFormatError(error) => error.error_code(),
            Self::PayloadError(error) => match error {
                actix_web::error::PayloadError::Overflow => Code::PayloadTooLarge,
                _ => Code::Internal,
            },
        }
    }
}

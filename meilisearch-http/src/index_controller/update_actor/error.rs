use std::error::Error;

use meilisearch_error::{Code, ErrorCode};

use crate::index_controller::index_actor::error::IndexActorError;

pub type Result<T> = std::result::Result<T, UpdateActorError>;

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum UpdateActorError {
    #[error("Update {0} not found.")]
    UnexistingUpdate(u64),
    #[error("Internal error: {0}")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
    #[error("{0}")]
    IndexActor(#[from] IndexActorError),
    #[error(
        "update store was shut down due to a fatal error, please check your logs for more info."
    )]
    FatalUpdateStoreError,
    #[error("{0}")]
    InvalidPayload(Box<dyn Error + Send + Sync + 'static>),
    #[error("{0}")]
    PayloadError(#[from] actix_web::error::PayloadError),
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for UpdateActorError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::FatalUpdateStoreError
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for UpdateActorError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::FatalUpdateStoreError
    }
}

internal_error!(
    UpdateActorError: heed::Error,
    std::io::Error,
    serde_json::Error,
    tokio::task::JoinError
);

impl ErrorCode for UpdateActorError {
    fn error_code(&self) -> Code {
        match self {
            UpdateActorError::UnexistingUpdate(_) => Code::NotFound,
            UpdateActorError::Internal(_) => Code::Internal,
            UpdateActorError::IndexActor(e) => e.error_code(),
            UpdateActorError::FatalUpdateStoreError => Code::Internal,
            UpdateActorError::InvalidPayload(_) => Code::BadRequest,
            UpdateActorError::PayloadError(error) => match error {
                actix_web::error::PayloadError::Overflow => Code::PayloadTooLarge,
                _ => Code::Internal,
            },
        }
    }
}

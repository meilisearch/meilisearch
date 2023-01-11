use std::error::Error;

use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::internal_error;

pub type Result<T> = std::result::Result<T, AuthControllerError>;

#[derive(Debug, thiserror::Error)]
pub enum AuthControllerError {
    #[error("API key `{0}` not found.")]
    ApiKeyNotFound(String),
    #[error("`uid` field value `{0}` is already an existing API key.")]
    ApiKeyAlreadyExists(String),
    #[error("Internal error: {0}")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
}

internal_error!(
    AuthControllerError: meilisearch_types::milli::heed::Error,
    std::io::Error,
    serde_json::Error,
    std::str::Utf8Error
);

impl ErrorCode for AuthControllerError {
    fn error_code(&self) -> Code {
        match self {
            Self::ApiKeyNotFound(_) => Code::ApiKeyNotFound,
            Self::ApiKeyAlreadyExists(_) => Code::ApiKeyAlreadyExists,
            Self::Internal(_) => Code::Internal,
        }
    }
}

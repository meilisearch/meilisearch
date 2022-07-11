use std::error::Error;

use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::internal_error;
use serde_json::Value;

pub type Result<T> = std::result::Result<T, AuthControllerError>;

#[derive(Debug, thiserror::Error)]
pub enum AuthControllerError {
    #[error("`{0}` field is mandatory.")]
    MissingParameter(&'static str),
    #[error("`actions` field value `{0}` is invalid. It should be an array of string representing action names.")]
    InvalidApiKeyActions(Value),
    #[error("`indexes` field value `{0}` is invalid. It should be an array of string representing index names.")]
    InvalidApiKeyIndexes(Value),
    #[error("`expiresAt` field value `{0}` is invalid. It should follow the RFC 3339 format to represents a date or datetime in the future or specified as a null value. e.g. 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.")]
    InvalidApiKeyExpiresAt(Value),
    #[error("`description` field value `{0}` is invalid. It should be a string or specified as a null value.")]
    InvalidApiKeyDescription(Value),
    #[error(
        "`name` field value `{0}` is invalid. It should be a string or specified as a null value."
    )]
    InvalidApiKeyName(Value),
    #[error("`uid` field value `{0}` is invalid. It should be a valid UUID v4 string or omitted.")]
    InvalidApiKeyUid(Value),
    #[error("API key `{0}` not found.")]
    ApiKeyNotFound(String),
    #[error("`uid` field value `{0}` is already an existing API key.")]
    ApiKeyAlreadyExists(String),
    #[error("The `{0}` field cannot be modified for the given resource.")]
    ImmutableField(String),
    #[error("Internal error: {0}")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
}

internal_error!(
    AuthControllerError: milli::heed::Error,
    std::io::Error,
    serde_json::Error,
    std::str::Utf8Error
);

impl ErrorCode for AuthControllerError {
    fn error_code(&self) -> Code {
        match self {
            Self::MissingParameter(_) => Code::MissingParameter,
            Self::InvalidApiKeyActions(_) => Code::InvalidApiKeyActions,
            Self::InvalidApiKeyIndexes(_) => Code::InvalidApiKeyIndexes,
            Self::InvalidApiKeyExpiresAt(_) => Code::InvalidApiKeyExpiresAt,
            Self::InvalidApiKeyDescription(_) => Code::InvalidApiKeyDescription,
            Self::InvalidApiKeyName(_) => Code::InvalidApiKeyName,
            Self::ApiKeyNotFound(_) => Code::ApiKeyNotFound,
            Self::InvalidApiKeyUid(_) => Code::InvalidApiKeyUid,
            Self::ApiKeyAlreadyExists(_) => Code::ApiKeyAlreadyExists,
            Self::ImmutableField(_) => Code::ImmutableField,
            Self::Internal(_) => Code::Internal,
        }
    }
}

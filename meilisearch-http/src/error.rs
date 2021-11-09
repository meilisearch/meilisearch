use std::error::Error;
use std::fmt;

use actix_web as aweb;
use aweb::error::{JsonPayloadError, QueryPayloadError};
use meilisearch_error::{Code, ErrorCode, ResponseError};

#[derive(Debug, thiserror::Error)]
pub enum MeilisearchHttpError {
    #[error("A Content-Type header is missing. Accepted values for the Content-Type header are: {}",
            .0.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", "))]
    MissingContentType(Vec<String>),
    #[error(
        "The Content-Type `{0}` is invalid. Accepted values for the Content-Type header are: {}",
        .1.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", ")
    )]
    InvalidContentType(String, Vec<String>),
}

impl ErrorCode for MeilisearchHttpError {
    fn error_code(&self) -> Code {
        match self {
            MeilisearchHttpError::MissingContentType(_) => Code::MissingContentType,
            MeilisearchHttpError::InvalidContentType(_, _) => Code::InvalidContentType,
        }
    }
}

impl From<MeilisearchHttpError> for aweb::Error {
    fn from(other: MeilisearchHttpError) -> Self {
        aweb::Error::from(ResponseError::from(other))
    }
}

impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadError::Json(e) => e.fmt(f),
            PayloadError::Query(e) => e.fmt(f),
        }
    }
}

#[derive(Debug)]
pub enum PayloadError {
    Json(JsonPayloadError),
    Query(QueryPayloadError),
}

impl Error for PayloadError {}

impl ErrorCode for PayloadError {
    fn error_code(&self) -> Code {
        match self {
            PayloadError::Json(err) => match err {
                JsonPayloadError::Overflow { .. } => Code::PayloadTooLarge,
                JsonPayloadError::ContentType => Code::UnsupportedMediaType,
                JsonPayloadError::Payload(aweb::error::PayloadError::Overflow) => {
                    Code::PayloadTooLarge
                }
                JsonPayloadError::Deserialize(_) | JsonPayloadError::Payload(_) => Code::BadRequest,
                JsonPayloadError::Serialize(_) => Code::Internal,
                _ => Code::Internal,
            },
            PayloadError::Query(err) => match err {
                QueryPayloadError::Deserialize(_) => Code::BadRequest,
                _ => Code::Internal,
            },
        }
    }
}

impl From<JsonPayloadError> for PayloadError {
    fn from(other: JsonPayloadError) -> Self {
        Self::Json(other)
    }
}

impl From<QueryPayloadError> for PayloadError {
    fn from(other: QueryPayloadError) -> Self {
        Self::Query(other)
    }
}

impl From<PayloadError> for aweb::Error {
    fn from(other: PayloadError) -> Self {
        aweb::Error::from(ResponseError::from(other))
    }
}

use std::error::Error;
use std::fmt;

use actix_web as aweb;
use actix_web::body::Body;
use actix_web::dev::BaseHttpResponseBuilder;
use actix_web::http::StatusCode;
use meilisearch_error::{Code, ErrorCode};
use milli::UserError;
use serde::{Serialize, Deserialize};

#[derive(Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error("you must have an authorization token")]
    MissingAuthorizationHeader,
    #[error("invalid API key")]
    InvalidToken(String),
}

impl ErrorCode for AuthenticationError {
    fn error_code(&self) -> Code {
        match self {
            AuthenticationError::MissingAuthorizationHeader => Code::MissingAuthorizationHeader,
            AuthenticationError::InvalidToken(_) => Code::InvalidToken,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResponseError {
    #[serde(skip)]
    code: StatusCode,
    message: String,
    error_code: String,
    error_type: String,
    error_link: String,
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

impl<T> From<T> for ResponseError
where T: ErrorCode
{
    fn from(other: T) -> Self {
        Self {
            code: other.http_status(),
            message: other.to_string(),
            error_code: other.error_name(),
            error_type: other.error_type(),
            error_link: other.error_url(),
        }
    }
}

impl aweb::error::ResponseError for ResponseError {
    fn error_response(&self) -> aweb::BaseHttpResponse<Body> {
        let json = serde_json::to_vec(self).unwrap();
        BaseHttpResponseBuilder::new(self.status_code())
            .content_type("application/json")
            .body(json)
    }

    fn status_code(&self) -> StatusCode {
        self.code
    }
}

#[derive(Debug)]
struct PayloadError<E>(E);

impl<E: Error> fmt::Display for PayloadError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl<E: Error> Error for PayloadError<E> {}

impl<E: Error> ErrorCode for PayloadError<E> {
    fn error_code(&self) -> Code {
        Code::Internal
    }
}

macro_rules! internal_error {
    ($target:ty : $($other:path), *) => {
        $(
            impl From<$other> for $target {
                fn from(other: $other) -> Self {
                    Self::Internal(Box::new(other))
                }
            }
        )*
    }
}

#[derive(Debug)]
pub struct MilliError<'a>(pub &'a milli::Error);

impl Error for MilliError<'_> {}

impl fmt::Display for MilliError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ErrorCode for MilliError<'_> {
    fn error_code(&self) -> Code {
        match self.0 {
            milli::Error::InternalError(_) => Code::Internal,
            milli::Error::IoError(_) => Code::Internal,
            milli::Error::UserError(ref error) => {
                match error {
                    // TODO: wait for spec for new error codes.
                    UserError::AttributeLimitReached
                    | UserError::Csv(_)
                    | UserError::SerdeJson(_)
                    | UserError::MaxDatabaseSizeReached
                    | UserError::InvalidCriterionName { .. }
                    | UserError::InvalidDocumentId { .. }
                    | UserError::InvalidStoreFile
                    | UserError::NoSpaceLeftOnDevice
                    | UserError::DocumentLimitReached => Code::Internal,
                    UserError::InvalidFilter(_) => Code::Filter,
                    UserError::InvalidFilterAttribute(_) => Code::Filter,
                    UserError::MissingDocumentId { .. } => Code::MissingDocumentId,
                    UserError::MissingPrimaryKey => Code::MissingPrimaryKey,
                    UserError::PrimaryKeyCannotBeChanged => Code::PrimaryKeyAlreadyPresent,
                    UserError::PrimaryKeyCannotBeReset => Code::PrimaryKeyAlreadyPresent,
                    UserError::UnknownInternalDocumentId { .. } => Code::DocumentNotFound,
                }
            }
        }
    }
}

pub fn payload_error_handler<E>(err: E) -> ResponseError
where
    E: Error + Sync + Send + 'static,
{
    let error = PayloadError(err);
    error.into()
}

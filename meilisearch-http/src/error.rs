use std::error;
use std::error::Error;
use std::fmt;

use actix_web as aweb;
use actix_web::body::Body;
use actix_web::dev::BaseHttpResponseBuilder;
use actix_web::http::StatusCode;
use meilisearch_error::{Code, ErrorCode};
use milli::UserError;
use serde::ser::{Serialize, SerializeStruct, Serializer};

use crate::index_controller::error::IndexControllerError;

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

#[derive(Debug)]
pub struct ResponseError {
    inner: Box<dyn ErrorCode>,
}

impl error::Error for ResponseError {}

impl ErrorCode for ResponseError {
    fn error_code(&self) -> Code {
        self.inner.error_code()
    }
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

macro_rules! response_error {
    ($($other:path), *) => {
        $(
            impl From<$other> for ResponseError {
                fn from(error: $other) -> ResponseError {
                    ResponseError {
                        inner: Box::new(error),
                    }
                }
            }

        )*
    };
}

response_error!(IndexControllerError, AuthenticationError);

impl Serialize for ResponseError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let struct_name = "ResponseError";
        let field_count = 4;

        let mut state = serializer.serialize_struct(struct_name, field_count)?;
        state.serialize_field("message", &self.to_string())?;
        state.serialize_field("errorCode", &self.error_name())?;
        state.serialize_field("errorType", &self.error_type())?;
        state.serialize_field("errorLink", &self.error_url())?;
        state.end()
    }
}

impl aweb::error::ResponseError for ResponseError {
    fn error_response(&self) -> aweb::BaseHttpResponse<Body> {
        let json = serde_json::to_vec(self).unwrap();
        BaseHttpResponseBuilder::new(self.status_code()).body(json)
    }

    fn status_code(&self) -> StatusCode {
        self.http_status()
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

impl<E> From<PayloadError<E>> for ResponseError
where
    E: Error + Sync + Send + 'static,
{
    fn from(other: PayloadError<E>) -> Self {
        ResponseError {
            inner: Box::new(other),
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
                    | UserError::DocumentLimitReached => todo!(),
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

use std::error::Error;
use std::fmt;

use actix_web as aweb;
use actix_web::body::Body;
use actix_web::http::StatusCode;
use actix_web::HttpResponseBuilder;
use aweb::error::{JsonPayloadError, QueryPayloadError};
use meilisearch_error::{Code, ErrorCode};
use milli::UserError;
use serde::{Deserialize, Serialize};

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
where
    T: ErrorCode,
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
    fn error_response(&self) -> aweb::HttpResponse<Body> {
        let json = serde_json::to_vec(self).unwrap();
        HttpResponseBuilder::new(self.status_code())
            .content_type("application/json")
            .body(json)
    }

    fn status_code(&self) -> StatusCode {
        self.code
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
                    UserError::Csv(_)
                    | UserError::SerdeJson(_)
                    | UserError::MaxDatabaseSizeReached
                    | UserError::InvalidDocumentId { .. }
                    | UserError::InvalidStoreFile
                    | UserError::NoSpaceLeftOnDevice
                    | UserError::InvalidAscDescSyntax { .. }
                    | UserError::DocumentLimitReached => Code::Internal,
                    UserError::AttributeLimitReached => Code::MaxFieldsLimitExceeded,
                    UserError::InvalidFilter(_) => Code::Filter,
                    UserError::InvalidFilterAttribute(_) => Code::Filter,
                    UserError::InvalidSortName { .. } => Code::Sort,
                    UserError::MissingDocumentId { .. } => Code::MissingDocumentId,
                    UserError::MissingPrimaryKey => Code::MissingPrimaryKey,
                    UserError::PrimaryKeyCannotBeChanged => Code::PrimaryKeyAlreadyPresent,
                    UserError::PrimaryKeyCannotBeReset => Code::PrimaryKeyAlreadyPresent,
                    UserError::SortRankingRuleMissing => Code::Sort,
                    UserError::UnknownInternalDocumentId { .. } => Code::DocumentNotFound,
                    UserError::InvalidFacetsDistribution { .. } => Code::BadRequest,
                    UserError::InvalidSortableAttribute { .. } => Code::Sort,

                    UserError::InvalidRankingRuleName { .. } => Code::BadRequest,
                    UserError::InvalidGeoField { .. } => Code::InvalidGeoField,
                    UserError::InvalidReservedRankingRuleName { .. } => Code::BadRequest,
                }
            }
        }
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

pub fn payload_error_handler<E>(err: E) -> ResponseError
where
    E: Into<PayloadError>,
{
    err.into().into()
}

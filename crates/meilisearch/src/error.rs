use actix_web as aweb;
use aweb::error::{JsonPayloadError, QueryPayloadError};
use byte_unit::{Byte, UnitType};
use meilisearch_types::document_formats::{DocumentFormatError, PayloadType};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use meilisearch_types::index_uid::{IndexUid, IndexUidFormatError};
use meilisearch_types::milli;
use meilisearch_types::milli::OrderBy;
use serde_json::Value;
use tokio::task::JoinError;

#[derive(Debug, thiserror::Error)]
pub enum MeilisearchHttpError {
    #[error("A Content-Type header is missing. Accepted values for the Content-Type header are: {}",
            .0.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", "))]
    MissingContentType(Vec<String>),
    #[error("The `/logs/stream` route is currently in use by someone else.")]
    AlreadyUsedLogRoute,
    #[error("The Content-Type `{0}` does not support the use of a csv delimiter. The csv delimiter can only be used with the Content-Type `text/csv`.")]
    CsvDelimiterWithWrongContentType(String),
    #[error(
        "The Content-Type `{}` is invalid. Accepted values for the Content-Type header are: {}",
        .0, .1.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", ")
    )]
    InvalidContentType(String, Vec<String>),
    #[error("Document `{0}` not found.")]
    DocumentNotFound(String),
    #[error("Sending an empty filter is forbidden.")]
    EmptyFilter,
    #[error("Invalid syntax for the filter parameter: `expected {}, found: {}`.", .0.join(", "), .1)]
    InvalidExpression(&'static [&'static str], Value),
    #[error("Using `federationOptions` is not allowed in a non-federated search.\n - Hint: remove `federationOptions` from query #{0} or add `federation` to the request.")]
    FederationOptionsInNonFederatedRequest(usize),
    #[error("Inside `.queries[{0}]`: Using pagination options is not allowed in federated queries.\n - Hint: remove `{1}` from query #{0} or remove `federation` from the request\n - Hint: pass `federation.limit` and `federation.offset` for pagination in federated search")]
    PaginationInFederatedQuery(usize, &'static str),
    #[error("Inside `.queries[{0}]`: Using facet options is not allowed in federated queries.\n - Hint: remove `facets` from query #{0} or remove `federation` from the request\n - Hint: pass `federation.facetsByIndex.{1}: {2:?}` for facets in federated search")]
    FacetsInFederatedQuery(usize, String, Vec<String>),
    #[error("Inconsistent order for values in facet `{facet}`: index `{previous_uid}` orders {previous_facet_order}, but index `{current_uid}` orders {index_facet_order}.\n - Hint: Remove `federation.mergeFacets` or change `faceting.sortFacetValuesBy` to be consistent in settings.")]
    InconsistentFacetOrder {
        facet: String,
        previous_facet_order: OrderBy,
        previous_uid: String,
        index_facet_order: OrderBy,
        current_uid: String,
    },
    #[error("A {0} payload is missing.")]
    MissingPayload(PayloadType),
    #[error("Too many search requests running at the same time: {0}. Retry after 10s.")]
    TooManySearchRequests(usize),
    #[error("Internal error: Search limiter is down.")]
    SearchLimiterIsDown,
    #[error("The provided payload reached the size limit. The maximum accepted payload size is {}.",  Byte::from_u64(*.0 as u64).get_appropriate_unit(UnitType::Binary))]
    PayloadTooLarge(usize),
    #[error("Two indexes must be given for each swap. The list `[{}]` contains {} indexes.",
        .0.iter().map(|uid| format!("\"{uid}\"")).collect::<Vec<_>>().join(", "), .0.len()
    )]
    SwapIndexPayloadWrongLength(Vec<IndexUid>),
    #[error(transparent)]
    IndexUid(#[from] IndexUidFormatError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    HeedError(#[from] meilisearch_types::heed::Error),
    #[error(transparent)]
    IndexScheduler(#[from] index_scheduler::Error),
    #[error("{}", match .index_name {
        Some(name) if !name.is_empty() => format!("Index `{}`: {error}", name),
        _ => format!("{error}")
    })]
    Milli { error: milli::Error, index_name: Option<String> },
    #[error(transparent)]
    Payload(#[from] PayloadError),
    #[error(transparent)]
    FileStore(#[from] file_store::Error),
    #[error(transparent)]
    DocumentFormat(#[from] DocumentFormatError),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error("Invalid request: missing `hybrid` parameter when `vector` is present.")]
    MissingSearchHybrid,
}

impl MeilisearchHttpError {
    pub(crate) fn from_milli(error: milli::Error, index_name: Option<String>) -> Self {
        Self::Milli { error, index_name }
    }
}

impl ErrorCode for MeilisearchHttpError {
    fn error_code(&self) -> Code {
        match self {
            MeilisearchHttpError::MissingContentType(_) => Code::MissingContentType,
            MeilisearchHttpError::AlreadyUsedLogRoute => Code::BadRequest,
            MeilisearchHttpError::CsvDelimiterWithWrongContentType(_) => Code::InvalidContentType,
            MeilisearchHttpError::MissingPayload(_) => Code::MissingPayload,
            MeilisearchHttpError::InvalidContentType(_, _) => Code::InvalidContentType,
            MeilisearchHttpError::DocumentNotFound(_) => Code::DocumentNotFound,
            MeilisearchHttpError::EmptyFilter => Code::InvalidDocumentFilter,
            MeilisearchHttpError::InvalidExpression(_, _) => Code::InvalidSearchFilter,
            MeilisearchHttpError::PayloadTooLarge(_) => Code::PayloadTooLarge,
            MeilisearchHttpError::TooManySearchRequests(_) => Code::TooManySearchRequests,
            MeilisearchHttpError::SearchLimiterIsDown => Code::Internal,
            MeilisearchHttpError::SwapIndexPayloadWrongLength(_) => Code::InvalidSwapIndexes,
            MeilisearchHttpError::IndexUid(e) => e.error_code(),
            MeilisearchHttpError::SerdeJson(_) => Code::Internal,
            MeilisearchHttpError::HeedError(_) => Code::Internal,
            MeilisearchHttpError::IndexScheduler(e) => e.error_code(),
            MeilisearchHttpError::Milli { error, .. } => error.error_code(),
            MeilisearchHttpError::Payload(e) => e.error_code(),
            MeilisearchHttpError::FileStore(_) => Code::Internal,
            MeilisearchHttpError::DocumentFormat(e) => e.error_code(),
            MeilisearchHttpError::Join(_) => Code::Internal,
            MeilisearchHttpError::MissingSearchHybrid => Code::MissingSearchHybrid,
            MeilisearchHttpError::FederationOptionsInNonFederatedRequest(_) => {
                Code::InvalidMultiSearchFederationOptions
            }
            MeilisearchHttpError::PaginationInFederatedQuery(_, _) => {
                Code::InvalidMultiSearchQueryPagination
            }
            MeilisearchHttpError::FacetsInFederatedQuery(..) => Code::InvalidMultiSearchQueryFacets,
            MeilisearchHttpError::InconsistentFacetOrder { .. } => {
                Code::InvalidMultiSearchFacetOrder
            }
        }
    }
}

impl From<MeilisearchHttpError> for aweb::Error {
    fn from(other: MeilisearchHttpError) -> Self {
        aweb::Error::from(ResponseError::from(other))
    }
}

impl From<aweb::error::PayloadError> for MeilisearchHttpError {
    fn from(error: aweb::error::PayloadError) -> Self {
        match error {
            aweb::error::PayloadError::Incomplete(_) => MeilisearchHttpError::Payload(
                PayloadError::Payload(ActixPayloadError::IncompleteError),
            ),
            _ => MeilisearchHttpError::Payload(PayloadError::Payload(
                ActixPayloadError::OtherError(error),
            )),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ActixPayloadError {
    #[error("The provided payload is incomplete and cannot be parsed")]
    IncompleteError,
    #[error(transparent)]
    OtherError(aweb::error::PayloadError),
}

#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    #[error(transparent)]
    Payload(ActixPayloadError),
    #[error(transparent)]
    Json(JsonPayloadError),
    #[error(transparent)]
    Query(QueryPayloadError),
    #[error("The json payload provided is malformed. `{0}`.")]
    MalformedPayload(serde_json::error::Error),
    #[error("A json payload is missing.")]
    MissingPayload,
    #[error("Error while receiving the playload. `{0}`.")]
    ReceivePayload(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl ErrorCode for PayloadError {
    fn error_code(&self) -> Code {
        match self {
            PayloadError::Payload(e) => match e {
                ActixPayloadError::IncompleteError => Code::BadRequest,
                ActixPayloadError::OtherError(error) => match error {
                    aweb::error::PayloadError::EncodingCorrupted => Code::Internal,
                    aweb::error::PayloadError::Overflow => Code::PayloadTooLarge,
                    aweb::error::PayloadError::UnknownLength => Code::Internal,
                    aweb::error::PayloadError::Http2Payload(_) => Code::Internal,
                    aweb::error::PayloadError::Io(_) => Code::Internal,
                    _ => todo!(),
                },
            },
            PayloadError::Json(err) => match err {
                JsonPayloadError::Overflow { .. } => Code::PayloadTooLarge,
                JsonPayloadError::ContentType => Code::UnsupportedMediaType,
                JsonPayloadError::Payload(aweb::error::PayloadError::Overflow) => {
                    Code::PayloadTooLarge
                }
                JsonPayloadError::Payload(_) => Code::BadRequest,
                JsonPayloadError::Deserialize(_) => Code::BadRequest,
                JsonPayloadError::Serialize(_) => Code::Internal,
                _ => Code::Internal,
            },
            PayloadError::Query(err) => match err {
                QueryPayloadError::Deserialize(_) => Code::BadRequest,
                _ => Code::Internal,
            },
            PayloadError::MissingPayload => Code::MissingPayload,
            PayloadError::MalformedPayload(_) => Code::MalformedPayload,
            PayloadError::ReceivePayload(_) => Code::Internal,
        }
    }
}

impl From<JsonPayloadError> for PayloadError {
    fn from(other: JsonPayloadError) -> Self {
        match other {
            JsonPayloadError::Deserialize(e)
                if e.classify() == serde_json::error::Category::Eof
                    && e.line() == 1
                    && e.column() == 0 =>
            {
                Self::MissingPayload
            }
            JsonPayloadError::Deserialize(e)
                if e.classify() != serde_json::error::Category::Data =>
            {
                Self::MalformedPayload(e)
            }
            _ => Self::Json(other),
        }
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

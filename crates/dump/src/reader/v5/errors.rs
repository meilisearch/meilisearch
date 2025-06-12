use std::fmt;

use http::StatusCode;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct ResponseError {
    #[serde(skip)]
    code: StatusCode,

    pub message: String,
    #[serde(rename = "code")]
    pub error_code: String,
    #[serde(rename = "type")]
    pub error_type: String,
    #[serde(rename = "link")]
    pub error_link: String,
}

impl ResponseError {
    pub fn from_msg(message: String, code: Code) -> Self {
        Self {
            code: code.http(),
            message,
            error_code: code.err_code().error_name.to_string(),
            error_type: code.type_(),
            error_link: code.url(),
        }
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Debug, Clone, Copy)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum Code {
    // index related error
    CreateIndex,
    IndexAlreadyExists,
    IndexNotFound,
    InvalidIndexUid,
    InvalidMinWordLengthForTypo,

    // invalid state error
    InvalidState,
    MissingPrimaryKey,
    PrimaryKeyAlreadyPresent,

    MaxFieldsLimitExceeded,
    MissingDocumentId,
    InvalidDocumentId,

    Filter,
    Sort,

    BadParameter,
    BadRequest,
    DatabaseSizeLimitReached,
    DocumentNotFound,
    Internal,
    InvalidGeoField,
    InvalidRankingRule,
    InvalidStore,
    InvalidToken,
    MissingAuthorizationHeader,
    NoSpaceLeftOnDevice,
    DumpNotFound,
    TaskNotFound,
    PayloadTooLarge,
    RetrieveDocument,
    SearchDocuments,
    UnsupportedMediaType,

    DumpAlreadyInProgress,
    DumpProcessFailed,

    InvalidContentType,
    MissingContentType,
    MalformedPayload,
    MissingPayload,

    ApiKeyNotFound,
    MissingParameter,
    InvalidApiKeyActions,
    InvalidApiKeyIndexes,
    InvalidApiKeyExpiresAt,
    InvalidApiKeyDescription,
    InvalidApiKeyName,
    InvalidApiKeyUid,
    ImmutableField,
    ApiKeyAlreadyExists,

    UnretrievableErrorCode,
}

impl Code {
    /// associate a `Code` variant to the actual ErrCode
    fn err_code(&self) -> ErrCode {
        use Code::*;

        match self {
            // index related errors
            // create index is thrown on internal error while creating an index.
            CreateIndex => {
                ErrCode::internal("index_creation_failed", StatusCode::INTERNAL_SERVER_ERROR)
            }
            IndexAlreadyExists => ErrCode::invalid("index_already_exists", StatusCode::CONFLICT),
            // thrown when requesting an unexisting index
            IndexNotFound => ErrCode::invalid("index_not_found", StatusCode::NOT_FOUND),
            InvalidIndexUid => ErrCode::invalid("invalid_index_uid", StatusCode::BAD_REQUEST),

            // invalid state error
            InvalidState => ErrCode::internal("invalid_state", StatusCode::INTERNAL_SERVER_ERROR),
            // thrown when no primary key has been set
            MissingPrimaryKey => {
                ErrCode::invalid("primary_key_inference_failed", StatusCode::BAD_REQUEST)
            }
            // error thrown when trying to set an already existing primary key
            PrimaryKeyAlreadyPresent => {
                ErrCode::invalid("index_primary_key_already_exists", StatusCode::BAD_REQUEST)
            }
            // invalid ranking rule
            InvalidRankingRule => ErrCode::invalid("invalid_ranking_rule", StatusCode::BAD_REQUEST),

            // invalid database
            InvalidStore => {
                ErrCode::internal("invalid_store_file", StatusCode::INTERNAL_SERVER_ERROR)
            }

            // invalid document
            MaxFieldsLimitExceeded => {
                ErrCode::invalid("max_fields_limit_exceeded", StatusCode::BAD_REQUEST)
            }
            MissingDocumentId => ErrCode::invalid("missing_document_id", StatusCode::BAD_REQUEST),
            InvalidDocumentId => ErrCode::invalid("invalid_document_id", StatusCode::BAD_REQUEST),

            // error related to filters
            Filter => ErrCode::invalid("invalid_filter", StatusCode::BAD_REQUEST),
            // error related to sorts
            Sort => ErrCode::invalid("invalid_sort", StatusCode::BAD_REQUEST),

            BadParameter => ErrCode::invalid("bad_parameter", StatusCode::BAD_REQUEST),
            BadRequest => ErrCode::invalid("bad_request", StatusCode::BAD_REQUEST),
            DatabaseSizeLimitReached => {
                ErrCode::internal("database_size_limit_reached", StatusCode::INTERNAL_SERVER_ERROR)
            }
            DocumentNotFound => ErrCode::invalid("document_not_found", StatusCode::NOT_FOUND),
            Internal => ErrCode::internal("internal", StatusCode::INTERNAL_SERVER_ERROR),
            InvalidGeoField => ErrCode::invalid("invalid_geo_field", StatusCode::BAD_REQUEST),
            InvalidToken => ErrCode::authentication("invalid_api_key", StatusCode::FORBIDDEN),
            MissingAuthorizationHeader => {
                ErrCode::authentication("missing_authorization_header", StatusCode::UNAUTHORIZED)
            }
            TaskNotFound => ErrCode::invalid("task_not_found", StatusCode::NOT_FOUND),
            DumpNotFound => ErrCode::invalid("dump_not_found", StatusCode::NOT_FOUND),
            NoSpaceLeftOnDevice => {
                ErrCode::internal("no_space_left_on_device", StatusCode::INTERNAL_SERVER_ERROR)
            }
            PayloadTooLarge => ErrCode::invalid("payload_too_large", StatusCode::PAYLOAD_TOO_LARGE),
            RetrieveDocument => {
                ErrCode::internal("unretrievable_document", StatusCode::BAD_REQUEST)
            }
            SearchDocuments => ErrCode::internal("search_error", StatusCode::BAD_REQUEST),
            UnsupportedMediaType => {
                ErrCode::invalid("unsupported_media_type", StatusCode::UNSUPPORTED_MEDIA_TYPE)
            }

            // error related to dump
            DumpAlreadyInProgress => {
                ErrCode::invalid("dump_already_processing", StatusCode::CONFLICT)
            }
            DumpProcessFailed => {
                ErrCode::internal("dump_process_failed", StatusCode::INTERNAL_SERVER_ERROR)
            }
            MissingContentType => {
                ErrCode::invalid("missing_content_type", StatusCode::UNSUPPORTED_MEDIA_TYPE)
            }
            MalformedPayload => ErrCode::invalid("malformed_payload", StatusCode::BAD_REQUEST),
            InvalidContentType => {
                ErrCode::invalid("invalid_content_type", StatusCode::UNSUPPORTED_MEDIA_TYPE)
            }
            MissingPayload => ErrCode::invalid("missing_payload", StatusCode::BAD_REQUEST),

            // error related to keys
            ApiKeyNotFound => ErrCode::invalid("api_key_not_found", StatusCode::NOT_FOUND),
            MissingParameter => ErrCode::invalid("missing_parameter", StatusCode::BAD_REQUEST),
            InvalidApiKeyActions => {
                ErrCode::invalid("invalid_api_key_actions", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyIndexes => {
                ErrCode::invalid("invalid_api_key_indexes", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyExpiresAt => {
                ErrCode::invalid("invalid_api_key_expires_at", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyDescription => {
                ErrCode::invalid("invalid_api_key_description", StatusCode::BAD_REQUEST)
            }
            InvalidApiKeyName => ErrCode::invalid("invalid_api_key_name", StatusCode::BAD_REQUEST),
            InvalidApiKeyUid => ErrCode::invalid("invalid_api_key_uid", StatusCode::BAD_REQUEST),
            ApiKeyAlreadyExists => ErrCode::invalid("api_key_already_exists", StatusCode::CONFLICT),
            ImmutableField => ErrCode::invalid("immutable_field", StatusCode::BAD_REQUEST),
            InvalidMinWordLengthForTypo => {
                ErrCode::invalid("invalid_min_word_length_for_typo", StatusCode::BAD_REQUEST)
            }
            UnretrievableErrorCode => {
                ErrCode::invalid("unretrievable_error_code", StatusCode::BAD_REQUEST)
            }
        }
    }

    /// return the HTTP status code associated with the `Code`
    fn http(&self) -> StatusCode {
        self.err_code().status_code
    }

    /// return error name, used as error code
    fn name(&self) -> String {
        self.err_code().error_name.to_string()
    }

    /// return the error type
    fn type_(&self) -> String {
        self.err_code().error_type.to_string()
    }

    /// return the doc url associated with the error
    fn url(&self) -> String {
        format!("https://www.meilisearch.com/docs/reference/errors/error_codes#{}", self.name())
    }
}

/// Internal structure providing a convenient way to create error codes
struct ErrCode {
    status_code: StatusCode,
    error_type: ErrorType,
    error_name: &'static str,
}

impl ErrCode {
    fn authentication(error_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::AuthenticationError }
    }

    fn internal(error_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::InternalError }
    }

    fn invalid(error_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode { status_code, error_name, error_type: ErrorType::InvalidRequestError }
    }
}

#[allow(clippy::enum_variant_names)]
enum ErrorType {
    InternalError,
    InvalidRequestError,
    AuthenticationError,
}

impl fmt::Display for ErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ErrorType::*;

        match self {
            InternalError => write!(f, "internal"),
            InvalidRequestError => write!(f, "invalid_request"),
            AuthenticationError => write!(f, "auth"),
        }
    }
}

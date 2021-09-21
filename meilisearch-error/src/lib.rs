use std::fmt;

use actix_http::http::StatusCode;

pub trait ErrorCode: std::error::Error {
    fn error_code(&self) -> Code;

    /// returns the HTTP status code ascociated with the error
    fn http_status(&self) -> StatusCode {
        self.error_code().http()
    }

    /// returns the doc url ascociated with the error
    fn error_url(&self) -> String {
        self.error_code().url()
    }

    /// returns error name, used as error code
    fn error_name(&self) -> String {
        self.error_code().name()
    }

    /// return the error type
    fn error_type(&self) -> String {
        self.error_code().type_()
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
            InternalError => write!(f, "internal_error"),
            InvalidRequestError => write!(f, "invalid_request_error"),
            AuthenticationError => write!(f, "authentication_error"),
        }
    }
}

pub enum Code {
    // index related error
    CreateIndex,
    IndexAlreadyExists,
    IndexNotFound,
    InvalidIndexUid,
    OpenIndex,

    // invalid state error
    InvalidState,
    MissingPrimaryKey,
    PrimaryKeyAlreadyPresent,

    MaxFieldsLimitExceeded,
    MissingDocumentId,

    Facet,
    Filter,
    Sort,

    BadParameter,
    BadRequest,
    DocumentNotFound,
    Internal,
    InvalidGeoField,
    InvalidToken,
    MissingAuthorizationHeader,
    NotFound,
    PayloadTooLarge,
    RetrieveDocument,
    SearchDocuments,
    UnsupportedMediaType,

    DumpAlreadyInProgress,
    DumpProcessFailed,
}

impl Code {
    /// ascociate a `Code` variant to the actual ErrCode
    fn err_code(&self) -> ErrCode {
        use Code::*;

        match self {
            // index related errors
            // create index is thrown on internal error while creating an index.
            CreateIndex => ErrCode::internal("index_creation_failed", StatusCode::BAD_REQUEST),
            IndexAlreadyExists => ErrCode::invalid("index_already_exists", StatusCode::BAD_REQUEST),
            // thrown when requesting an unexisting index
            IndexNotFound => ErrCode::invalid("index_not_found", StatusCode::NOT_FOUND),
            InvalidIndexUid => ErrCode::invalid("invalid_index_uid", StatusCode::BAD_REQUEST),
            OpenIndex => {
                ErrCode::internal("index_not_accessible", StatusCode::INTERNAL_SERVER_ERROR)
            }

            // invalid state error
            InvalidState => ErrCode::internal("invalid_state", StatusCode::INTERNAL_SERVER_ERROR),
            // thrown when no primary key has been set
            MissingPrimaryKey => ErrCode::invalid("missing_primary_key", StatusCode::BAD_REQUEST),
            // error thrown when trying to set an already existing primary key
            PrimaryKeyAlreadyPresent => {
                ErrCode::invalid("primary_key_already_present", StatusCode::BAD_REQUEST)
            }

            // invalid document
            MaxFieldsLimitExceeded => {
                ErrCode::invalid("max_fields_limit_exceeded", StatusCode::BAD_REQUEST)
            }
            MissingDocumentId => ErrCode::invalid("missing_document_id", StatusCode::BAD_REQUEST),

            // error related to facets
            Facet => ErrCode::invalid("invalid_facet", StatusCode::BAD_REQUEST),
            // error related to filters
            Filter => ErrCode::invalid("invalid_filter", StatusCode::BAD_REQUEST),
            // error related to sorts
            Sort => ErrCode::invalid("invalid_sort", StatusCode::BAD_REQUEST),

            BadParameter => ErrCode::invalid("bad_parameter", StatusCode::BAD_REQUEST),
            BadRequest => ErrCode::invalid("bad_request", StatusCode::BAD_REQUEST),
            DocumentNotFound => ErrCode::invalid("document_not_found", StatusCode::NOT_FOUND),
            Internal => ErrCode::internal("internal", StatusCode::INTERNAL_SERVER_ERROR),
            InvalidGeoField => {
                ErrCode::authentication("invalid_geo_field", StatusCode::BAD_REQUEST)
            }
            InvalidToken => ErrCode::authentication("invalid_token", StatusCode::FORBIDDEN),
            MissingAuthorizationHeader => {
                ErrCode::authentication("missing_authorization_header", StatusCode::UNAUTHORIZED)
            }
            NotFound => ErrCode::invalid("not_found", StatusCode::NOT_FOUND),
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
                ErrCode::invalid("dump_already_in_progress", StatusCode::CONFLICT)
            }
            DumpProcessFailed => {
                ErrCode::internal("dump_process_failed", StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    /// return the HTTP status code ascociated with the `Code`
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

    /// return the doc url ascociated with the error
    fn url(&self) -> String {
        format!("https://docs.meilisearch.com/errors#{}", self.name())
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
        ErrCode {
            status_code,
            error_name,
            error_type: ErrorType::AuthenticationError,
        }
    }

    fn internal(error_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode {
            status_code,
            error_name,
            error_type: ErrorType::InternalError,
        }
    }

    fn invalid(error_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode {
            status_code,
            error_name,
            error_type: ErrorType::InvalidRequestError,
        }
    }
}

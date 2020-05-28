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

enum ErrorType {
    InternalError,
    InvalidRequest,
    Authentication,
}

impl fmt::Display for ErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ErrorType::*;

        match self {
            InternalError => write!(f, "internal_error"),
            InvalidRequest => write!(f, "invalid_request"),
            Authentication => write!(f, "authentication"),
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

    BadParameter,
    BadRequest,
    DocumentNotFound,
    Internal,
    InvalidToken,
    Maintenance,
    MissingAuthorizationHeader,
    MissingHeader,
    NotFound,
    PayloadTooLarge,
    RetrieveDocument,
    SearchDocuments,
    UnsupportedMediaType,
}

impl Code {

    /// ascociate a `Code` variant to the actual ErrCode
    fn err_code(&self) -> ErrCode {
        use Code::*;

        match self {
            // index related errors
            CreateIndex => ErrCode::invalid("create_index", StatusCode::BAD_REQUEST),
            IndexAlreadyExists => ErrCode::invalid("existing_index", StatusCode::BAD_REQUEST),
            IndexNotFound => ErrCode::invalid("index_not_found", StatusCode::NOT_FOUND), InvalidIndexUid => ErrCode::invalid("invalid_index_uid", StatusCode::BAD_REQUEST),
            OpenIndex => ErrCode::internal("open_index", StatusCode::INTERNAL_SERVER_ERROR),

            // invalid state error
            InvalidState => ErrCode::internal("invalid_state", StatusCode::INTERNAL_SERVER_ERROR),
            MissingPrimaryKey => ErrCode::internal("missing_primary_key", StatusCode::INTERNAL_SERVER_ERROR),
            PrimaryKeyAlreadyPresent => ErrCode::internal("primary_key_already_present", StatusCode::INTERNAL_SERVER_ERROR),

            // invalid document
            MaxFieldsLimitExceeded => ErrCode::invalid("max_field_limit_exceeded", StatusCode::BAD_REQUEST),
            MissingDocumentId => ErrCode::invalid("missing_document_id", StatusCode::BAD_REQUEST),

            Facet => ErrCode::invalid("invalid_facet", StatusCode::BAD_REQUEST),
            Filter => ErrCode::invalid("invalid_filter", StatusCode::BAD_REQUEST),

            BadParameter => ErrCode::invalid("bad_parameter", StatusCode::BAD_REQUEST),
            BadRequest => ErrCode::invalid("bad_request", StatusCode::BAD_REQUEST),
            DocumentNotFound => ErrCode::internal("document_not_found", StatusCode::NOT_FOUND),
            Internal => ErrCode::internal("internal", StatusCode::INTERNAL_SERVER_ERROR),
            InvalidToken => ErrCode::authentication("invalid_token", StatusCode::UNAUTHORIZED),
            Maintenance =>  ErrCode::internal("maintenance", StatusCode::SERVICE_UNAVAILABLE),
            MissingAuthorizationHeader => ErrCode::authentication("missing_authorization_header", StatusCode::FORBIDDEN),
            MissingHeader => ErrCode::authentication("missing_header", StatusCode::UNAUTHORIZED),
            NotFound => ErrCode::invalid("not_found", StatusCode::NOT_FOUND),
            PayloadTooLarge => ErrCode::invalid("payload_too_large", StatusCode::PAYLOAD_TOO_LARGE),
            RetrieveDocument => ErrCode::internal("retrieve_document", StatusCode::BAD_REQUEST),
            SearchDocuments => ErrCode::internal("search_error", StatusCode::BAD_REQUEST),
            UnsupportedMediaType => ErrCode::invalid("unsupported_media_type", StatusCode::UNSUPPORTED_MEDIA_TYPE),
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
        format!("https://docs.meilisearch.com/error/{}", self.name())
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
            error_type: ErrorType::Authentication,
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
            error_type: ErrorType::InvalidRequest,
        }
    }
}

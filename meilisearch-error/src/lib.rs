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
        self.error_code().r#type()
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

    // invalid documents FIXME make one error code?
    MissingDocumentId,
    MaxFieldsLimitExceeded,

    Filter,
    Facet,

    BadParameter,
    BadRequest,
    DocumentNotFound,
    Internal,
    InvalidToken,
    Maintenance,
    MissingAuthorizationHeader,
    MissingHeader,
    NotFound,
    RetrieveDocument,
    SearchDocuments,
    PayloadTooLarge,
    UnsupportedMediaType,
    Other,
}

impl Code {

    /// ascociate a `Code` variant to the actual ErrCode
    fn err_code(&self) -> ErrCode {
        use Code::*;

        match self {
            // index related errors
            CreateIndex => ErrCode::invalid("create_index", StatusCode::BAD_REQUEST),
            IndexAlreadyExists => ErrCode::invalid("existing_index", StatusCode::BAD_REQUEST),
            IndexNotFound => ErrCode::invalid("index_not_found", StatusCode::NOT_FOUND),
            InvalidIndexUid => ErrCode::invalid("invalid_index_uid", StatusCode::BAD_REQUEST),
            OpenIndex => ErrCode::internal("open_index", StatusCode::INTERNAL_SERVER_ERROR),

            // invalid state error
            InvalidState => ErrCode::internal("invalid_state", StatusCode::INTERNAL_SERVER_ERROR),
            // FIXME probably not an internal statuscode there
            MissingPrimaryKey => ErrCode::internal("missing_primary_key", StatusCode::INTERNAL_SERVER_ERROR),
            PrimaryKeyAlreadyPresent => ErrCode::internal("primary_key_already_present", StatusCode::INTERNAL_SERVER_ERROR),

            // invalid document
            MissingDocumentId => ErrCode::invalid("MissingDocumentId", StatusCode::BAD_REQUEST),
            MaxFieldsLimitExceeded => ErrCode::invalid("max_field_limit_exceeded", StatusCode::BAD_REQUEST),

            Filter => ErrCode::invalid("fitler", StatusCode::BAD_REQUEST),
            Facet => ErrCode::invalid("facet", StatusCode::BAD_REQUEST),

            BadParameter => ErrCode::invalid("bad_parameter", StatusCode::BAD_REQUEST),
            BadRequest => ErrCode::invalid("bad_request", StatusCode::BAD_REQUEST),
            RetrieveDocument => ErrCode::invalid("retrieve_document", StatusCode::BAD_REQUEST),
            SearchDocuments => ErrCode::invalid("search_document", StatusCode::BAD_REQUEST),
            DocumentNotFound => ErrCode::invalid("document_not_found", StatusCode::NOT_FOUND),
            NotFound => ErrCode::invalid("not_found", StatusCode::NOT_FOUND),
            InvalidToken => ErrCode::authentication("invalid_token", StatusCode::UNAUTHORIZED),
            MissingHeader => ErrCode::authentication("missing_header", StatusCode::UNAUTHORIZED),
            MissingAuthorizationHeader => ErrCode::authentication("missing_authorization_header", StatusCode::FORBIDDEN),
            Internal => ErrCode::internal("internal", StatusCode::INTERNAL_SERVER_ERROR),
            Maintenance =>  ErrCode::invalid("maintenance", StatusCode::SERVICE_UNAVAILABLE),
            PayloadTooLarge => ErrCode::invalid("payload_too_large", StatusCode::PAYLOAD_TOO_LARGE),
            UnsupportedMediaType => ErrCode::invalid("unsupported_media_type", StatusCode::UNSUPPORTED_MEDIA_TYPE),
            _ => ErrCode::invalid("other", StatusCode::BAD_REQUEST),
        }
    }

    /// return the HTTP status code ascociated with the `Code`
    fn http(&self) -> StatusCode {
        self.err_code().status_code
    }

    /// return error name, used as error code
    fn name(&self) -> String {
        self.err_code().err_name.to_string()
    }

    /// return the error type
    fn r#type(&self) -> String {
        self.err_code().err_type.to_string()
    }

    /// return the doc url ascociated with the error
    fn url(&self) -> String {
        format!("docs.meilisearch.come/error/{}", self.name())
    }
}

/// Internal structure providing a convenient way to create error codes
struct ErrCode {
    status_code: StatusCode,
    err_type: ErrorType,
    err_name: &'static str,
}

impl ErrCode {

    fn authentication(err_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode {
            status_code,
            err_name,
            err_type: ErrorType::Authentication,
        }
    }

    fn internal(err_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode {
            status_code,
            err_name,
            err_type: ErrorType::InternalError,
        }
    }

    fn invalid(err_name: &'static str, status_code: StatusCode) -> ErrCode {
        ErrCode {
            status_code,
            err_name,
            err_type: ErrorType::InvalidRequest,
        }
    }
}

use std::fmt;

use actix_http::ResponseBuilder;
use actix_web as aweb;
use actix_web::http::StatusCode;
use serde_json::json;
use actix_web::error::JsonPayloadError;

#[derive(Debug)]
pub enum Error {
    BadParameter(String, String),
    BadRequest(String),
    CreateIndex(String),
    DocumentNotFound(String),
    IndexNotFound(String),
    Internal(String),
    InvalidIndexUid,
    InvalidToken(String),
    Maintenance,
    MissingAuthorizationHeader,
    MissingHeader(String),
    NotFound(String),
    OpenIndex(String),
    FilterParsing(String),
    RetrieveDocument(u32, String),
    SearchDocuments(String),
    PayloadTooLarge,
    UnsupportedMediaType,
    FacetExpression(String),
    FacetCount(String),
}

pub enum FacetCountError {
    AttributeNotSet(String),
    SyntaxError(String),
    UnexpectedToken { found: String, expected: &'static [&'static str] },
    NoFacetSet,
}

impl FacetCountError {
    pub fn unexpected_token(found: impl ToString, expected: &'static [&'static str]) -> FacetCountError {
        let found = found.to_string();
        FacetCountError::UnexpectedToken { expected, found }
    }
}

impl From<serde_json::error::Error> for FacetCountError {
    fn from(other: serde_json::error::Error) -> FacetCountError {
        FacetCountError::SyntaxError(other.to_string())
    }
}

impl fmt::Display for FacetCountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use FacetCountError::*;

        match self {
            AttributeNotSet(attr) => write!(f, "attribute {} is not set as facet", attr),
            SyntaxError(msg) => write!(f, "syntax error: {}", msg),
            UnexpectedToken { expected, found } => write!(f, "unexpected {} found, expected {:?}", found, expected),
            NoFacetSet => write!(f, "can't perform facet count, as no facet is set"),
        }
    }
}

impl Error {
    pub fn internal(err: impl fmt::Display) -> Error {
        Error::Internal(err.to_string())
    }

    pub fn bad_request(err: impl fmt::Display) -> Error {
        Error::BadRequest(err.to_string())
    }

    pub fn missing_authorization_header() -> Error {
        Error::MissingAuthorizationHeader
    }

    pub fn invalid_token(err: impl fmt::Display) -> Error {
        Error::InvalidToken(err.to_string())
    }

    pub fn not_found(err: impl fmt::Display) -> Error {
        Error::NotFound(err.to_string())
    }

    pub fn index_not_found(err: impl fmt::Display) -> Error {
        Error::IndexNotFound(err.to_string())
    }

    pub fn document_not_found(err: impl fmt::Display) -> Error {
        Error::DocumentNotFound(err.to_string())
    }

    pub fn missing_header(err: impl fmt::Display) -> Error {
        Error::MissingHeader(err.to_string())
    }

    pub fn bad_parameter(param: impl fmt::Display, err: impl fmt::Display) -> Error {
        Error::BadParameter(param.to_string(), err.to_string())
    }

    pub fn open_index(err: impl fmt::Display) -> Error {
        Error::OpenIndex(err.to_string())
    }

    pub fn create_index(err: impl fmt::Display) -> Error {
        Error::CreateIndex(err.to_string())
    }

    pub fn invalid_index_uid() -> Error {
        Error::InvalidIndexUid
    }

    pub fn maintenance() -> Error {
        Error::Maintenance
    }

    pub fn retrieve_document(doc_id: u64, err: impl fmt::Display) -> Error {
        Error::RetrieveDocument(doc_id, err.to_string())
    }

    pub fn search_documents(err: impl fmt::Display) -> Error {
        Error::SearchDocuments(err.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadParameter(param, err) => write!(f, "Url parameter {} error: {}", param, err),
            Self::BadRequest(err) => f.write_str(err),
            Self::CreateIndex(err) => write!(f, "Impossible to create index; {}", err),
            Self::DocumentNotFound(document_id) => write!(f, "Document with id {} not found", document_id),
            Self::IndexNotFound(index_uid) => write!(f, "Index {} not found", index_uid),
            Self::Internal(err) => f.write_str(err),
            Self::InvalidIndexUid => f.write_str("Index must have a valid uid; Index uid can be of type integer or string only composed of alphanumeric characters, hyphens (-) and underscores (_)."),
            Self::InvalidToken(err) => write!(f, "Invalid API key: {}", err),
            Self::Maintenance => f.write_str("Server is in maintenance, please try again later"),
            Self::FilterParsing(err) => write!(f, "parsing error: {}", err),
            Self::MissingAuthorizationHeader => f.write_str("You must have an authorization token"),
            Self::MissingHeader(header) => write!(f, "Header {} is missing", header),
            Self::NotFound(err) => write!(f, "{} not found", err),
            Self::OpenIndex(err) => write!(f, "Impossible to open index; {}", err),
            Self::RetrieveDocument(id, err) => write!(f, "impossible to retrieve the document with id: {}; {}", id, err),
            Self::SearchDocuments(err) => write!(f, "impossible to search documents; {}", err),
            Self::FacetExpression(e) => write!(f, "error parsing facet filter expression: {}", e),
            Self::PayloadTooLarge => f.write_str("Payload to large"),
            Self::UnsupportedMediaType => f.write_str("Unsupported media type"),
            Self::FacetCount(e) => write!(f, "error with facet count: {}", e),
        }
    }
}

impl aweb::error::ResponseError for Error {
    fn error_response(&self) -> aweb::HttpResponse {
        ResponseBuilder::new(self.status_code()).json(json!({
            "message": self.to_string(),
        }))
    }

    fn status_code(&self) -> StatusCode {
       match *self {
            Self::BadParameter(_, _)
            | Self::BadRequest(_)
            | Self::CreateIndex(_)
            | Self::InvalidIndexUid
            | Self::OpenIndex(_)
            | Self::RetrieveDocument(_, _)
            | Self::FacetExpression(_)
            | Self::SearchDocuments(_)
            | Self::FacetCount(_)
            | Self::FilterParsing(_) => StatusCode::BAD_REQUEST,
            Self::DocumentNotFound(_)
            | Self::IndexNotFound(_)
            | Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::InvalidToken(_)
            | Self::MissingHeader(_) => StatusCode::UNAUTHORIZED,
            Self::MissingAuthorizationHeader => StatusCode::FORBIDDEN,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Maintenance => StatusCode::SERVICE_UNAVAILABLE,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
        }
    }
}

impl From<meilisearch_core::HeedError> for Error {
    fn from(err: meilisearch_core::HeedError) -> Error {
        Error::Internal(err.to_string())
    }
}

impl From<meilisearch_core::FstError> for Error {
    fn from(err: meilisearch_core::FstError) -> Error {
        Error::Internal(err.to_string())
    }
}

impl From<meilisearch_core::FacetError> for Error {
    fn from(error: meilisearch_core::FacetError) -> Error {
        Error::FacetExpression(error.to_string())
    }
}

impl From<meilisearch_core::Error> for Error {
    fn from(err: meilisearch_core::Error) -> Error {
        use meilisearch_core::pest_error::LineColLocation::*;
        match err {
            meilisearch_core::Error::FilterParseError(e) => {
                let (line, column) = match e.line_col {
                    Span((line, _), (column, _)) => (line, column),
                    Pos((line, column)) => (line, column),
                };
                let message = format!("parsing error on line {} at column {}: {}", line, column, e.variant.message());

                Error::FilterParsing(message)
            },
            meilisearch_core::Error::FacetError(e) => Error::FacetExpression(e.to_string()),
            _ => Error::Internal(err.to_string()),
        }
    }
}

impl From<meilisearch_schema::Error> for Error {
    fn from(err: meilisearch_schema::Error) -> Error {
        Error::Internal(err.to_string())
    }
}

impl From<actix_http::Error> for Error {
    fn from(err: actix_http::Error) -> Error {
        Error::Internal(err.to_string())
    }
}

impl From<FacetCountError> for Error {
    fn from(other: FacetCountError) -> Error {
        Error::FacetCount(other.to_string())
    }
}

impl From<JsonPayloadError> for Error {
    fn from(err: JsonPayloadError) -> Error {
        match err {
            JsonPayloadError::Deserialize(err) => Error::BadRequest(format!("Invalid JSON: {}", err)),
            JsonPayloadError::Overflow => Error::PayloadTooLarge,
            JsonPayloadError::ContentType => Error::UnsupportedMediaType,
            JsonPayloadError::Payload(err) => Error::BadRequest(format!("Problem while decoding the request: {}", err)),
        }
    }
}

pub fn json_error_handler(err: JsonPayloadError) -> Error {
    err.into()
}

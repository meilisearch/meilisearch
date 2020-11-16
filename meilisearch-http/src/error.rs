use std::error;
use std::fmt;

use actix_http::ResponseBuilder;
use actix_web as aweb;
use actix_web::error::{JsonPayloadError, QueryPayloadError};
use actix_web::http::StatusCode;
use serde::ser::{Serialize, Serializer, SerializeStruct};

use meilisearch_error::{ErrorCode, Code};

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

impl From<Error> for ResponseError {
    fn from(error: Error) -> ResponseError {
        ResponseError { inner: Box::new(error) }
    }
}

impl From<meilisearch_core::Error> for ResponseError {
    fn from(err: meilisearch_core::Error) -> ResponseError {
        ResponseError { inner: Box::new(err) }
    }
}

impl From<meilisearch_schema::Error> for ResponseError {
    fn from(err: meilisearch_schema::Error) -> ResponseError {
        ResponseError { inner: Box::new(err) }
    }
}

impl From<FacetCountError> for ResponseError {
    fn from(err: FacetCountError) -> ResponseError {
        ResponseError { inner: Box::new(err) }
    }
}

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
    fn error_response(&self) -> aweb::HttpResponse {
        ResponseBuilder::new(self.status_code()).json(&self)
    }

    fn status_code(&self) -> StatusCode {
        self.http_status()
    }
}

#[derive(Debug)]
pub enum Error {
    BadParameter(String, String),
    BadRequest(String),
    CreateIndex(String),
    DocumentNotFound(String),
    IndexNotFound(String),
    IndexAlreadyExists(String),
    Internal(String),
    InvalidIndexUid,
    InvalidToken(String),
    MissingAuthorizationHeader,
    NotFound(String),
    OpenIndex(String),
    RetrieveDocument(u32, String),
    SearchDocuments(String),
    PayloadTooLarge,
    UnsupportedMediaType,
    DumpAlreadyInProgress,
    DumpProcessFailed(String),
}

impl error::Error for Error {}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        use Error::*;
        match self {
            BadParameter(_, _) => Code::BadParameter,
            BadRequest(_) => Code::BadRequest,
            CreateIndex(_) => Code::CreateIndex,
            DocumentNotFound(_) => Code::DocumentNotFound,
            IndexNotFound(_) => Code::IndexNotFound,
            IndexAlreadyExists(_) => Code::IndexAlreadyExists,
            Internal(_) => Code::Internal,
            InvalidIndexUid => Code::InvalidIndexUid,
            InvalidToken(_) => Code::InvalidToken,
            MissingAuthorizationHeader => Code::MissingAuthorizationHeader,
            NotFound(_) => Code::NotFound,
            OpenIndex(_) => Code::OpenIndex,
            RetrieveDocument(_, _) => Code::RetrieveDocument,
            SearchDocuments(_) => Code::SearchDocuments,
            PayloadTooLarge => Code::PayloadTooLarge,
            UnsupportedMediaType => Code::UnsupportedMediaType,
            DumpAlreadyInProgress => Code::DumpAlreadyInProgress,
            DumpProcessFailed(_) => Code::DumpProcessFailed,
        }
    }
}

#[derive(Debug)]
pub enum FacetCountError {
    AttributeNotSet(String),
    SyntaxError(String),
    UnexpectedToken { found: String, expected: &'static [&'static str] },
    NoFacetSet,
}

impl error::Error for FacetCountError {}

impl ErrorCode for FacetCountError {
    fn error_code(&self) -> Code {
        Code::BadRequest
    }
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
            AttributeNotSet(attr) => write!(f, "Attribute {} is not set as facet", attr),
            SyntaxError(msg) => write!(f, "Syntax error: {}", msg),
            UnexpectedToken { expected, found } => write!(f, "Unexpected {} found, expected {:?}", found, expected),
            NoFacetSet => write!(f, "Can't perform facet count, as no facet is set"),
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

    pub fn retrieve_document(doc_id: u32, err: impl fmt::Display) -> Error {
        Error::RetrieveDocument(doc_id, err.to_string())
    }

    pub fn search_documents(err: impl fmt::Display) -> Error {
        Error::SearchDocuments(err.to_string())
    }

    pub fn dump_conflict() -> Error {
        Error::DumpAlreadyInProgress
    }

    pub fn dump_failed(message: String) -> Error {
        Error::DumpProcessFailed(message)
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
            Self::IndexAlreadyExists(index_uid) => write!(f, "Index {} already exists", index_uid),
            Self::Internal(err) => f.write_str(err),
            Self::InvalidIndexUid => f.write_str("Index must have a valid uid; Index uid can be of type integer or string only composed of alphanumeric characters, hyphens (-) and underscores (_)."),
            Self::InvalidToken(err) => write!(f, "Invalid API key: {}", err),
            Self::MissingAuthorizationHeader => f.write_str("You must have an authorization token"),
            Self::NotFound(err) => write!(f, "{} not found", err),
            Self::OpenIndex(err) => write!(f, "Impossible to open index; {}", err),
            Self::RetrieveDocument(id, err) => write!(f, "Impossible to retrieve the document with id: {}; {}", id, err),
            Self::SearchDocuments(err) => write!(f, "Impossible to search documents; {}", err),
            Self::PayloadTooLarge => f.write_str("Payload too large"),
            Self::UnsupportedMediaType => f.write_str("Unsupported media type"),
            Self::DumpAlreadyInProgress => f.write_str("Another dump is already in progress"),
            Self::DumpProcessFailed(message) => write!(f, "Dump process failed: {}", message),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Internal(err.to_string())
    }
}

impl From<actix_http::Error> for Error {
    fn from(err: actix_http::Error) -> Error {
        Error::Internal(err.to_string())
    }
}

impl From<meilisearch_core::Error> for Error {
    fn from(err: meilisearch_core::Error) -> Error {
        Error::Internal(err.to_string())
    }
}

impl From<serde_json::error::Error> for Error {
    fn from(err: serde_json::error::Error) -> Error {
        Error::Internal(err.to_string())
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

impl From<QueryPayloadError> for Error {
    fn from(err: QueryPayloadError) -> Error {
        match err {
            QueryPayloadError::Deserialize(err) => Error::BadRequest(format!("Invalid query parameters: {}", err)),
        }
    }
}

pub fn payload_error_handler<E: Into<Error>>(err: E) -> ResponseError {
    let error: Error = err.into();
    error.into()
}

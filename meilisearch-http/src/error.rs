use std::fmt;

use actix_http::ResponseBuilder;
use actix_web as aweb;
use actix_web::http::StatusCode;
use serde_json::json;

#[derive(Debug)]
pub enum ResponseError {
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
    RetrieveDocument(u64, String),
    SearchDocuments(String),
}

impl ResponseError {
    pub fn internal(err: impl fmt::Display) -> ResponseError {
        ResponseError::Internal(err.to_string())
    }

    pub fn bad_request(err: impl fmt::Display) -> ResponseError {
        ResponseError::BadRequest(err.to_string())
    }

    pub fn missing_authorization_header() -> ResponseError {
        ResponseError::MissingAuthorizationHeader
    }

    pub fn invalid_token(err: impl fmt::Display) -> ResponseError {
        ResponseError::InvalidToken(err.to_string())
    }

    pub fn not_found(err: impl fmt::Display) -> ResponseError {
        ResponseError::NotFound(err.to_string())
    }

    pub fn index_not_found(err: impl fmt::Display) -> ResponseError {
        ResponseError::IndexNotFound(err.to_string())
    }

    pub fn document_not_found(err: impl fmt::Display) -> ResponseError {
        ResponseError::DocumentNotFound(err.to_string())
    }

    pub fn missing_header(err: impl fmt::Display) -> ResponseError {
        ResponseError::MissingHeader(err.to_string())
    }

    pub fn bad_parameter(param: impl fmt::Display, err: impl fmt::Display) -> ResponseError {
        ResponseError::BadParameter(param.to_string(), err.to_string())
    }

    pub fn open_index(err: impl fmt::Display) -> ResponseError {
        ResponseError::OpenIndex(err.to_string())
    }

    pub fn create_index(err: impl fmt::Display) -> ResponseError {
        ResponseError::CreateIndex(err.to_string())
    }

    pub fn invalid_index_uid() -> ResponseError {
        ResponseError::InvalidIndexUid
    }

    pub fn maintenance() -> ResponseError {
        ResponseError::Maintenance
    }

    pub fn retrieve_document(doc_id: u64, err: impl fmt::Display) -> ResponseError {
        ResponseError::RetrieveDocument(doc_id, err.to_string())
    }

    pub fn search_documents(err: impl fmt::Display) -> ResponseError {
        ResponseError::SearchDocuments(err.to_string())
    }
}

impl fmt::Display for ResponseError {
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
        }
    }
}

impl aweb::error::ResponseError for ResponseError {
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
            | Self::SearchDocuments(_)
            | Self::FilterParsing(_) => StatusCode::BAD_REQUEST,
            Self::DocumentNotFound(_)
            | Self::IndexNotFound(_)
            | Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::InvalidToken(_)
            | Self::MissingHeader(_) => StatusCode::UNAUTHORIZED,
            Self::MissingAuthorizationHeader => StatusCode::FORBIDDEN,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Maintenance => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

impl From<meilisearch_core::HeedError> for ResponseError {
    fn from(err: meilisearch_core::HeedError) -> ResponseError {
        ResponseError::Internal(err.to_string())
    }
}

impl From<meilisearch_core::FstError> for ResponseError {
    fn from(err: meilisearch_core::FstError) -> ResponseError {
        ResponseError::Internal(err.to_string())
    }
}

impl From<meilisearch_core::Error> for ResponseError {
    fn from(err: meilisearch_core::Error) -> ResponseError {
        use meilisearch_core::pest_error::LineColLocation::*;
        match err {
            meilisearch_core::Error::FilterParseError(e) => {
                let (line, column) = match e.line_col {
                    Span((line, _), (column, _)) => (line, column),
                    Pos((line, column)) => (line, column),
                };
                let message = format!("parsing error on line {} at column {}: {}", line, column, e.variant.message());

                ResponseError::FilterParsing(message)
            },
            _ => ResponseError::Internal(err.to_string()),
        }
    }
}

impl From<meilisearch_schema::Error> for ResponseError {
    fn from(err: meilisearch_schema::Error) -> ResponseError {
        ResponseError::Internal(err.to_string())
    }
}

impl From<actix_http::Error> for ResponseError {
    fn from(err: actix_http::Error) -> ResponseError {
        ResponseError::Internal(err.to_string())
    }
}

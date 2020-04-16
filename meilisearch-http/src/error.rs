use std::fmt;

use actix_http::ResponseBuilder;
use actix_web as aweb;
use actix_web::http::StatusCode;
use serde_json::json;

#[derive(Debug)]
pub enum ResponseError {
    Internal(String),
    BadRequest(String),
    MissingAuthorizationHeader,
    InvalidToken(String),
    NotFound(String),
    IndexNotFound(String),
    DocumentNotFound(String),
    MissingHeader(String),
    FilterParsing(String),
    BadParameter(String, String),
    OpenIndex(String),
    CreateIndex(String),
    InvalidIndexUid,
    Maintenance,
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal(err) => write!(f, "{}", err),
            Self::BadRequest(err) => write!(f, "{}", err),
            Self::MissingAuthorizationHeader => write!(f, "You must have an authorization token"),
            Self::InvalidToken(err) => write!(f, "Invalid API key: {}", err),
            Self::NotFound(err) => write!(f, "{} not found", err),
            Self::IndexNotFound(index_uid) => write!(f, "Index {} not found", index_uid),
            Self::DocumentNotFound(document_id) => write!(f, "Document with id {} not found", document_id),
            Self::MissingHeader(header) => write!(f, "Header {} is missing", header),
            Self::BadParameter(param, err) => write!(f, "Url parameter {} error: {}", param, err),
            Self::OpenIndex(err) => write!(f, "Impossible to open index; {}", err),
            Self::CreateIndex(err) => write!(f, "Impossible to create index; {}", err),
            Self::InvalidIndexUid => write!(f, "Index must have a valid uid; Index uid can be of type integer or string only composed of alphanumeric characters, hyphens (-) and underscores (_)."),
            Self::Maintenance => write!(f, "Server is in maintenance, please try again later"),
            Self::FilterParsing(err) => write!(f, "parsing error: {}", err),
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
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::MissingAuthorizationHeader => StatusCode::FORBIDDEN,
            Self::InvalidToken(_) => StatusCode::UNAUTHORIZED,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::IndexNotFound(_) => StatusCode::NOT_FOUND,
            Self::DocumentNotFound(_) => StatusCode::NOT_FOUND,
            Self::MissingHeader(_) => StatusCode::UNAUTHORIZED,
            Self::BadParameter(_, _) => StatusCode::BAD_REQUEST,
            Self::OpenIndex(_) => StatusCode::BAD_REQUEST,
            Self::CreateIndex(_) => StatusCode::BAD_REQUEST,
            Self::InvalidIndexUid => StatusCode::BAD_REQUEST,
            Self::Maintenance => StatusCode::SERVICE_UNAVAILABLE,
            Self::FilterParsing(_) => StatusCode::BAD_REQUEST,
        }
    }
}

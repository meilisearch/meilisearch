use std::fmt;
use serde_json::json;
use actix_http::ResponseBuilder;
use actix_web::http::StatusCode;
use actix_web::*;

#[derive(Debug)]
pub enum ResponseError {
    Internal(String),
    BadRequest(String),
    InvalidToken(String),
    NotFound(String),
    IndexNotFound(String),
    DocumentNotFound(String),
    UpdateNotFound(u64),
    MissingHeader(String),
    FilterParsing(String),
    BadParameter(String, String),
    OpenIndex(String),
    CreateIndex(String),
    CreateTransaction,
    CommitTransaction,
    Schema,
    InferPrimaryKey,
    InvalidIndexUid,
    Maintenance,
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal(err) => write!(f, "Internal server error: {}", err),
            Self::BadRequest(err) => write!(f, "Bad request: {}", err),
            Self::InvalidToken(err) => write!(f, "Invalid API key: {}", err),
            Self::NotFound(err) => write!(f, "{} not found", err),
            Self::IndexNotFound(index_uid) => write!(f, "Index {} not found", index_uid),
            Self::DocumentNotFound(document_id) => write!(f, "Document with id {} not found", document_id),
            Self::UpdateNotFound(update_id) => write!(f, "Update with id {} not found", update_id),
            Self::MissingHeader(header) => write!(f, "Header {} is missing", header),
            Self::BadParameter(param, err) => write!(f, "Url parameter {} error: {}", param, err),
            Self::OpenIndex(err) => write!(f, "Impossible to open index; {}", err),
            Self::CreateIndex(err) => write!(f, "Impossible to create index; {}", err),
            Self::CreateTransaction => write!(f, "Impossible to create transaction"),
            Self::CommitTransaction => write!(f, "Impossible to commit transaction"),
            Self::Schema => write!(f, "Internal schema is innaccessible"),
            Self::InferPrimaryKey => write!(f, "Could not infer primary key"),
            Self::InvalidIndexUid => write!(f, "Index must have a valid uid; Index uid can be of type integer or string only composed of alphanumeric characters, hyphens (-) and underscores (_)."),
            Self::Maintenance => write!(f, "Server is in maintenance, please try again later"),
            Self::FilterParsing(err) => write!(f, "parsing error: {}", err),
        }
    }
}

impl error::ResponseError for ResponseError {
    fn error_response(&self) -> HttpResponse {
        ResponseBuilder::new(self.status_code()).json(json!({
            "message": self.to_string(),
        }))
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::InvalidToken(_) => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::IndexNotFound(_) => StatusCode::NOT_FOUND,
            Self::DocumentNotFound(_) => StatusCode::NOT_FOUND,
            Self::UpdateNotFound(_) => StatusCode::NOT_FOUND,
            Self::MissingHeader(_) => StatusCode::UNAUTHORIZED,
            Self::BadParameter(_, _) => StatusCode::BAD_REQUEST,
            Self::OpenIndex(_) => StatusCode::BAD_REQUEST,
            Self::CreateIndex(_) => StatusCode::BAD_REQUEST,
            Self::CreateTransaction => StatusCode::INTERNAL_SERVER_ERROR,
            Self::CommitTransaction => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Schema => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InferPrimaryKey => StatusCode::BAD_REQUEST,
            Self::InvalidIndexUid => StatusCode::BAD_REQUEST,
            Self::Maintenance => StatusCode::SERVICE_UNAVAILABLE,
            Self::FilterParsing(_) => StatusCode::BAD_REQUEST,
        }
    }
}

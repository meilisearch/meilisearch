use std::error::Error;

use meilisearch_error::{internal_error, Code, ErrorCode};
use serde_json::Value;

use crate::error::MilliError;

pub type Result<T> = std::result::Result<T, IndexError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("An internal error has occurred. `{0}`.")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
    #[error("Document `{0}` not found.")]
    DocumentNotFound(String),
    #[error("{0}")]
    Facet(#[from] FacetError),
    #[error("{0}")]
    Milli(#[from] milli::Error),
}

internal_error!(
    IndexError: std::io::Error,
    heed::Error,
    fst::Error,
    serde_json::Error
);

impl ErrorCode for IndexError {
    fn error_code(&self) -> Code {
        match self {
            IndexError::Internal(_) => Code::Internal,
            IndexError::DocumentNotFound(_) => Code::DocumentNotFound,
            IndexError::Facet(e) => e.error_code(),
            IndexError::Milli(e) => MilliError(e).error_code(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FacetError {
    #[error("Invalid syntax for the filter parameter: `expected {}, found: {1}`.", .0.join(", "))]
    InvalidExpression(&'static [&'static str], Value),
}

impl ErrorCode for FacetError {
    fn error_code(&self) -> Code {
        match self {
            FacetError::InvalidExpression(_, _) => Code::Filter,
        }
    }
}

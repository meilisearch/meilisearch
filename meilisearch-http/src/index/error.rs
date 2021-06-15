use std::error::Error;

use meilisearch_error::{Code, ErrorCode};
use serde_json::Value;

pub type Result<T> = std::result::Result<T, IndexError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("Internal error: {0}")]
    Internal(Box<dyn Error + Send + Sync + 'static>),
    #[error("Document with id {0} not found.")]
    DocumentNotFound(String),
    #[error("error with facet: {0}")]
    Facet(#[from] FacetError),
}

macro_rules! internal_error {
    ($($other:path), *) => {
        $(
            impl From<$other> for IndexError {
                fn from(other: $other) -> Self {
                    Self::Internal(Box::new(other))
                }
            }
        )*
    }
}

internal_error!(std::io::Error, heed::Error, fst::Error, serde_json::Error);

impl ErrorCode for IndexError {
    fn error_code(&self) -> Code {
        match self {
            IndexError::Internal(_) => Code::Internal,
            IndexError::DocumentNotFound(_) => Code::DocumentNotFound,
            IndexError::Facet(e) => e.error_code(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FacetError {
    #[error("Invalid facet expression, expected {}, found: {1}", .0.join(", "))]
    InvalidExpression(&'static [&'static str], Value),
}

impl ErrorCode for FacetError {
    fn error_code(&self) -> Code {
        match self {
            FacetError::InvalidExpression(_, _) => Code::Facet,
        }
    }
}

use std::error::Error;

use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::internal_error;
use serde_json::Value;

use crate::{error::MilliError, update_file_store};

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
    milli::heed::Error,
    fst::Error,
    serde_json::Error,
    update_file_store::UpdateFileStoreError,
    milli::documents::Error
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

impl From<milli::UserError> for IndexError {
    fn from(error: milli::UserError) -> IndexError {
        IndexError::Milli(error.into())
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

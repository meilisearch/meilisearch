use std::error::Error;
use std::fmt;

use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::internal_error;
use milli::UserError;
use serde_json::Value;

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
    file_store::Error,
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

impl ErrorCode for &IndexError {
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

#[derive(Debug)]
pub struct MilliError<'a>(pub &'a milli::Error);

impl Error for MilliError<'_> {}

impl fmt::Display for MilliError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ErrorCode for MilliError<'_> {
    fn error_code(&self) -> Code {
        match self.0 {
            milli::Error::InternalError(_) => Code::Internal,
            milli::Error::IoError(_) => Code::Internal,
            milli::Error::UserError(ref error) => {
                match error {
                    // TODO: wait for spec for new error codes.
                    UserError::SerdeJson(_)
                    | UserError::InvalidLmdbOpenOptions
                    | UserError::DocumentLimitReached
                    | UserError::AccessingSoftDeletedDocument { .. }
                    | UserError::UnknownInternalDocumentId { .. } => Code::Internal,
                    UserError::InvalidStoreFile => Code::InvalidStore,
                    UserError::NoSpaceLeftOnDevice => Code::NoSpaceLeftOnDevice,
                    UserError::MaxDatabaseSizeReached => Code::DatabaseSizeLimitReached,
                    UserError::AttributeLimitReached => Code::MaxFieldsLimitExceeded,
                    UserError::InvalidFilter(_) => Code::Filter,
                    UserError::MissingDocumentId { .. } => Code::MissingDocumentId,
                    UserError::InvalidDocumentId { .. } | UserError::TooManyDocumentIds { .. } => {
                        Code::InvalidDocumentId
                    }
                    UserError::MissingPrimaryKey => Code::MissingPrimaryKey,
                    UserError::PrimaryKeyCannotBeChanged(_) => Code::PrimaryKeyAlreadyPresent,
                    UserError::SortRankingRuleMissing => Code::Sort,
                    UserError::InvalidFacetsDistribution { .. } => Code::BadRequest,
                    UserError::InvalidSortableAttribute { .. } => Code::Sort,
                    UserError::CriterionError(_) => Code::InvalidRankingRule,
                    UserError::InvalidGeoField { .. } => Code::InvalidGeoField,
                    UserError::SortError(_) => Code::Sort,
                    UserError::InvalidMinTypoWordLenSetting(_, _) => {
                        Code::InvalidMinWordLengthForTypo
                    }
                }
            }
        }
    }
}

use meilisearch_error::{Code, ErrorCode};

use crate::{error::MilliError, index::error::IndexError};

pub type Result<T> = std::result::Result<T, IndexActorError>;

#[derive(thiserror::Error, Debug)]
pub enum IndexActorError {
    #[error("index error: {0}")]
    IndexError(#[from] IndexError),
    #[error("index already exists")]
    IndexAlreadyExists,
    #[error("index doesn't exists")]
    UnexistingIndex,
    #[error("existing primary key")]
    ExistingPrimaryKey,
    #[error("internal Index Error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    Milli(#[from] milli::Error),
}

macro_rules! internal_error {
    ($($other:path), *) => {
        $(
            impl From<$other> for IndexActorError {
                fn from(other: $other) -> Self {
                    Self::Internal(Box::new(other))
                }
            }
        )*
    }
}

internal_error!(heed::Error, tokio::task::JoinError, std::io::Error);

impl ErrorCode for IndexActorError {
    fn error_code(&self) -> Code {
        match self {
            IndexActorError::IndexError(e) => e.error_code(),
            IndexActorError::IndexAlreadyExists => Code::IndexAlreadyExists,
            IndexActorError::UnexistingIndex => Code::IndexNotFound,
            IndexActorError::ExistingPrimaryKey => Code::PrimaryKeyAlreadyPresent,
            IndexActorError::Internal(_) => Code::Internal,
            IndexActorError::Milli(e) => MilliError(e).error_code(),
        }
    }
}

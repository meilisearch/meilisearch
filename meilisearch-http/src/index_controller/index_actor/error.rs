use meilisearch_error::{Code, ErrorCode};

use crate::index::error::IndexError;

pub type Result<T> = std::result::Result<T, IndexActorError>;

#[derive(thiserror::Error, Debug)]
pub enum IndexActorError {
    #[error("index error: {0}")]
    IndexError(#[from] IndexError),
    #[error("index already exists")]
    IndexAlreadyExists,
    #[error("Index doesn't exists")]
    UnexistingIndex,
    #[error("Existing primary key")]
    ExistingPrimaryKey,
    #[error("Internal Index Error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
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

internal_error!(
    heed::Error,
    tokio::task::JoinError,
    std::io::Error
);

impl ErrorCode for IndexActorError {
    fn error_code(&self) -> Code {
        match self {
            IndexActorError::IndexError(e) => e.error_code(),
            IndexActorError::IndexAlreadyExists => Code::IndexAlreadyExists,
            IndexActorError::UnexistingIndex => Code::IndexNotFound,
            IndexActorError::ExistingPrimaryKey => Code::PrimaryKeyAlreadyPresent,
            IndexActorError::Internal(_) => Code::Internal,
        }
    }
}

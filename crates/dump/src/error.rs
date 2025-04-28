use meilisearch_types::error::{Code, ErrorCode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Bad index name.")]
    BadIndexName,
    #[error("Malformed task.")]
    MalformedTask,

    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        match self {
            Self::Io(e) => e.error_code(),

            // These errors either happen when creating a dump and don't need any error code,
            // or come from an internal bad deserialization.
            Self::Serde(_) |
            Self::Uuid(_) |

            // all these errors should never be raised when creating a dump, thus no error code should be associated.
            Self::BadIndexName |
            Self::MalformedTask => Code::Internal,
        }
    }
}

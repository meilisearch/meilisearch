use meilisearch_types::error::{Code, ErrorCode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("The version 1 of the dumps is not supported anymore. You can re-export your dump from a version between 0.21 and 0.24, or start fresh from a version 0.25 onwards.")]
    DumpV1Unsupported,
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
            // Are these three really Internal errors?
            // TODO look at that later.
            Error::Io(_) => Code::Internal,
            Error::Serde(_) => Code::Internal,
            Error::Uuid(_) => Code::Internal,

            // all these errors should never be raised when creating a dump, thus no error code should be associated.
            Error::DumpV1Unsupported => Code::Internal,
            Error::BadIndexName => Code::Internal,
            Error::MalformedTask => Code::Internal,
        }
    }
}

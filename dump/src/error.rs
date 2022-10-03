use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("The version 1 of the dumps is not supported anymore. You can re-export your dump from a version between 0.21 and 0.24, or start fresh from a version 0.25 onwards.")]
    DumpV1Unsupported,
    #[error("Bad index name")]
    BadIndexName,

    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
}

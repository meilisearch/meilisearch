use meilisearch_error::{Code, ErrorCode};

#[derive(Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error("The X-MEILI-API-KEY header is missing.")]
    MissingAuthorizationHeader,
    #[error("The provided API key is invalid.")]
    InvalidToken(String),
    // Triggered on configuration error.
    #[error("An internal error has occurred. `Irretrievable state`.")]
    IrretrievableState,
    #[error("An internal error has occurred. `Unknown authentication policy`.")]
    UnknownPolicy,
}

impl ErrorCode for AuthenticationError {
    fn error_code(&self) -> Code {
        match self {
            AuthenticationError::MissingAuthorizationHeader => Code::MissingAuthorizationHeader,
            AuthenticationError::InvalidToken(_) => Code::InvalidToken,
            AuthenticationError::IrretrievableState => Code::Internal,
            AuthenticationError::UnknownPolicy => Code::Internal,
        }
    }
}

use meilisearch_error::{Code, ErrorCode};

#[derive(Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error("You must have an authorization token")]
    MissingAuthorizationHeader,
    #[error("Invalid API key")]
    InvalidToken(String),
    // Triggered on configuration error.
    #[error("Irretrievable state")]
    IrretrievableState,
    #[error("Unknown authentication policy")]
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

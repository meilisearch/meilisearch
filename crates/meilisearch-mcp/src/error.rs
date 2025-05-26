use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Protocol error: {0}")]
    Protocol(String),
    
    #[error("Tool not found: {0}")]
    ToolNotFound(String),
    
    #[error("Invalid parameters: {0}")]
    InvalidParameters(String),
    
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),
    
    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("Meilisearch error: {0}")]
    Meilisearch(String),
}

impl Error {
    pub fn to_mcp_error(&self) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "error": {
                "code": self.error_code(),
                "message": self.to_string(),
            }
        })
    }
    
    fn error_code(&self) -> i32 {
        match self {
            Error::Protocol(_) => -32700,
            Error::ToolNotFound(_) => -32601,
            Error::InvalidParameters(_) => -32602,
            Error::AuthenticationFailed(_) => -32000,
            Error::Internal(_) => -32603,
            Error::Json(_) => -32700,
            Error::Meilisearch(_) => -32000,
        }
    }
}
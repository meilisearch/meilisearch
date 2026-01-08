use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RealtimeAPIError {
    /// The type of error (e.g., "invalid_request_error", "server_error").
    pub r#type: String,

    /// Error code, if any.
    pub code: Option<String>,

    /// A human-readable error message.
    pub message: String,

    /// Parameter related to the error, if any.
    pub param: Option<String>,

    /// The event_id of the client event that caused the error, if applicable.
    pub event_id: Option<String>,
}

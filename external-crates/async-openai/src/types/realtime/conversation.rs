use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Conversation {
    /// The unique ID of the conversation.
    pub id: String,

    /// The object type, must be "realtime.conversation".
    pub object: String,
}

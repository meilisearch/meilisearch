use serde::{Deserialize, Serialize};

/// Describes an OpenAI model offering that can be used with the API.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Model {
    /// The model identifier, which can be referenced in the API endpoints.
    pub id: String,
    /// The object type, which is always "model".
    pub object: String,
    /// The Unix timestamp (in seconds) when the model was created.
    pub created: u32,
    /// The organization that owns the model.
    pub owned_by: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct ListModelResponse {
    pub object: String,
    pub data: Vec<Model>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct DeleteModelResponse {
    pub id: String,
    pub object: String,
    pub deleted: bool,
}

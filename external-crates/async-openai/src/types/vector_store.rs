use std::collections::HashMap;

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::error::OpenAIError;

use super::StaticChunkingStrategy;

#[derive(Debug, Serialize, Deserialize, Default, Clone, Builder, PartialEq)]
#[builder(name = "CreateVectorStoreRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateVectorStoreRequest {
    /// A list of [File](https://platform.openai.com/docs/api-reference/files) IDs that the vector store should use. Useful for tools like `file_search` that can access files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_ids: Option<Vec<String>>,
    /// The name of the vector store.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The expiration policy for a vector store.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_after: Option<VectorStoreExpirationAfter>,

    /// The chunking strategy used to chunk the file(s). If not set, will use the `auto` strategy. Only applicable if `file_ids` is non-empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunking_strategy: Option<VectorStoreChunkingStrategy>,

    /// Set of 16 key-value pairs that can be attached to an object. This can be useful for storing additional information about the object in a structured format. Keys can be a maximum of 64 characters long and values can be a maximum of 512 characters long.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum VectorStoreChunkingStrategy {
    /// The default strategy. This strategy currently uses a `max_chunk_size_tokens` of `800` and `chunk_overlap_tokens` of `400`.
    #[default]
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "static")]
    Static {
        #[serde(rename = "static")]
        config: StaticChunkingStrategy,
    },
}

/// Vector store expiration policy
#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
pub struct VectorStoreExpirationAfter {
    /// Anchor timestamp after which the expiration policy applies. Supported anchors: `last_active_at`.
    pub anchor: String,
    /// The number of days after the anchor time that the vector store will expire.
    pub days: u16, // min: 1, max: 365
}

/// A vector store is a collection of processed files can be used by the `file_search` tool.
#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct VectorStoreObject {
    /// The identifier, which can be referenced in API endpoints.
    pub id: String,
    /// The object type, which is always `vector_store`.
    pub object: String,
    /// The Unix timestamp (in seconds) for when the vector store was created.
    pub created_at: u32,
    /// The name of the vector store.
    pub name: Option<String>,
    /// The total number of bytes used by the files in the vector store.
    pub usage_bytes: u64,
    pub file_counts: VectorStoreFileCounts,
    /// The status of the vector store, which can be either `expired`, `in_progress`, or `completed`. A status of `completed` indicates that the vector store is ready for use.
    pub status: VectorStoreStatus,
    pub expires_after: Option<VectorStoreExpirationAfter>,
    /// The Unix timestamp (in seconds) for when the vector store will expire.
    pub expires_at: Option<u32>,
    /// The Unix timestamp (in seconds) for when the vector store was last active.
    pub last_active_at: Option<u32>,

    /// Set of 16 key-value pairs that can be attached to an object. This can be useful for storing additional information about the object in a structured format. Keys can be a maximum of 64 characters long and values can be a maximum of 512 characters long.
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorStoreStatus {
    Expired,
    InProgress,
    Completed,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct VectorStoreFileCounts {
    /// The number of files that are currently being processed.
    pub in_progress: u32,
    /// The number of files that have been successfully processed.
    pub completed: u32,
    /// The number of files that have failed to process.
    pub failed: u32,
    /// The number of files that were cancelled.
    pub cancelled: u32,
    /// The total number of files.
    pub total: u32,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct ListVectorStoresResponse {
    pub object: String,
    pub data: Vec<VectorStoreObject>,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct DeleteVectorStoreResponse {
    pub id: String,
    pub object: String,
    pub deleted: bool,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Builder, PartialEq)]
#[builder(name = "UpdateVectorStoreRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct UpdateVectorStoreRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_after: Option<VectorStoreExpirationAfter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct ListVectorStoreFilesResponse {
    pub object: String,
    pub data: Vec<VectorStoreFileObject>,
    pub first_id: String,
    pub last_id: String,
    pub has_more: bool,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct VectorStoreFileObject {
    /// The identifier, which can be referenced in API endpoints.
    pub id: String,
    /// The object type, which is always `vector_store.file`.
    pub object: String,
    /// The total vector store usage in bytes. Note that this may be different from the original file size.
    pub usage_bytes: u64,
    /// The Unix timestamp (in seconds) for when the vector store file was created.
    pub created_at: u32,
    /// The ID of the [vector store](https://platform.openai.com/docs/api-reference/vector-stores/object) that the [File](https://platform.openai.com/docs/api-reference/files) is attached to.
    pub vector_store_id: String,
    /// The status of the vector store file, which can be either `in_progress`, `completed`, `cancelled`, or `failed`. The status `completed` indicates that the vector store file is ready for use.
    pub status: VectorStoreFileStatus,
    /// The last error associated with this vector store file. Will be `null` if there are no errors.
    pub last_error: Option<VectorStoreFileError>,
    /// The strategy used to chunk the file.
    pub chunking_strategy: Option<VectorStoreFileObjectChunkingStrategy>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorStoreFileStatus {
    InProgress,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct VectorStoreFileError {
    pub code: VectorStoreFileErrorCode,
    /// A human-readable description of the error.
    pub message: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorStoreFileErrorCode {
    ServerError,
    UnsupportedFile,
    InvalidFile,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum VectorStoreFileObjectChunkingStrategy {
    /// This is returned when the chunking strategy is unknown. Typically, this is because the file was indexed before the `chunking_strategy` concept was introduced in the API.
    Other,
    Static {
        r#static: StaticChunkingStrategy,
    },
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Builder, PartialEq)]
#[builder(name = "CreateVectorStoreFileRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateVectorStoreFileRequest {
    /// A [File](https://platform.openai.com/docs/api-reference/files) ID that the vector store should use. Useful for tools like `file_search` that can access files.
    pub file_id: String,
    pub chunking_strategy: Option<VectorStoreChunkingStrategy>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct DeleteVectorStoreFileResponse {
    pub id: String,
    pub object: String,
    pub deleted: bool,
}

#[derive(Debug, Serialize, Default, Clone, Builder, PartialEq, Deserialize)]
#[builder(name = "CreateVectorStoreFileBatchRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateVectorStoreFileBatchRequest {
    /// A list of [File](https://platform.openai.com/docs/api-reference/files) IDs that the vector store should use. Useful for tools like `file_search` that can access files.
    pub file_ids: Vec<String>, // minItems: 1, maxItems: 500
    pub chunking_strategy: Option<VectorStoreChunkingStrategy>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorStoreFileBatchStatus {
    InProgress,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct VectorStoreFileBatchCounts {
    /// The number of files that are currently being processed.
    pub in_progress: u32,
    /// The number of files that have been successfully processed.
    pub completed: u32,
    /// The number of files that have failed to process.
    pub failed: u32,
    /// The number of files that were cancelled.
    pub cancelled: u32,
    /// The total number of files.
    pub total: u32,
}

///  A batch of files attached to a vector store.
#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct VectorStoreFileBatchObject {
    /// The identifier, which can be referenced in API endpoints.
    pub id: String,
    /// The object type, which is always `vector_store.file_batch`.
    pub object: String,
    /// The Unix timestamp (in seconds) for when the vector store files batch was created.
    pub created_at: u32,
    /// The ID of the [vector store](https://platform.openai.com/docs/api-reference/vector-stores/object) that the [File](https://platform.openai.com/docs/api-reference/files) is attached to.
    pub vector_store_id: String,
    /// The status of the vector store files batch, which can be either `in_progress`, `completed`, `cancelled` or `failed`.
    pub status: VectorStoreFileBatchStatus,
    pub file_counts: VectorStoreFileBatchCounts,
}

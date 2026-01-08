use crate::error::OpenAIError;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use super::{InputSource, OpenAIFile};

/// Request to create an upload object that can accept byte chunks in the form of Parts.
#[derive(Clone, Serialize, Default, Debug, Deserialize, Builder, PartialEq)]
#[builder(name = "CreateUploadRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateUploadRequest {
    /// The name of the file to upload.
    pub filename: String,

    /// The intended purpose of the uploaded file.
    ///
    /// See the [documentation on File purposes](https://platform.openai.com/docs/api-reference/files/create#files-create-purpose).
    pub purpose: UploadPurpose,

    /// The number of bytes in the file you are uploading.
    pub bytes: u64,

    /// The MIME type of the file.
    ///
    /// This must fall within the supported MIME types for your file purpose. See the supported MIME
    /// types for assistants and vision.
    pub mime_type: String,
}

/// The intended purpose of the uploaded file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum UploadPurpose {
    /// For use with Assistants and Message files
    Assistants,
    /// For Assistants image file inputs
    Vision,
    /// For use with the Batch API
    Batch,
    /// For use with Fine-tuning
    #[default]
    FineTune,
}

/// The Upload object can accept byte chunks in the form of Parts.
#[derive(Debug, Serialize, Deserialize)]
pub struct Upload {
    /// The Upload unique identifier, which can be referenced in API endpoints
    pub id: String,

    /// The Unix timestamp (in seconds) for when the Upload was created
    pub created_at: u32,

    /// The name of the file to be uploaded
    pub filename: String,

    /// The intended number of bytes to be uploaded
    pub bytes: u64,

    /// The intended purpose of the file. [Pelase refer here]([Please refer here](/docs/api-reference/files/object#files/object-purpose) for acceptable values.)
    pub purpose: UploadPurpose,

    /// The status of the Upload.
    pub status: UploadStatus,

    /// The Unix timestamp (in seconds) for when the Upload was created
    pub expires_at: u32,

    /// The object type, which is always "upload"
    pub object: String,

    /// The ready File object after the Upload is completed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<OpenAIFile>,
}

/// The status of an upload
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UploadStatus {
    /// Upload is pending
    Pending,
    /// Upload has completed successfully
    Completed,
    /// Upload was cancelled
    Cancelled,
    /// Upload has expired
    Expired,
}

/// The upload Part represents a chunk of bytes we can add to an Upload object.
#[derive(Debug, Serialize, Deserialize)]
pub struct UploadPart {
    /// The upload Part unique identifier, which can be referenced in API endpoints
    pub id: String,

    /// The Unix timestamp (in seconds) for when the Part was created
    pub created_at: u32,

    /// The ID of the Upload object that this Part was added to
    pub upload_id: String,

    /// The object type, which is always `upload.part`
    pub object: String,
}

/// Request parameters for adding a part to an Upload
#[derive(Debug, Clone)]
pub struct AddUploadPartRequest {
    /// The chunk of bytes for this Part
    pub data: InputSource,
}

/// Request parameters for completing an Upload
#[derive(Debug, Serialize)]
pub struct CompleteUploadRequest {
    /// The ordered list of Part IDs
    pub part_ids: Vec<String>,

    /// The optional md5 checksum for the file contents to verify if the bytes uploaded matches what you expect
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
}

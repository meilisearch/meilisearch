use serde::{Deserialize, Serialize};

use super::{ProjectServiceAccount, ProjectUser};

/// Represents an individual API key in a project.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectApiKey {
    /// The object type, which is always `organization.project.api_key`.
    pub object: String,
    /// The redacted value of the API key.
    pub redacted_value: String,
    /// The name of the API key.
    pub name: String,
    /// The Unix timestamp (in seconds) of when the API key was created.
    pub created_at: u32,
    /// The identifier, which can be referenced in API endpoints.
    pub id: String,
    /// The owner of the API key.
    pub owner: ProjectApiKeyOwner,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "snake_case")]
pub enum ProjectApiKeyOwnerType {
    User,
    ServiceAccount,
}

/// Represents the owner of a project API key.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectApiKeyOwner {
    /// The type of owner, which is either `user` or `service_account`.
    pub r#type: ProjectApiKeyOwnerType,
    /// The user owner of the API key, if applicable.
    pub user: Option<ProjectUser>,
    /// The service account owner of the API key, if applicable.
    pub service_account: Option<ProjectServiceAccount>,
}

/// Represents the response object for listing project API keys.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectApiKeyListResponse {
    /// The object type, which is always `list`.
    pub object: String,
    /// The list of project API keys.
    pub data: Vec<ProjectApiKey>,
    /// The ID of the first project API key in the list.
    pub first_id: String,
    /// The ID of the last project API key in the list.
    pub last_id: String,
    /// Indicates if there are more project API keys available.
    pub has_more: bool,
}

/// Represents the response object for deleting a project API key.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectApiKeyDeleteResponse {
    /// The object type, which is always `organization.project.api_key.deleted`.
    pub object: String,
    /// The ID of the deleted API key.
    pub id: String,
    /// Indicates if the API key was successfully deleted.
    pub deleted: bool,
}

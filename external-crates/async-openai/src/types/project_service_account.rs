use serde::{Deserialize, Serialize};

use super::ProjectUserRole;

/// Represents an individual service account in a project.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectServiceAccount {
    /// The object type, which is always `organization.project.service_account`.
    pub object: String,
    /// The identifier, which can be referenced in API endpoints.
    pub id: String,
    /// The name of the service account.
    pub name: String,
    /// `owner` or `member`.
    pub role: ProjectUserRole,
    /// The Unix timestamp (in seconds) of when the service account was created.
    pub created_at: u32,
}

/// Represents the response object for listing project service accounts.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectServiceAccountListResponse {
    /// The object type, which is always `list`.
    pub object: String,
    /// The list of project service accounts.
    pub data: Vec<ProjectServiceAccount>,
    /// The ID of the first project service account in the list.
    pub first_id: String,
    /// The ID of the last project service account in the list.
    pub last_id: String,
    /// Indicates if there are more project service accounts available.
    pub has_more: bool,
}

/// Represents the request object for creating a project service account.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectServiceAccountCreateRequest {
    /// The name of the service account being created.
    pub name: String,
}

/// Represents the response object for creating a project service account.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectServiceAccountCreateResponse {
    /// The object type, which is always `organization.project.service_account`.
    pub object: String,
    /// The ID of the created service account.
    pub id: String,
    /// The name of the created service account.
    pub name: String,
    /// Service accounts can only have one role of type `member`.
    pub role: String,
    /// The Unix timestamp (in seconds) of when the service account was created.
    pub created_at: u32,
    /// The API key associated with the created service account.
    pub api_key: ProjectServiceAccountApiKey,
}

/// Represents the API key associated with a project service account.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectServiceAccountApiKey {
    /// The object type, which is always `organization.project.service_account.api_key`.
    pub object: String,
    /// The value of the API key.
    pub value: String,
    /// The name of the API key.
    pub name: String,
    /// The Unix timestamp (in seconds) of when the API key was created.
    pub created_at: u32,
    /// The ID of the API key.
    pub id: String,
}

/// Represents the response object for deleting a project service account.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectServiceAccountDeleteResponse {
    /// The object type, which is always `organization.project.service_account.deleted`.
    pub object: String,
    /// The ID of the deleted service account.
    pub id: String,
    /// Indicates if the service account was successfully deleted.
    pub deleted: bool,
}

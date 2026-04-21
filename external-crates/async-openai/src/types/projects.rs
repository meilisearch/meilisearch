use crate::types::OpenAIError;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

/// `active` or `archived`
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ProjectStatus {
    Active,
    Archived,
}

/// Represents an individual project.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Project {
    /// The identifier, which can be referenced in API endpoints
    pub id: String,
    /// The object type, which is always `organization.project`
    pub object: String,
    /// The name of the project. This appears in reporting.
    pub name: String,
    /// The Unix timestamp (in seconds) of when the project was created.
    pub created_at: u32,
    /// The Unix timestamp (in seconds) of when the project was archived or `null`.
    pub archived_at: Option<u32>,
    /// `active` or `archived`
    pub status: ProjectStatus,
}

/// A list of Project objects.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ProjectListResponse {
    pub object: String,
    pub data: Vec<Project>,
    pub first_id: String,
    pub last_id: String,
    pub has_more: String,
}

/// The project create request payload.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Builder)]
#[builder(name = "ProjectCreateRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ProjectCreateRequest {
    /// The friendly name of the project, this name appears in reports.
    pub name: String,
}

/// The project update request payload.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Builder)]
#[builder(name = "ProjectUpdateRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ProjectUpdateRequest {
    /// The updated name of the project, this name appears in reports.
    pub name: String,
}

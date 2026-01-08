use crate::types::OpenAIError;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use super::OrganizationRole;

/// Represents an individual `user` within an organization.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct User {
    /// The object type, which is always `organization.user`
    pub object: String,
    /// The identifier, which can be referenced in API endpoints
    pub id: String,
    /// The name of the user
    pub name: String,
    /// The email address of the user
    pub email: String,
    /// `owner` or `reader`
    pub role: OrganizationRole,
    /// The Unix timestamp (in seconds) of when the users was added.
    pub added_at: u32,
}

/// A list of `User` objects.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct UserListResponse {
    pub object: String,
    pub data: Vec<User>,
    pub first_id: String,
    pub last_id: String,
    pub has_more: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Builder)]
#[builder(name = "UserRoleUpdateRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct UserRoleUpdateRequest {
    /// `owner` or `reader`
    pub role: OrganizationRole,
}

/// Confirmation of the deleted user
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct UserDeleteResponse {
    pub object: String,
    pub id: String,
    pub deleted: bool,
}

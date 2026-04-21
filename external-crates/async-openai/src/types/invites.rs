use crate::types::OpenAIError;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use super::OrganizationRole;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum InviteStatus {
    Accepted,
    Expired,
    Pending,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Builder)]
#[builder(name = "InviteRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct InviteRequest {
    pub email: String,
    pub role: OrganizationRole,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct InviteListResponse {
    pub object: String,
    pub data: Vec<Invite>,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
    pub has_more: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct InviteDeleteResponse {
    /// The object type, which is always `organization.invite.deleted`
    pub object: String,
    pub id: String,
    pub deleted: bool,
}

/// Represents an individual `invite` to the organization.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Invite {
    /// The object type, which is always `organization.invite`
    pub object: String,
    /// The identifier, which can be referenced in API endpoints
    pub id: String,
    /// The email address of the individual to whom the invite was sent
    pub email: String,
    /// `owner` or `reader`
    pub role: OrganizationRole,
    /// `accepted`, `expired`, or `pending`
    pub status: InviteStatus,
    /// The Unix timestamp (in seconds) of when the invite was sent.
    pub invited_at: u32,
    /// The Unix timestamp (in seconds) of when the invite expires.
    pub expires_at: u32,
    /// The Unix timestamp (in seconds) of when the invite was accepted.
    pub accepted_at: Option<u32>,
}

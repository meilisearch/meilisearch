use serde::Serialize;

use crate::{
    config::Config,
    error::OpenAIError,
    types::{Invite, InviteDeleteResponse, InviteListResponse, InviteRequest},
    Client,
};

/// Invite and manage invitations for an organization. Invited users are automatically added to the Default project.
pub struct Invites<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> Invites<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    /// Returns a list of invites in the organization.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn list<Q>(&self, query: &Q) -> Result<InviteListResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client
            .get_with_query("/organization/invites", &query)
            .await
    }

    /// Retrieves an invite.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, invite_id: &str) -> Result<Invite, OpenAIError> {
        self.client
            .get(format!("/organization/invites/{invite_id}").as_str())
            .await
    }

    /// Create an invite for a user to the organization. The invite must be accepted by the user before they have access to the organization.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create(&self, request: InviteRequest) -> Result<Invite, OpenAIError> {
        self.client.post("/organization/invites", request).await
    }

    /// Delete an invite. If the invite has already been accepted, it cannot be deleted.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn delete(&self, invite_id: &str) -> Result<InviteDeleteResponse, OpenAIError> {
        self.client
            .delete(format!("/organization/invites/{invite_id}").as_str())
            .await
    }
}

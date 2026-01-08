use serde::Serialize;

use crate::{
    config::Config,
    error::OpenAIError,
    types::{User, UserDeleteResponse, UserListResponse, UserRoleUpdateRequest},
    Client,
};

/// Manage users and their role in an organization. Users will be automatically added to the Default project.
pub struct Users<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> Users<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    /// Lists all of the users in the organization.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn list<Q>(&self, query: &Q) -> Result<UserListResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client
            .get_with_query("/organization/users", &query)
            .await
    }

    /// Modifies a user's role in the organization.
    #[crate::byot(T0 = std::fmt::Display, T1 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn modify(
        &self,
        user_id: &str,
        request: UserRoleUpdateRequest,
    ) -> Result<User, OpenAIError> {
        self.client
            .post(format!("/organization/users/{user_id}").as_str(), request)
            .await
    }

    /// Retrieve a user by their identifier
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, user_id: &str) -> Result<User, OpenAIError> {
        self.client
            .get(format!("/organization/users/{user_id}").as_str())
            .await
    }

    /// Deletes a user from the organization.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn delete(&self, user_id: &str) -> Result<UserDeleteResponse, OpenAIError> {
        self.client
            .delete(format!("/organizations/users/{user_id}").as_str())
            .await
    }
}

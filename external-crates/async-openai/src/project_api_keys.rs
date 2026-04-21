use serde::Serialize;

use crate::{
    config::Config,
    error::OpenAIError,
    types::{ProjectApiKey, ProjectApiKeyDeleteResponse, ProjectApiKeyListResponse},
    Client,
};

/// Manage API keys for a given project. Supports listing and deleting keys for users.
/// This API does not allow issuing keys for users, as users need to authorize themselves to generate keys.
pub struct ProjectAPIKeys<'c, C: Config> {
    client: &'c Client<C>,
    pub project_id: String,
}

impl<'c, C: Config> ProjectAPIKeys<'c, C> {
    pub fn new(client: &'c Client<C>, project_id: &str) -> Self {
        Self {
            client,
            project_id: project_id.into(),
        }
    }

    /// Returns a list of API keys in the project.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn list<Q>(&self, query: &Q) -> Result<ProjectApiKeyListResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client
            .get_with_query(
                format!("/organization/projects/{}/api_keys", self.project_id).as_str(),
                &query,
            )
            .await
    }

    /// Retrieves an API key in the project.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, api_key: &str) -> Result<ProjectApiKey, OpenAIError> {
        self.client
            .get(
                format!(
                    "/organization/projects/{}/api_keys/{api_key}",
                    self.project_id
                )
                .as_str(),
            )
            .await
    }

    /// Deletes an API key from the project.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn delete(&self, api_key: &str) -> Result<ProjectApiKeyDeleteResponse, OpenAIError> {
        self.client
            .delete(
                format!(
                    "/organization/projects/{}/api_keys/{api_key}",
                    self.project_id
                )
                .as_str(),
            )
            .await
    }
}

use serde::Serialize;

use crate::{
    config::Config,
    error::OpenAIError,
    project_api_keys::ProjectAPIKeys,
    types::{Project, ProjectCreateRequest, ProjectListResponse, ProjectUpdateRequest},
    Client, ProjectServiceAccounts, ProjectUsers,
};

/// Manage the projects within an organization includes creation, updating, and archiving or projects.
/// The Default project cannot be modified or archived.
pub struct Projects<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> Projects<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    // call [ProjectUsers] group APIs
    pub fn users(&self, project_id: &str) -> ProjectUsers<C> {
        ProjectUsers::new(self.client, project_id)
    }

    // call [ProjectServiceAccounts] group APIs
    pub fn service_accounts(&self, project_id: &str) -> ProjectServiceAccounts<C> {
        ProjectServiceAccounts::new(self.client, project_id)
    }

    // call [ProjectAPIKeys] group APIs
    pub fn api_keys(&self, project_id: &str) -> ProjectAPIKeys<C> {
        ProjectAPIKeys::new(self.client, project_id)
    }

    /// Returns a list of projects.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn list<Q>(&self, query: &Q) -> Result<ProjectListResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client
            .get_with_query("/organization/projects", &query)
            .await
    }

    /// Create a new project in the organization. Projects can be created and archived, but cannot be deleted.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create(&self, request: ProjectCreateRequest) -> Result<Project, OpenAIError> {
        self.client.post("/organization/projects", request).await
    }

    /// Retrieves a project.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, project_id: String) -> Result<Project, OpenAIError> {
        self.client
            .get(format!("/organization/projects/{project_id}").as_str())
            .await
    }

    /// Modifies a project in the organization.
    #[crate::byot(T0 = std::fmt::Display, T1 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn modify(
        &self,
        project_id: String,
        request: ProjectUpdateRequest,
    ) -> Result<Project, OpenAIError> {
        self.client
            .post(
                format!("/organization/projects/{project_id}").as_str(),
                request,
            )
            .await
    }

    /// Archives a project in the organization. Archived projects cannot be used or updated.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn archive(&self, project_id: String) -> Result<Project, OpenAIError> {
        self.client
            .post(
                format!("/organization/projects/{project_id}/archive").as_str(),
                (),
            )
            .await
    }
}

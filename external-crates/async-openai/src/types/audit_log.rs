use serde::{Deserialize, Serialize};

/// The event type.
#[derive(Debug, Serialize, Deserialize)]
pub enum AuditLogEventType {
    #[serde(rename = "api_key.created")]
    ApiKeyCreated,
    #[serde(rename = "api_key.updated")]
    ApiKeyUpdated,
    #[serde(rename = "api_key.deleted")]
    ApiKeyDeleted,
    #[serde(rename = "invite.sent")]
    InviteSent,
    #[serde(rename = "invite.accepted")]
    InviteAccepted,
    #[serde(rename = "invite.deleted")]
    InviteDeleted,
    #[serde(rename = "login.succeeded")]
    LoginSucceeded,
    #[serde(rename = "login.failed")]
    LoginFailed,
    #[serde(rename = "logout.succeeded")]
    LogoutSucceeded,
    #[serde(rename = "logout.failed")]
    LogoutFailed,
    #[serde(rename = "organization.updated")]
    OrganizationUpdated,
    #[serde(rename = "project.created")]
    ProjectCreated,
    #[serde(rename = "project.updated")]
    ProjectUpdated,
    #[serde(rename = "project.archived")]
    ProjectArchived,
    #[serde(rename = "service_account.created")]
    ServiceAccountCreated,
    #[serde(rename = "service_account.updated")]
    ServiceAccountUpdated,
    #[serde(rename = "service_account.deleted")]
    ServiceAccountDeleted,
    #[serde(rename = "user.added")]
    UserAdded,
    #[serde(rename = "user.updated")]
    UserUpdated,
    #[serde(rename = "user.deleted")]
    UserDeleted,
}

/// Represents a list of audit logs.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListAuditLogsResponse {
    /// The object type, which is always `list`.
    pub object: String,
    /// A list of `AuditLog` objects.
    pub data: Vec<AuditLog>,
    /// The first `audit_log_id` in the retrieved `list`.
    pub first_id: String,
    /// The last `audit_log_id` in the retrieved `list`.
    pub last_id: String,
    /// The `has_more` property is used for pagination to indicate there are additional results.
    pub has_more: bool,
}

/// The project that the action was scoped to. Absent for actions not scoped to projects.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogProject {
    /// The project ID.
    pub id: String,
    /// The project title.
    pub name: String,
}

/// The actor who performed the audit logged action.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogActor {
    /// The type of actor. Is either `session` or `api_key`.
    pub r#type: String,
    /// The session in which the audit logged action was performed.
    pub session: Option<AuditLogActorSession>,
    /// The API Key used to perform the audit logged action.
    pub api_key: Option<AuditLogActorApiKey>,
}

/// The session in which the audit logged action was performed.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogActorSession {
    /// The user who performed the audit logged action.
    pub user: AuditLogActorUser,
    /// The IP address from which the action was performed.
    pub ip_address: String,
}

/// The API Key used to perform the audit logged action.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogActorApiKey {
    /// The tracking id of the API key.
    pub id: String,
    /// The type of API key. Can be either `user` or `service_account`.
    pub r#type: AuditLogActorApiKeyType,
    /// The user who performed the audit logged action, if applicable.
    pub user: Option<AuditLogActorUser>,
    /// The service account that performed the audit logged action, if applicable.
    pub service_account: Option<AuditLogActorServiceAccount>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditLogActorApiKeyType {
    User,
    ServiceAccount,
}

/// The user who performed the audit logged action.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogActorUser {
    /// The user id.
    pub id: String,
    /// The user email.
    pub email: String,
}

/// The service account that performed the audit logged action.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogActorServiceAccount {
    /// The service account id.
    pub id: String,
}

/// A log of a user action or configuration change within this organization.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLog {
    /// The ID of this log.
    pub id: String,
    /// The event type.
    pub r#type: AuditLogEventType,
    /// The Unix timestamp (in seconds) of the event.
    pub effective_at: u32,
    /// The project that the action was scoped to. Absent for actions not scoped to projects.
    pub project: Option<AuditLogProject>,
    /// The actor who performed the audit logged action.
    pub actor: AuditLogActor,
    /// The details for events with the type `api_key.created`.
    #[serde(rename = "api_key.created")]
    pub api_key_created: Option<AuditLogApiKeyCreated>,
    /// The details for events with the type `api_key.updated`.
    #[serde(rename = "api_key.updated")]
    pub api_key_updated: Option<AuditLogApiKeyUpdated>,
    /// The details for events with the type `api_key.deleted`.
    #[serde(rename = "api_key.deleted")]
    pub api_key_deleted: Option<AuditLogApiKeyDeleted>,
    /// The details for events with the type `invite.sent`.
    #[serde(rename = "invite.sent")]
    pub invite_sent: Option<AuditLogInviteSent>,
    /// The details for events with the type `invite.accepted`.
    #[serde(rename = "invite.accepted")]
    pub invite_accepted: Option<AuditLogInviteAccepted>,
    /// The details for events with the type `invite.deleted`.
    #[serde(rename = "invite.deleted")]
    pub invite_deleted: Option<AuditLogInviteDeleted>,
    /// The details for events with the type `login.failed`.
    #[serde(rename = "login.failed")]
    pub login_failed: Option<AuditLogLoginFailed>,
    /// The details for events with the type `logout.failed`.
    #[serde(rename = "logout.failed")]
    pub logout_failed: Option<AuditLogLogoutFailed>,
    /// The details for events with the type `organization.updated`.
    #[serde(rename = "organization.updated")]
    pub organization_updated: Option<AuditLogOrganizationUpdated>,
    /// The details for events with the type `project.created`.
    #[serde(rename = "project.created")]
    pub project_created: Option<AuditLogProjectCreated>,
    /// The details for events with the type `project.updated`.
    #[serde(rename = "project.updated")]
    pub project_updated: Option<AuditLogProjectUpdated>,
    /// The details for events with the type `project.archived`.
    #[serde(rename = "project.archived")]
    pub project_archived: Option<AuditLogProjectArchived>,
    /// The details for events with the type `service_account.created`.
    #[serde(rename = "service_account.created")]
    pub service_account_created: Option<AuditLogServiceAccountCreated>,
    /// The details for events with the type `service_account.updated`.
    #[serde(rename = "service_account.updated")]
    pub service_account_updated: Option<AuditLogServiceAccountUpdated>,
    /// The details for events with the type `service_account.deleted`.
    #[serde(rename = "service_account.deleted")]
    pub service_account_deleted: Option<AuditLogServiceAccountDeleted>,
    /// The details for events with the type `user.added`.
    #[serde(rename = "user.added")]
    pub user_added: Option<AuditLogUserAdded>,
    /// The details for events with the type `user.updated`.
    #[serde(rename = "user.updated")]
    pub user_updated: Option<AuditLogUserUpdated>,
    /// The details for events with the type `user.deleted`.
    #[serde(rename = "user.deleted")]
    pub user_deleted: Option<AuditLogUserDeleted>,
}

/// The details for events with the type `api_key.created`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogApiKeyCreated {
    /// The tracking ID of the API key.
    pub id: String,
    /// The payload used to create the API key.
    pub data: Option<AuditLogApiKeyCreatedData>,
}

/// The payload used to create the API key.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogApiKeyCreatedData {
    /// A list of scopes allowed for the API key, e.g. `["api.model.request"]`.
    pub scopes: Option<Vec<String>>,
}

/// The details for events with the type `api_key.updated`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogApiKeyUpdated {
    /// The tracking ID of the API key.
    pub id: String,
    /// The payload used to update the API key.
    pub changes_requested: Option<AuditLogApiKeyUpdatedChangesRequested>,
}

/// The payload used to update the API key.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogApiKeyUpdatedChangesRequested {
    /// A list of scopes allowed for the API key, e.g. `["api.model.request"]`.
    pub scopes: Option<Vec<String>>,
}

/// The details for events with the type `api_key.deleted`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogApiKeyDeleted {
    /// The tracking ID of the API key.
    pub id: String,
}

/// The details for events with the type `invite.sent`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogInviteSent {
    /// The ID of the invite.
    pub id: String,
    /// The payload used to create the invite.
    pub data: Option<AuditLogInviteSentData>,
}

/// The payload used to create the invite.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogInviteSentData {
    /// The email invited to the organization.
    pub email: String,
    /// The role the email was invited to be. Is either `owner` or `member`.
    pub role: String,
}

/// The details for events with the type `invite.accepted`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogInviteAccepted {
    /// The ID of the invite.
    pub id: String,
}

/// The details for events with the type `invite.deleted`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogInviteDeleted {
    /// The ID of the invite.
    pub id: String,
}

/// The details for events with the type `login.failed`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogLoginFailed {
    /// The error code of the failure.
    pub error_code: String,
    /// The error message of the failure.
    pub error_message: String,
}

/// The details for events with the type `logout.failed`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogLogoutFailed {
    /// The error code of the failure.
    pub error_code: String,
    /// The error message of the failure.
    pub error_message: String,
}

/// The details for events with the type `organization.updated`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogOrganizationUpdated {
    /// The organization ID.
    pub id: String,
    /// The payload used to update the organization settings.
    pub changes_requested: Option<AuditLogOrganizationUpdatedChangesRequested>,
}

/// The payload used to update the organization settings.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogOrganizationUpdatedChangesRequested {
    /// The organization title.
    pub title: Option<String>,
    /// The organization description.
    pub description: Option<String>,
    /// The organization name.
    pub name: Option<String>,
    /// The organization settings.
    pub settings: Option<AuditLogOrganizationUpdatedChangesRequestedSettings>,
}

/// The organization settings.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogOrganizationUpdatedChangesRequestedSettings {
    /// Visibility of the threads page which shows messages created with the Assistants API and Playground. One of `ANY_ROLE`, `OWNERS`, or `NONE`.
    pub threads_ui_visibility: Option<String>,
    /// Visibility of the usage dashboard which shows activity and costs for your organization. One of `ANY_ROLE` or `OWNERS`.
    pub usage_dashboard_visibility: Option<String>,
}

/// The details for events with the type `project.created`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogProjectCreated {
    /// The project ID.
    pub id: String,
    /// The payload used to create the project.
    pub data: Option<AuditLogProjectCreatedData>,
}

/// The payload used to create the project.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogProjectCreatedData {
    /// The project name.
    pub name: String,
    /// The title of the project as seen on the dashboard.
    pub title: Option<String>,
}

/// The details for events with the type `project.updated`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogProjectUpdated {
    /// The project ID.
    pub id: String,
    /// The payload used to update the project.
    pub changes_requested: Option<AuditLogProjectUpdatedChangesRequested>,
}

/// The payload used to update the project.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogProjectUpdatedChangesRequested {
    /// The title of the project as seen on the dashboard.
    pub title: Option<String>,
}

/// The details for events with the type `project.archived`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogProjectArchived {
    /// The project ID.
    pub id: String,
}

/// The details for events with the type `service_account.created`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogServiceAccountCreated {
    /// The service account ID.
    pub id: String,
    /// The payload used to create the service account.
    pub data: Option<AuditLogServiceAccountCreatedData>,
}

/// The payload used to create the service account.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogServiceAccountCreatedData {
    /// The role of the service account. Is either `owner` or `member`.
    pub role: String,
}

/// The details for events with the type `service_account.updated`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogServiceAccountUpdated {
    /// The service account ID.
    pub id: String,
    /// The payload used to updated the service account.
    pub changes_requested: Option<AuditLogServiceAccountUpdatedChangesRequested>,
}

/// The payload used to updated the service account.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogServiceAccountUpdatedChangesRequested {
    /// The role of the service account. Is either `owner` or `member`.
    pub role: String,
}

/// The details for events with the type `service_account.deleted`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogServiceAccountDeleted {
    /// The service account ID.
    pub id: String,
}

/// The details for events with the type `user.added`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogUserAdded {
    /// The user ID.
    pub id: String,
    /// The payload used to add the user to the project.
    pub data: Option<AuditLogUserAddedData>,
}

/// The payload used to add the user to the project.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogUserAddedData {
    /// The role of the user. Is either `owner` or `member`.
    pub role: String,
}

/// The details for events with the type `user.updated`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogUserUpdated {
    /// The project ID.
    pub id: String,
    /// The payload used to update the user.
    pub changes_requested: Option<AuditLogUserUpdatedChangesRequested>,
}

/// The payload used to update the user.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogUserUpdatedChangesRequested {
    /// The role of the user. Is either `owner` or `member`.
    pub role: String,
}

/// The details for events with the type `user.deleted`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogUserDeleted {
    /// The user ID.
    pub id: String,
}

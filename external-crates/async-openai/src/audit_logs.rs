use serde::Serialize;

use crate::{config::Config, error::OpenAIError, types::ListAuditLogsResponse, Client};

/// Logs of user actions and configuration changes within this organization.
/// To log events, you must activate logging in the [Organization Settings](https://platform.openai.com/settings/organization/general).
/// Once activated, for security reasons, logging cannot be deactivated.
pub struct AuditLogs<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> AuditLogs<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    /// List user actions and configuration changes within this organization.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn get<Q>(&self, query: &Q) -> Result<ListAuditLogsResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client
            .get_with_query("/organization/audit_logs", &query)
            .await
    }
}

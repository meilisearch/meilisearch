use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Webhook {
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

impl Webhook {
    pub fn redact_authorization_header(&mut self) {
        // headers are case insensitive, so to make the redaction robust we iterate over qualifying headers
        // rather than getting one canonical `Authorization` header.
        for value in self
            .headers
            .iter_mut()
            .filter_map(|(name, value)| name.eq_ignore_ascii_case("authorization").then_some(value))
        {
            if value.starts_with("Bearer ") {
                crate::settings::hide_secret(value, "Bearer ".len());
            } else {
                crate::settings::hide_secret(value, 0);
            }
        }
    }
}

#[derive(Debug, Serialize, Default, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WebhooksView {
    #[serde(default)]
    pub webhooks: BTreeMap<Uuid, Webhook>,
}

// Same as the WebhooksView instead it should never contains the CLI webhooks.
// It's the right structure to use in the dump
#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WebhooksDumpView {
    #[serde(default)]
    pub webhooks: BTreeMap<Uuid, Webhook>,
}

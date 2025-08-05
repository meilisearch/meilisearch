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

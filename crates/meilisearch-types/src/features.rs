use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct RuntimeTogglableFeatures {
    pub metrics: bool,
    pub logs_route: bool,
    pub edit_documents_by_function: bool,
    pub contains_filter: bool,
    pub network: bool,
    pub get_task_documents_route: bool,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct InstanceTogglableFeatures {
    pub metrics: bool,
    pub logs_route: bool,
    pub contains_filter: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Remote {
    pub url: String,
    #[serde(default)]
    pub search_api_key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Network {
    #[serde(default, rename = "self")]
    pub local: Option<String>,
    #[serde(default)]
    pub remotes: BTreeMap<String, Remote>,
}

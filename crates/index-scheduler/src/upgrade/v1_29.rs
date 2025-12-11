use std::collections::BTreeMap;

use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Env, RoTxn, WithoutTls};
use serde::{Deserialize, Serialize};

use crate::Result;

/// Database const names for the `FeatureData`.
mod db_name {
    pub const EXPERIMENTAL_FEATURES: &str = "experimental-features";
}

mod db_keys {
    pub const NETWORK: &str = "network";
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Network {
    #[serde(default, rename = "self")]
    pub local: Option<String>,
    #[serde(default)]
    pub remotes: BTreeMap<String, Remote>,
    #[serde(default)]
    pub sharding: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Remote {
    pub url: String,
    #[serde(default)]
    pub search_api_key: Option<String>,
    #[serde(default)]
    pub write_api_key: Option<String>,
}

pub fn get_network(env: &Env<WithoutTls>, rtxn: &RoTxn) -> Result<Option<Network>> {
    let Some(network_db) =
        env.open_database::<Str, SerdeJson<Network>>(rtxn, Some(db_name::EXPERIMENTAL_FEATURES))?
    else {
        return Ok(None);
    };

    Ok(network_db.get(rtxn, db_keys::NETWORK)?)
}

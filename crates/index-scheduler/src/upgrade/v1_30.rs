use std::collections::BTreeMap;

use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Env, RoTxn, RwTxn, WithoutTls};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Network {
    #[serde(default, rename = "self")]
    pub local: Option<String>,
    #[serde(default)]
    pub remotes: BTreeMap<String, Remote>,
    #[serde(default)]
    pub leader: Option<String>,
    #[serde(default)]
    pub version: Uuid,
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

use super::v1_29;
use crate::Result;

/// Database const names for the `FeatureData`.
mod db_name {
    pub const EXPERIMENTAL_FEATURES: &str = "experimental-features";
}

mod db_keys {
    pub const NETWORK: &str = "network";
}

pub struct MigrateNetwork;

impl super::UpgradeIndexScheduler for MigrateNetwork {
    fn upgrade(&self, env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> anyhow::Result<()> {
        let Some(v1_29::Network { local, remotes, sharding }) = v1_29::get_network(env, wtxn)?
        else {
            return Ok(());
        };

        let leader = if sharding { remotes.keys().next().cloned() } else { None };

        let remotes = remotes
            .into_iter()
            .map(|(name, v1_29::Remote { url, search_api_key, write_api_key })| {
                (name, Remote { url, search_api_key, write_api_key })
            })
            .collect();

        let network = Network { local, remotes, leader, version: Uuid::nil() };

        set_network(env, wtxn, &network)?;
        Ok(())
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 30, 0)
    }

    fn description(&self) -> &'static str {
        "updating the network struct"
    }
}

fn set_network(env: &Env<WithoutTls>, wtxn: &mut RwTxn<'_>, network: &Network) -> Result<()> {
    let network_db =
        env.create_database::<Str, SerdeJson<Network>>(wtxn, Some(db_name::EXPERIMENTAL_FEATURES))?;

    network_db.put(wtxn, db_keys::NETWORK, network)?;
    Ok(())
}

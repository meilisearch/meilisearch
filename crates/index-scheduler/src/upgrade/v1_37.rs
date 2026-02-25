use std::collections::{BTreeMap, BTreeSet};

use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Env, RwTxn, WithoutTls};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::v1_30;
use crate::Result;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Network {
    #[serde(default, rename = "self")]
    pub local: Option<String>,
    #[serde(default)]
    pub remotes: BTreeMap<String, Remote>,
    #[serde(default)]
    pub shards: BTreeMap<String, Shard>,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Shard {
    pub remotes: BTreeSet<String>,
}

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
        let Some(v1_30::Network { local, remotes, leader, version }) =
            v1_30::get_network(env, wtxn)?
        else {
            return Ok(());
        };

        // shards: create one shard per remote, with the remote's name as shard name
        // (important to not change the result of rendezvous hashing)
        // and have the remote as sole owner of the shard.
        let shards = remotes
            .keys()
            .map(|remote_name| {
                (remote_name.clone(), Shard { remotes: BTreeSet::from_iter([remote_name.clone()]) })
            })
            .collect();

        // remotes are actually unchanged
        let remotes = remotes
            .into_iter()
            .map(|(name, v1_30::Remote { url, search_api_key, write_api_key })| {
                (name, Remote { url, search_api_key, write_api_key })
            })
            .collect();

        let network = Network { local, remotes, leader, version, shards };
        set_network(env, wtxn, &network)?;
        Ok(())
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 37, 0)
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

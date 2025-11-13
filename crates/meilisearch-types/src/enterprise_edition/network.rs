// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;

use milli::update::new::indexer::enterprise_edition::sharding::Shards;
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

impl Network {
    pub fn shards(&self) -> Option<Shards> {
        if self.leader.is_some() {
            Some(Shards::from_remotes_local(
                self.remotes.keys().map(String::as_str),
                self.local.as_deref(),
            ))
        } else {
            None
        }
    }
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

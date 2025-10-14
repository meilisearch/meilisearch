// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;

use deserr::Deserr;
use milli::update::new::indexer::enterprise_edition::sharding::{Shard, Shards};
use milli::update::Setting;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::deserr::DeserrJsonError;
use crate::error::deserr_codes::{
    InvalidNetworkRemotes, InvalidNetworkSearchApiKey, InvalidNetworkSelf, InvalidNetworkSharding,
    InvalidNetworkUrl, InvalidNetworkWriteApiKey,
};

#[derive(Clone, Debug, Deserr, ToSchema, Serialize, Deserialize, PartialEq, Eq)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct Network {
    #[schema(value_type = Option<BTreeMap<String, Remote>>, example = json!("http://localhost:7700"))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkRemotes>)]
    #[serde(default)]
    pub remotes: Setting<BTreeMap<String, Option<Remote>>>,
    #[schema(value_type = Option<String>, example = json!("ms-00"), rename = "self")]
    #[serde(default, rename = "self")]
    #[deserr(default, rename = "self", error = DeserrJsonError<InvalidNetworkSelf>)]
    pub local: Setting<String>,
    #[schema(value_type = Option<bool>, example = json!(true))]
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkSharding>)]
    pub sharding: Setting<bool>,
}

#[derive(Clone, Debug, Deserr, ToSchema, Serialize, Deserialize, PartialEq, Eq)]
#[deserr(error = DeserrJsonError<InvalidNetworkRemotes>, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct Remote {
    #[schema(value_type = Option<String>, example = json!({
        "ms-0": Remote { url: Setting::Set("http://localhost:7700".into()), search_api_key: Setting::Reset, write_api_key: Setting::Reset },
        "ms-1": Remote { url: Setting::Set("http://localhost:7701".into()), search_api_key: Setting::Set("foo".into()), write_api_key: Setting::Set("bar".into()) },
        "ms-2": Remote { url: Setting::Set("http://localhost:7702".into()), search_api_key: Setting::Set("bar".into()), write_api_key: Setting::Set("foo".into()) },
    }))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkUrl>)]
    #[serde(default)]
    pub url: Setting<String>,
    #[schema(value_type = Option<String>, example = json!("XWnBI8QHUc-4IlqbKPLUDuhftNq19mQtjc6JvmivzJU"))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkSearchApiKey>)]
    #[serde(default)]
    pub search_api_key: Setting<String>,
    #[schema(value_type = Option<String>, example = json!("XWnBI8QHUc-4IlqbKPLUDuhftNq19mQtjc6JvmivzJU"))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkWriteApiKey>)]
    #[serde(default)]
    pub write_api_key: Setting<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DbNetwork {
    #[serde(default, rename = "self")]
    pub local: Option<String>,
    #[serde(default)]
    pub remotes: BTreeMap<String, DbRemote>,
    #[serde(default)]
    pub sharding: bool,
}

impl DbNetwork {
    pub fn shards(&self) -> Option<Shards> {
        if self.sharding {
            let this = self.local.as_deref();

            Some(Shards(
                self.remotes
                    .keys()
                    .map(|name| Shard {
                        is_own: Some(name.as_str()) == this,
                        name: name.to_owned(),
                    })
                    .collect(),
            ))
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DbRemote {
    pub url: String,
    #[serde(default)]
    pub search_api_key: Option<String>,
    #[serde(default)]
    pub write_api_key: Option<String>,
}

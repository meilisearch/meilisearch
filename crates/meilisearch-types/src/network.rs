use std::collections::{BTreeMap, BTreeSet};

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

pub mod route {
    use actix_web::error::HttpError;
    use actix_web::http::uri::PathAndQuery;
    use actix_web::http::Uri;
    use serde::{Deserialize, Serialize};

    use crate::tasks::network::Origin;

    pub const NETWORK_PATH_SUFFIX: &str = "/change";

    pub fn network_change_path() -> PathAndQuery {
        PathAndQuery::from_static("/network/change")
    }

    pub fn url_from_base_and_route(
        remote_base_url: &str,
        route: PathAndQuery,
    ) -> Result<Uri, HttpError> {
        let mut base_url_parts = Uri::try_from(remote_base_url)?.into_parts();
        base_url_parts.path_and_query = Some(route);
        Ok(Uri::from_parts(base_url_parts)?)
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", tag = "type")]
    pub enum Message {
        ExportNoIndexForRemote { remote: String },
        ImportFinishedForRemote { remote: String, successful: bool },
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct NetworkChange {
        /// The origin of this message
        pub origin: Origin,
        pub message: Message,
    }
}

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
    use utoipa::ToSchema;

    use crate::tasks::network::Origin;

    pub fn network_control_path() -> PathAndQuery {
        // WARNING: if you change this path, you must also change the path in the network route macro in the meilisearch crate.
        PathAndQuery::from_static("/network/control")
    }

    pub fn url_from_base_and_route(
        remote_base_url: &str,
        route: PathAndQuery,
    ) -> Result<Uri, HttpError> {
        let mut base_url_parts = Uri::try_from(remote_base_url)?.into_parts();
        base_url_parts.path_and_query = Some(route);
        Ok(Uri::from_parts(base_url_parts)?)
    }

    #[derive(Serialize, Deserialize, ToSchema)]
    #[serde(rename_all = "camelCase", tag = "type")]
    #[schema(rename_all = "camelCase")]
    pub enum Message {
        /// The specified remote will not longer export any document to this instance.
        ///
        /// Send this message to remotes that are blocked waiting on the specified remote to export its documents.
        ExportNoIndexForRemote {
            /// Name of the remote that will no longer export any document to this instance.
            remote: String,
        },
        /// The specified remote is finished importing its documents.
        ///
        /// Send this message to remotes that are blocked waiting on the specified remote to finish importing its documents.
        ImportFinishedForRemote {
            /// Name of the remote that finished importing its documents.
            remote: String,
            /// Whether the import was successful.
            ///
            /// Documents from shards that no longer belong to remotes are only deleted if all remotes are successful
            /// importing their documents.
            successful: bool,
        },
    }

    #[derive(Serialize, Deserialize, ToSchema)]
    #[serde(rename_all = "camelCase")]
    #[schema(rename_all = "camelCase")]
    pub struct NetworkChange {
        /// The origin of this message
        ///
        /// Get it in the details of the network topology change task that is currently processing.
        pub origin: Origin,
        /// Message to send to control the network topology change task.
        pub message: Message,
    }
}

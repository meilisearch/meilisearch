use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use papaya::HashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const BASE_UNAVAILABILITY_DURATION: Duration = Duration::from_secs(30); // 30s
const MAX_UNAVAILABILITY_DURATION: Duration = Duration::from_mins(5); //   5min

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
    #[serde(skip_deserializing)]
    pub status: route::Status,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Shard {
    pub remotes: BTreeSet<String>,
}

/// Keeps track of the unavailability period for each remote.
#[derive(Debug)]
pub struct RemoteAvailability(HashMap<String, Unavailability>);

impl Default for RemoteAvailability {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoteAvailability {
    pub fn new() -> Self {
        Self(HashMap::default())
    }

    /// Returns `true` if the remote is available, `false` otherwise.
    ///
    /// Note that this method modifies the internal state by removing
    /// any expired unavailability periods.
    pub fn is_available(&self, remote: &str) -> bool {
        self.0.pin().remove_if(remote, |_, u| Unavailability::is_available(u)).is_ok()
    }

    /// Marks a remote as unavailable indefinitely, removing any existing unavailability period.
    pub fn mark_unavailable_indefinitely(&self, remote: String) {
        self.0.pin().update_or_insert_with(
            remote,
            |_| Unavailability::indefinite(),
            Unavailability::indefinite,
        );
    }

    /// Marks a remote as unavailable, extending the existing unavailability period if any.
    pub fn mark_unavailable(&self, remote: String) {
        self.0.pin().update_or_insert_with(
            remote,
            Unavailability::next_unavailability,
            Unavailability::default,
        );
    }

    /// Marks a remote as available, removing any existing unavailability period.
    pub fn mark_available(&self, remote: &str) {
        self.0.pin().remove(remote);
    }
}

/// Represents an unavailability period for a remote index
#[derive(Debug)]
struct Unavailability {
    /// From when the unavailability started
    since: Instant,
    /// Until when the unavailability ends. None means the unavailability is indefinite.
    until: Option<Instant>,
}

impl Unavailability {
    fn indefinite() -> Self {
        Self { since: Instant::now(), until: None }
    }

    fn is_available(&self) -> bool {
        self.until.is_some_and(|u| Instant::now() > u)
    }

    fn next_unavailability(&self) -> Self {
        let Unavailability { since, until } = *self;
        match until {
            Some(until) => {
                let now = Instant::now();
                let new_duration = ((until - since) * 2).min(MAX_UNAVAILABILITY_DURATION);
                Self { since: now, until: Some(now + new_duration) }
            }
            None => Unavailability { since, until: None },
        }
    }
}

impl Default for Unavailability {
    fn default() -> Self {
        let now = Instant::now();
        Self { since: now, until: Some(now + BASE_UNAVAILABILITY_DURATION) }
    }
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
        /// The specified remote will see it's status change.
        ///
        /// Send this message to change the accessiblity of a remote.
        StatusChangeForRemote {
            /// Name of the remote whose status will be changed.
            remote: String,
            /// The new status for the remote.
            status: Status,
        },
    }

    #[derive(Debug, Default, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
    #[serde(rename_all = "camelCase")]
    #[schema(rename_all = "camelCase")]
    pub enum Status {
        #[default]
        Available,
        Unavailable,
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

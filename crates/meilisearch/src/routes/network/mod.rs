use std::collections::{BTreeMap, BTreeSet};

use actix_web::web::{self, Data, Json};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use itertools::{EitherOrBoth, Itertools};
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{
    InvalidNetworkLeader, InvalidNetworkRemotes, InvalidNetworkSearchApiKey, InvalidNetworkSelf,
    InvalidNetworkShards, InvalidNetworkUrl, InvalidNetworkWriteApiKey,
};
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::network::{
    route, Network as DbNetwork, Remote as DbRemote, Shard as DbShard,
};
use serde::Serialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use crate::analytics::{Aggregate, Analytics};
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

#[cfg(not(feature = "enterprise"))]
mod community_edition;

#[cfg(feature = "enterprise")]
mod enterprise_edition;
#[cfg(not(feature = "enterprise"))]
use community_edition as current_edition;
#[cfg(feature = "enterprise")]
use enterprise_edition as current_edition;

#[derive(OpenApi)]
#[openapi(
    paths(get_network, patch_network),
    tags((
        name = "Network",
        description = "The `/network` route allows you to describe the topology of a network of Meilisearch instances.

This route is **synchronous**. This means that no task object will be returned, and any change to the network will be made available immediately.",
    )),
)]
pub struct NetworkApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(get_network))
            .route(web::patch().to(SeqHandler(patch_network))),
    )
    .service(
        web::resource(route::NETWORK_PATH_SUFFIX)
            .route(web::post().to(SeqHandler(post_network_change))),
    );
}

/// Get network topology
///
/// Return the list of Meilisearch instances currently known to this node (self and remotes).
#[utoipa::path(
    get,
    path = "",
    tag = "Experimental features",
    security(("Bearer" = ["network.get", "*"])),
    responses(
        (status = OK, description = "Known nodes are returned.", body = Network, content_type = "application/json", example = json!(
            {
            "self": "ms-0",
            "remotes": {
                "ms-0": Remote { url: Setting::Set("http://localhost:7700".into()), search_api_key: Setting::Reset, write_api_key: Setting::Reset },
                "ms-1": Remote { url: Setting::Set("http://localhost:7701".into()), search_api_key: Setting::Set("foo".into()), write_api_key: Setting::Set("bar".into()) },
                "ms-2": Remote { url: Setting::Set("http://localhost:7702".into()), search_api_key: Setting::Set("bar".into()), write_api_key: Setting::Set("foo".into()) },
        }
    })),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
async fn get_network(
    index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_GET }>, Data<IndexScheduler>>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_network("Using the /network route")?;

    let network = index_scheduler.network();
    debug!(returns = ?network, "Get network");
    Ok(HttpResponse::Ok().json(network))
}

/// Configuration for a remote Meilisearch instance
#[derive(Clone, Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError<InvalidNetworkRemotes>, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct Remote {
    /// URL of the remote instance
    #[schema(value_type = Option<String>, example = "http://localhost:7700")]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkUrl>)]
    #[serde(default)]
    pub url: Setting<String>,
    /// API key for search operations on this remote
    #[schema(value_type = Option<String>, example = json!("XWnBI8QHUc-4IlqbKPLUDuhftNq19mQtjc6JvmivzJU"))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkSearchApiKey>)]
    #[serde(default)]
    pub search_api_key: Setting<String>,
    /// API key for write operations on this remote
    #[schema(value_type = Option<String>, example = json!("XWnBI8QHUc-4IlqbKPLUDuhftNq19mQtjc6JvmivzJU"))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkWriteApiKey>)]
    #[serde(default)]
    pub write_api_key: Setting<String>,
}

/// Configuration for a named shard of the
#[derive(Clone, Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError<InvalidNetworkShards>, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct Shard {
    /// List of remotes that own this shard.
    ///
    /// - The remotes must be part of the network's configuration
    /// - Setting this to a non-`null` value will replace all existing remotes for this shard.
    /// - `addRemotes` and `removeRemotes` are applied after `remotes` if multiple options are present.
    #[deserr(default, error = DeserrJsonError<InvalidNetworkRemotes>)]
    #[serde(default)]
    pub remotes: Option<BTreeSet<String>>,
    /// Remotes to add to the list of owners of this shard.
    ///
    /// - The remotes must be part of the network's configuration
    /// - Setting this to non-`null` will append the listed remotes to the list of owners of this shard.
    /// - `remotes` is applied before `addRemotes`
    /// - `removeRemotes` is applied after `addRemotes`
    #[deserr(default, error = DeserrJsonError<InvalidNetworkRemotes>)]
    #[serde(default)]
    pub add_remotes: Option<BTreeSet<String>>,
    /// Remotes to remove from the list of owners of this shard.
    ///
    /// - The remotes may or may not be part of the network's configuration
    /// - Setting this to non-`null` will remove the listed remotes from the list of owners of this shard.
    /// - `remotes` and `addRemotes` are applied before `removeRemotes`
    /// - Remotes removed from the configuration are automatically removed from all shards and it is not necessary
    ///   to explicitly pass them as `removeRemotes`.
    #[deserr(default, error = DeserrJsonError<InvalidNetworkRemotes>)]
    #[serde(default)]
    pub remove_remotes: Option<BTreeSet<String>>,
}

impl Shard {
    fn into_db_shard(self, old_remotes: BTreeSet<String>) -> DbShard {
        let Shard { remotes: new_remotes, add_remotes, remove_remotes } = self;
        let mut merged_remotes = match new_remotes {
            Some(remotes) => remotes,
            None => old_remotes,
        };
        if let Some(add_remotes) = add_remotes {
            merged_remotes = &merged_remotes | &add_remotes;
        }
        if let Some(remove_remotes) = remove_remotes {
            merged_remotes = &merged_remotes - &remove_remotes;
        }
        DbShard { remotes: merged_remotes }
    }
}

/// Network topology configuration for distributed Meilisearch
#[derive(Clone, Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct Network {
    /// Map of remote instance names to their configurations
    ///
    /// - Pass `null` as a value for a remote to remove it from the configuration.
    /// - Removing a remote will also remove it from all shards.
    /// - Remotes that don't appear in this list will be unmodified by the network call.
    #[schema(required = false, value_type = Option<BTreeMap<String, Remote>>, example = json!({
        "ms-00": {
            "url": "http://localhost:7700"
        },
        "ms-01": {
            "url": "http://localhost:7701"
        }
    }))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkRemotes>)]
    #[serde(default)]
    pub remotes: Setting<BTreeMap<String, Option<Remote>>>,
    /// Map of shard names to their configurations.
    ///
    /// - Pass `null` as a value for a shard to remove it from the configuration.
    /// - Shards that don't appear in this list will be unmodified by the network call.
    #[schema(required = false, value_type = Option<BTreeMap<String, Shard>>, example = json!({
        "shard-00": {
            "remotes": ["ms-00", "ms-01"]
        }
    }))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkShards>)]
    #[serde(default)]
    pub shards: Setting<BTreeMap<String, Option<Shard>>>,
    /// Previous shard configurations
    ///
    /// This field should not be passed by end-users. It is used in internal communications between Meilisearch instances
    #[schema(required = false, value_type = Option<BTreeMap<String, Shard>>, example = json!({
        "shard-00": {
            "remotes": ["ms-00", "ms-01"]
        }
    }))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkShards>)]
    #[serde(default)]
    pub previous_shards: Setting<BTreeMap<String, Option<Shard>>>,
    /// Name of this instance in the network
    #[schema(required = false, value_type = Option<String>, example = json!("ms-00"), rename = "self")]
    #[serde(default, rename = "self")]
    #[deserr(default, rename = "self", error = DeserrJsonError<InvalidNetworkSelf>)]
    pub local: Setting<String>,
    /// Name of the leader instance in the network
    #[schema(required = false, value_type = Option<String>, example = json!("ms-00"))]
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkLeader>)]
    pub leader: Setting<String>,
    /// Previous remote configurations
    ///
    /// This field should not be passed by end-users. It is used in internal communications between Meilisearch instances
    #[schema(required = false, value_type = Option<BTreeMap<String, Remote>>, example = json!({
        "ms-00": {
            "url": "http://localhost:7700"
        },
        "ms-01": {
            "url": "http://localhost:7701"
        }
    }))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkRemotes>)]
    #[serde(default)]
    pub previous_remotes: Setting<BTreeMap<String, Option<Remote>>>,
}

impl Remote {
    pub fn try_into_db_node(self, name: &str) -> Result<DbRemote, ResponseError> {
        Ok(DbRemote {
            url: self
                .url
                .set()
                .ok_or(ResponseError::from_msg(
                    format!("Missing field `.remotes.{name}.url`"),
                    meilisearch_types::error::Code::MissingNetworkUrl,
                ))
                .and_then(|url| {
                    if let Err(error) = url::Url::parse(&url) {
                        return Err(ResponseError::from_msg(
                            format!("Invalid `.remotes.{name}.url` (`{url}`): {error}"),
                            meilisearch_types::error::Code::InvalidNetworkUrl,
                        ));
                    }
                    Ok(url)
                })?,
            search_api_key: self.search_api_key.set(),
            write_api_key: self.write_api_key.set(),
        })
    }
}

#[derive(Serialize)]
pub struct PatchNetworkAnalytics {
    network_size: usize,
    network_has_self: bool,
}

impl Aggregate for PatchNetworkAnalytics {
    fn event_name(&self) -> &'static str {
        "Network Updated"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self { network_size: new.network_size, network_has_self: new.network_has_self })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Configure network topology
///
/// Add or remove remote nodes from the network. Changes apply to the current instance’s view of the cluster.
#[utoipa::path(
    patch,
    path = "",
    tag = "Experimental features",
    request_body = Network,
    security(("Bearer" = ["network.update", "*"])),
    responses(
        (status = OK, description = "New network state is returned.",  body = Network, content_type = "application/json", example = json!(
            {
                "self": "ms-0",
                "remotes": {
                    "ms-0": Remote { url: Setting::Set("http://localhost:7700".into()), search_api_key: Setting::Reset, write_api_key: Setting::Reset },
                    "ms-1": Remote { url: Setting::Set("http://localhost:7701".into()), search_api_key: Setting::Set("foo".into()), write_api_key: Setting::Set("bar".into()) },
                    "ms-2": Remote { url: Setting::Set("http://localhost:7702".into()), search_api_key: Setting::Set("bar".into()), write_api_key: Setting::Set("foo".into()) },
            }
        })),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
async fn patch_network(
    index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_UPDATE }>, Data<IndexScheduler>>,
    new_network: AwebJson<Network, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_network("Using the /network route")?;
    current_edition::patch_network(index_scheduler, new_network, req, analytics).await
}

async fn post_network_change(
    index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_UPDATE }>, Data<IndexScheduler>>,
    payload: Json<route::NetworkChange>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_network("Using the /network route")?;
    current_edition::post_network_change(index_scheduler, payload.into_inner()).await
}

/// Merges existing network from the DB with the incoming network patch.
///
/// **If the resulting network has a leader**, then it verifies the following post-conditions
///
/// 1. One of the remotes is the leader
/// 2. There exists at least one shard.
/// 3. Any shard has at least one remote.
/// 4. Any remote owning a shard is in the list of remotes.
fn merge_networks(
    old_network: DbNetwork,
    new_network: Network,
) -> Result<DbNetwork, ResponseError> {
    let DbNetwork {
        local: old_local,
        remotes: old_remotes,
        shards: old_shards,
        leader: old_leader,
        version: _,
    } = old_network;
    let Network {
        remotes: new_remotes,
        shards: new_shards,
        local: new_local,
        leader: new_leader,
        previous_remotes: _,
        previous_shards: _,
    } = new_network;

    let merged_self = match new_local {
        Setting::Set(new_self) => Some(new_self),
        Setting::Reset => None,
        Setting::NotSet => old_local,
    };
    let merged_leader = match new_leader {
        Setting::Set(new_leader) => Some(new_leader),
        Setting::Reset => None,
        Setting::NotSet => old_leader,
    };
    match (merged_leader.as_deref(), merged_self.as_deref()) {
        // 1. Always allowed if there is no leader
        (None, _) => (),
        // 2. Allowed if the leader is self
        (Some(leader), Some(this)) if leader == this => (),
        // 3. Any other change is disallowed
        (Some(leader), _) => {
            return Err(MeilisearchHttpError::NotLeader { leader: leader.to_string() }.into())
        }
    }
    let new_version = uuid::Uuid::now_v7();

    let mut merged_shards = match new_shards {
        Setting::Set(new_shards) => {
            let mut merged_shards = BTreeMap::new();
            for either_or_both in old_shards
                .into_iter()
                .merge_join_by(new_shards.into_iter(), |left, right| left.0.cmp(&right.0))
            {
                match either_or_both {
                    EitherOrBoth::Both((name, old_shard), (_, Some(new_shard))) => {
                        merged_shards.insert(name, new_shard.into_db_shard(old_shard.remotes));
                    }
                    EitherOrBoth::Both((_, _), (_, None)) | EitherOrBoth::Right((_, None)) => {}
                    EitherOrBoth::Left((name, shard)) => {
                        merged_shards.insert(name, shard);
                    }
                    EitherOrBoth::Right((name, Some(shard))) => {
                        merged_shards.insert(name, shard.into_db_shard(Default::default()));
                    }
                }
            }
            merged_shards
        }
        Setting::Reset => BTreeMap::new(),
        Setting::NotSet => old_shards,
    };

    let merged_remotes = match new_remotes {
        Setting::Set(new_remotes) => {
            let mut merged_remotes = BTreeMap::new();
            for either_or_both in old_remotes
                .into_iter()
                .merge_join_by(new_remotes.into_iter(), |left, right| left.0.cmp(&right.0))
            {
                match either_or_both {
                    EitherOrBoth::Both((key, old), (_, Some(new))) => {
                        let DbRemote {
                            url: old_url,
                            search_api_key: old_search_api_key,
                            write_api_key: old_write_api_key,
                        } = old;

                        let Remote {
                            url: new_url,
                            search_api_key: new_search_api_key,
                            write_api_key: new_write_api_key,
                        } = new;

                        let merged = DbRemote {
                            url: match new_url {
                                Setting::Set(new_url) => {
                                    if let Err(error) = url::Url::parse(&new_url) {
                                        return Err(ResponseError::from_msg(
                                            format!("Invalid `.remotes.{key}.url` (`{new_url}`): {error}"),
                                            meilisearch_types::error::Code::InvalidNetworkUrl,
                                        ));
                                    }
                                    new_url
                                }
                                Setting::Reset => {
                                    return Err(ResponseError::from_msg(
                                        format!(
                                            "Field `.remotes.{key}.url` cannot be set to `null`"
                                        ),
                                        meilisearch_types::error::Code::InvalidNetworkUrl,
                                    ))
                                }
                                Setting::NotSet => old_url,
                            },
                            search_api_key: match new_search_api_key {
                                Setting::Set(new_search_api_key) => Some(new_search_api_key),
                                Setting::Reset => None,
                                Setting::NotSet => old_search_api_key,
                            },
                            write_api_key: match new_write_api_key {
                                Setting::Set(new_write_api_key) => Some(new_write_api_key),
                                Setting::Reset => None,
                                Setting::NotSet => old_write_api_key,
                            },
                        };
                        merged_remotes.insert(key, merged);
                    }
                    EitherOrBoth::Both((removed_remote, _), (_, None))
                    | EitherOrBoth::Right((removed_remote, None)) => {
                        // remove removed remotes from all shards
                        for shard in merged_shards.values_mut() {
                            shard.remotes.remove(&removed_remote);
                        }
                    }
                    EitherOrBoth::Left((key, node)) => {
                        merged_remotes.insert(key, node);
                    }
                    EitherOrBoth::Right((key, Some(node))) => {
                        let node = node.try_into_db_node(&key)?;
                        merged_remotes.insert(key, node);
                    }
                }
            }
            merged_remotes
        }
        Setting::Reset => BTreeMap::new(),
        Setting::NotSet => old_remotes,
    };

    // enforce (3) by removing any shard without remotes
    merged_shards.retain(|_, shard| !shard.remotes.is_empty());

    if let Some(merged_leader) = &merged_leader {
        // (1): the leader is a remote
        if !merged_remotes.contains_key(merged_leader) {
            return Err(ResponseError::from_msg(
                format!("leader `{merged_leader}` is missing from remotes"),
                Code::InvalidNetworkRemotes,
            ));
        }
        // (2): there exists at least one shard
        if merged_shards.is_empty() {
            return Err(ResponseError::from_msg(
                "there must be at least one shard owned by at least one remote".into(),
                Code::InvalidNetworkShards,
            ));
        }
        // (3): any shard has at least one remote
        // enforced above
        // 4. Any remote owning a shard is in the list of remotes.
        for (shard_name, shard) in &merged_shards {
            for remote in &shard.remotes {
                if !merged_remotes.contains_key(remote) {
                    return Err(ResponseError::from_msg(
                        format!("unknown remote `{remote}` in `.{shard_name}.remotes`"),
                        Code::InvalidNetworkShards,
                    ));
                }
            }
        }
    }

    let merged_network = DbNetwork {
        local: merged_self,
        remotes: merged_remotes,
        leader: merged_leader,
        version: new_version,
        shards: merged_shards,
    };
    Ok(merged_network)
}

use std::collections::BTreeMap;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use itertools::{EitherOrBoth, Itertools};
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{
    InvalidNetworkLeader, InvalidNetworkRemotes, InvalidNetworkSearchApiKey, InvalidNetworkSelf,
    InvalidNetworkUrl, InvalidNetworkWriteApiKey,
};
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::network::{Network as DbNetwork, Remote as DbRemote};
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
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/network"),
    )),
)]
pub struct NetworkApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(get_network))
            .route(web::patch().to(SeqHandler(patch_network))),
    );
}

/// Get network topology
///
/// Get a list of all Meilisearch instances currently known to this instance.
#[utoipa::path(
    get,
    path = "",
    tag = "Network",
    security(("Bearer" = ["network.get", "*"])),
    responses(
        (status = OK, description = "Known nodes are returned", body = Network, content_type = "application/json", example = json!(
            {
            "self": "ms-0",
            "remotes": {
                "ms-0": Remote { url: Setting::Set("http://localhost:7700".into()), search_api_key: Setting::Reset, write_api_key: Setting::Reset },
                "ms-1": Remote { url: Setting::Set("http://localhost:7701".into()), search_api_key: Setting::Set("foo".into()), write_api_key: Setting::Set("bar".into()) },
                "ms-2": Remote { url: Setting::Set("http://localhost:7702".into()), search_api_key: Setting::Set("bar".into()), write_api_key: Setting::Set("foo".into()) },
        }
    })),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
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

#[derive(Clone, Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError<InvalidNetworkRemotes>, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct Remote {
    #[schema(value_type = Option<String>, example = "http://localhost:7700")]
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

#[derive(Clone, Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct Network {
    #[schema(value_type = Option<BTreeMap<String, Remote>>, example = json!({
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
    #[schema(value_type = Option<String>, example = json!("ms-00"), rename = "self")]
    #[serde(default, rename = "self")]
    #[deserr(default, rename = "self", error = DeserrJsonError<InvalidNetworkSelf>)]
    pub local: Setting<String>,
    #[schema(value_type = Option<String>, example = json!("ms-00"))]
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkLeader>)]
    pub leader: Setting<String>,
    #[schema(value_type = Option<BTreeMap<String, Remote>>, example = json!({
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

/// Configure Network
///
/// Add or remove nodes from network.
#[utoipa::path(
    patch,
    path = "",
    tag = "Network",
    request_body = Network,
    security(("Bearer" = ["network.update", "*"])),
    responses(
        (status = OK, description = "New network state is returned",  body = Network, content_type = "application/json", example = json!(
            {
                "self": "ms-0",
                "remotes": {
                    "ms-0": Remote { url: Setting::Set("http://localhost:7700".into()), search_api_key: Setting::Reset, write_api_key: Setting::Reset },
                    "ms-1": Remote { url: Setting::Set("http://localhost:7701".into()), search_api_key: Setting::Set("foo".into()), write_api_key: Setting::Set("bar".into()) },
                    "ms-2": Remote { url: Setting::Set("http://localhost:7702".into()), search_api_key: Setting::Set("bar".into()), write_api_key: Setting::Set("foo".into()) },
            }
        })),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
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

fn merge_networks(
    old_network: DbNetwork,
    new_network: Network,
) -> Result<DbNetwork, ResponseError> {
    let merged_self = match new_network.local {
        Setting::Set(new_self) => Some(new_self),
        Setting::Reset => None,
        Setting::NotSet => old_network.local,
    };
    let merged_leader = match new_network.leader {
        Setting::Set(new_leader) => Some(new_leader),
        Setting::Reset => None,
        Setting::NotSet => old_network.leader,
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
    let merged_remotes = match new_network.remotes {
        Setting::Set(new_remotes) => {
            let mut merged_remotes = BTreeMap::new();
            for either_or_both in old_network
                .remotes
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
                    EitherOrBoth::Both((_, _), (_, None)) | EitherOrBoth::Right((_, None)) => {}
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
        Setting::NotSet => old_network.remotes,
    };
    let merged_network = DbNetwork {
        local: merged_self,
        remotes: merged_remotes,
        leader: merged_leader,
        version: new_version,
    };
    Ok(merged_network)
}

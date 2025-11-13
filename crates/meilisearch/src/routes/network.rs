use std::collections::BTreeMap;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use itertools::{EitherOrBoth, Itertools};
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::enterprise_edition::network::{Network as DbNetwork, Remote as DbRemote};
use meilisearch_types::error::deserr_codes::{
    InvalidNetworkLeader, InvalidNetworkRemotes, InvalidNetworkSearchApiKey, InvalidNetworkSelf,
    InvalidNetworkUrl, InvalidNetworkWriteApiKey,
};
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::tasks::enterprise_edition::network::{
    NetworkTopologyChange, Origin, TaskNetwork,
};
use meilisearch_types::tasks::KindWithContent;
use serde::Serialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use crate::analytics::{Aggregate, Analytics};
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::indexes::enterprise_edition::proxy::{proxy, Body};
use crate::routes::SummarizedTaskView;

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

    match crate::routes::indexes::enterprise_edition::proxy::origin_from_req(&req)? {
        Some(origin) => {
            patch_network_with_origin(index_scheduler, new_network, req, origin, analytics).await
        }
        None => patch_network_without_origin(index_scheduler, new_network, req, analytics).await,
    }
}

async fn patch_network_without_origin(
    index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_UPDATE }>, Data<IndexScheduler>>,
    new_network: AwebJson<Network, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let new_network = new_network.0;
    let old_network = index_scheduler.network();
    debug!(parameters = ?new_network, "Patch network");

    if !matches!(new_network.previous_remotes, Setting::NotSet) {
        return Err(MeilisearchHttpError::UnexpectedNetworkPreviousRemotes.into());
    }

    let merged_network = merge_networks(old_network.clone(), new_network)?;
    index_scheduler.put_network(merged_network.clone())?;

    analytics.publish(
        PatchNetworkAnalytics {
            network_size: merged_network.remotes.len(),
            network_has_self: merged_network.local.is_some(),
        },
        &req,
    );

    /// TODO: spawn task only if necessary
    let network_topology_change =
        NetworkTopologyChange::new(old_network.clone(), merged_network.clone());
    let task = KindWithContent::NetworkTopologyChange(network_topology_change);
    let task = {
        let index_scheduler = index_scheduler.clone();
        tokio::task::spawn_blocking(move || index_scheduler.register(task, None, false)).await??
    };

    let mut proxied_network = Network {
        remotes: Setting::Set(to_settings_remotes(&merged_network.remotes)),
        local: Setting::NotSet,
        leader: Setting::some_or_not_set(merged_network.leader.clone()),
        previous_remotes: Setting::Set(to_settings_remotes(&old_network.remotes)),
    };
    let mut deleted_network = old_network;

    let deleted_remotes = &mut deleted_network.remotes;
    deleted_remotes.retain(|node, _| !merged_network.remotes.contains_key(node));

    // proxy network change to the remaining remotes.
    let updated_task = proxy(
        &index_scheduler,
        None,
        &req,
        TaskNetwork::Remotes {
            remote_tasks: Default::default(),
            network_version: merged_network.version,
        },
        merged_network,
        Body::generated(proxied_network.clone(), |name, _remote, network| {
            network.local = Setting::Set(name.to_string());
        }),
        &task,
    )
    .await?;
    // unwrap: network was set by `proxy`
    let task_network = updated_task.network.unwrap();

    proxied_network.previous_remotes = Setting::NotSet;

    // proxy network change to the deleted remotes
    proxy(
        &index_scheduler,
        None,
        &req,
        task_network,
        deleted_network,
        Body::generated(proxied_network.clone(), |_name, _remote, network| {
            network.local = Setting::Reset;
        }),
        &task,
    )
    .await?;

    let task: SummarizedTaskView = task.into();
    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

async fn patch_network_with_origin(
    index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_UPDATE }>, Data<IndexScheduler>>,
    merged_network: AwebJson<Network, DeserrJsonError>,
    req: HttpRequest,
    origin: Origin,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let merged_network = merged_network.into_inner();
    debug!(parameters = ?merged_network, ?origin, "Patch network");
    let mut remotes = BTreeMap::new();
    let mut old_network = index_scheduler.network();

    for (name, remote) in merged_network.remotes.set().into_iter().flat_map(|x| x.into_iter()) {
        let Some(remote) = remote else { continue };
        let remote = remote.try_into_db_node(&name)?;
        remotes.insert(name, remote);
    }
    let mut previous_remotes = BTreeMap::new();
    for (name, remote) in
        merged_network.previous_remotes.set().into_iter().flat_map(|x| x.into_iter())
    {
        let Some(remote) = remote else {
            continue;
        };
        let remote = remote.try_into_db_node(&name)?;
        previous_remotes.insert(name, remote);
    }

    old_network.remotes = previous_remotes;

    let new_network = DbNetwork {
        local: merged_network.local.set(),
        remotes,
        leader: merged_network.leader.set(),
        version: origin.network_version,
    };
    index_scheduler.put_network(new_network.clone())?;

    analytics.publish(
        PatchNetworkAnalytics {
            network_size: new_network.remotes.len(),
            network_has_self: new_network.local.is_some(),
        },
        &req,
    );

    /// TODO: spawn task only if necessary
    let network_topology_change = NetworkTopologyChange::new(old_network, new_network);
    let task = KindWithContent::NetworkTopologyChange(network_topology_change);
    let task = {
        let index_scheduler = index_scheduler.clone();
        tokio::task::spawn_blocking(move || index_scheduler.register(task, None, false)).await??
    };

    index_scheduler.set_task_network(task.uid, TaskNetwork::Origin { origin })?;

    let task: SummarizedTaskView = task.into();
    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

fn to_settings_remotes(
    db_remotes: &BTreeMap<String, DbRemote>,
) -> BTreeMap<String, Option<Remote>> {
    db_remotes
        .iter()
        .map(|(name, remote)| {
            (
                name.clone(),
                Some(Remote {
                    url: Setting::Set(remote.url.clone()),
                    search_api_key: Setting::some_or_not_set(remote.search_api_key.clone()),
                    write_api_key: Setting::some_or_not_set(remote.write_api_key.clone()),
                }),
            )
        })
        .collect()
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

use std::collections::BTreeMap;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use itertools::{EitherOrBoth, Itertools};
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{
    InvalidNetworkRemotes, InvalidNetworkSearchApiKey, InvalidNetworkSelf, InvalidNetworkUrl,
};
use meilisearch_types::error::ResponseError;
use meilisearch_types::features::{Network as DbNetwork, Remote as DbRemote};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use serde::Serialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

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
    security(("Bearer" = ["network.get", "network.*", "*"])),
    responses(
        (status = OK, description = "Known nodes are returned", body = Network, content_type = "application/json", example = json!(
            {
            "self": "ms-0",
            "remotes": {
            "ms-0": Remote { url: Setting::Set("http://localhost:7700".into()), search_api_key: Setting::Reset },
            "ms-1": Remote { url: Setting::Set("http://localhost:7701".into()), search_api_key: Setting::Set("foo".into()) },
            "ms-2": Remote { url: Setting::Set("http://localhost:7702".into()), search_api_key: Setting::Set("bar".into()) },
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

#[derive(Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError<InvalidNetworkRemotes>, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct Remote {
    #[schema(value_type = Option<String>, example = json!({
        "ms-0": Remote { url: Setting::Set("http://localhost:7700".into()), search_api_key: Setting::Reset },
        "ms-1": Remote { url: Setting::Set("http://localhost:7701".into()), search_api_key: Setting::Set("foo".into()) },
        "ms-2": Remote { url: Setting::Set("http://localhost:7702".into()), search_api_key: Setting::Set("bar".into()) },
    }))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkUrl>)]
    #[serde(default)]
    pub url: Setting<String>,
    #[schema(value_type = Option<String>, example = json!("XWnBI8QHUc-4IlqbKPLUDuhftNq19mQtjc6JvmivzJU"))]
    #[deserr(default, error = DeserrJsonError<InvalidNetworkSearchApiKey>)]
    #[serde(default)]
    pub search_api_key: Setting<String>,
}

#[derive(Debug, Deserr, ToSchema, Serialize)]
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
}

impl Remote {
    pub fn try_into_db_node(self, name: &str) -> Result<DbRemote, ResponseError> {
        Ok(DbRemote {
            url: self.url.set().ok_or(ResponseError::from_msg(
                format!("Missing field `.remotes.{name}.url`"),
                meilisearch_types::error::Code::MissingNetworkUrl,
            ))?,
            search_api_key: self.search_api_key.set(),
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
    security(("Bearer" = ["network.update", "network.*", "*"])),
    responses(
        (status = OK, description = "New network state is returned",  body = Network, content_type = "application/json", example = json!(
            {
                "self": "ms-0",
                "remotes": {
                "ms-0": Remote { url: Setting::Set("http://localhost:7700".into()), search_api_key: Setting::Reset },
                "ms-1": Remote { url: Setting::Set("http://localhost:7701".into()), search_api_key: Setting::Set("foo".into()) },
                "ms-2": Remote { url: Setting::Set("http://localhost:7702".into()), search_api_key: Setting::Set("bar".into()) },
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

    let new_network = new_network.0;
    let old_network = index_scheduler.network();
    debug!(parameters = ?new_network, "Patch network");

    let merged_self = match new_network.local {
        Setting::Set(new_self) => Some(new_self),
        Setting::Reset => None,
        Setting::NotSet => old_network.local,
    };

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
                        let DbRemote { url: old_url, search_api_key: old_search_api_key } = old;

                        let Remote { url: new_url, search_api_key: new_search_api_key } = new;

                        let merged = DbRemote {
                            url: match new_url {
                                Setting::Set(new_url) => new_url,
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

    analytics.publish(
        PatchNetworkAnalytics {
            network_size: merged_remotes.len(),
            network_has_self: merged_self.is_some(),
        },
        &req,
    );

    let merged_network = DbNetwork { local: merged_self, remotes: merged_remotes };
    index_scheduler.put_network(merged_network.clone())?;
    debug!(returns = ?merged_network, "Patch network");
    Ok(HttpResponse::Ok().json(merged_network))
}

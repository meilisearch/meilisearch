use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::enterprise_edition::network::{Network, Remote};
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use serde::Serialize;
use tracing::debug;
use utoipa::OpenApi;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::SummarizedTaskView;

#[derive(OpenApi)]
#[openapi(
    paths(get_network, patch_network),
    tags((
        name = "Network",
        description = "The `/network` route allows you to describe the topology of a network of Meilisearch instances.

This route is **asynchronous**. A task uid will be returned, and any change to the network will be effective after the corresponding task has been processed.",
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

    let new_network = new_network.0;
    debug!(parameters = ?new_network, "Patch network");

    // check the URLs of all remotes
    if let Setting::Set(remotes) = &new_network.remotes {
        for (remote_name, remote) in remotes.iter() {
            let Some(remote) = remote else {
                continue;
            };
            match &remote.url {
                Setting::Set(new_url) => {
                    if let Err(error) = url::Url::parse(&new_url) {
                        return Err(ResponseError::from_msg(
                            format!("Invalid `.remotes.{remote_name}.url` (`{new_url}`): {error}"),
                            meilisearch_types::error::Code::InvalidNetworkUrl,
                        ));
                    }
                }
                Setting::Reset => {
                    return Err(ResponseError::from_msg(
                        format!("Field `.remotes.{remote_name}.url` cannot be set to `null`"),
                        meilisearch_types::error::Code::InvalidNetworkUrl,
                    ))
                }
                Setting::NotSet => (),
            }
        }
    }

    analytics.publish(
        PatchNetworkAnalytics {
            network_size: new_network
                .remotes
                .as_ref()
                .set()
                .map(|remotes| remotes.len())
                .unwrap_or_default(),
            network_has_self: new_network.local.as_ref().set().is_some(),
        },
        &req,
    );

    let task = index_scheduler.register(
        meilisearch_types::tasks::KindWithContent::NetworkTopologyChange {
            network: Some(new_network),
            origin: None,
        },
        None,
        false,
    )?;
    debug!(returns = ?task, "Patch network");

    let task: SummarizedTaskView = task.into();

    return Ok(HttpResponse::Accepted().json(task));
}

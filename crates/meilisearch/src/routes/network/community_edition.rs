use actix_web::web::Data;
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::network::route;
use tracing::debug;

use super::{merge_networks, Network, PatchNetworkAnalytics};
use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;

pub async fn patch_network(
    index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_UPDATE }>, Data<IndexScheduler>>,
    new_network: AwebJson<Network, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let new_network = new_network.0;
    let old_network = index_scheduler.network();
    debug!(parameters = ?new_network, "Patch network");

    if new_network.leader.as_ref().set().is_some() {
        use meilisearch_types::error::Code;

        return Err(ResponseError::from_msg(
            "Meilisearch Enterprise Edition is required to set `network.leader`".into(),
            Code::RequiresEnterpriseEdition,
        ));
    }

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

    Ok(HttpResponse::Ok().json(merged_network))
}

pub async fn post_network_change(
    _index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_UPDATE }>, Data<IndexScheduler>>,
    _payload: route::NetworkChange,
) -> Result<HttpResponse, ResponseError> {
    Err(ResponseError::from_msg(
        "Meilisearch Enterprise Edition is required to call this route".into(),
        Code::RequiresEnterpriseEdition,
    ))
}

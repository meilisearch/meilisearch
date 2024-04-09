use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::actions;
use tracing::debug;

use super::ActionPolicy;
use crate::analytics::Analytics;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::search::{perform_recommend, RecommendQuery, SearchKind};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(recommend))));
}

pub async fn recommend(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebJson<RecommendQuery, DeserrJsonError>,
    _req: HttpRequest,
    _analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    // TODO analytics

    let query = params.into_inner();
    debug!(parameters = ?query, "Recommend post");

    let index = index_scheduler.index(&index_uid)?;

    let features = index_scheduler.features();

    features.check_vector("Using the recommend API.")?;

    let (embedder_name, embedder) =
        SearchKind::embedder(&index_scheduler, &index, query.embedder.as_deref(), None)?;

    let recommendations = tokio::task::spawn_blocking(move || {
        perform_recommend(&index, query, embedder_name, embedder)
    })
    .await?;

    let recommendations = recommendations?;

    debug!(returns = ?recommendations, "Recommend post");
    Ok(HttpResponse::Ok().json(recommendations))
}

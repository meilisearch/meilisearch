use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{ErrorCode as _, ResponseError};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::actions;
use meilisearch_types::serde_cs::vec::CS;
use serde_json::Value;
use tracing::debug;

use super::ActionPolicy;
use crate::analytics::Analytics;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::indexes::similar_analytics::{SimilarAggregator, SimilarGET, SimilarPOST};
use crate::search::{
    add_search_rules, perform_similar, RankingScoreThresholdSimilar, RetrieveVectors, SearchKind,
    SimilarQuery, SimilarResult, DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET,
};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(similar_get)))
            .route(web::post().to(SeqHandler(similar_post))),
    );
}

pub async fn similar_get(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<SimilarQueryGet, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let query = params.0.try_into()?;

    let mut aggregate = SimilarAggregator::<SimilarGET>::from_query(&query);

    debug!(parameters = ?query, "Similar get");

    let similar = similar(index_scheduler, index_uid, query).await;

    if let Ok(similar) = &similar {
        aggregate.succeed(similar);
    }
    analytics.publish(aggregate, &req);

    let similar = similar?;

    debug!(returns = ?similar, "Similar get");
    Ok(HttpResponse::Ok().json(similar))
}

pub async fn similar_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebJson<SimilarQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let query = params.into_inner();
    debug!(parameters = ?query, "Similar post");

    let mut aggregate = SimilarAggregator::<SimilarPOST>::from_query(&query);

    let similar = similar(index_scheduler, index_uid, query).await;

    if let Ok(similar) = &similar {
        aggregate.succeed(similar);
    }
    analytics.publish(aggregate, &req);

    let similar = similar?;

    debug!(returns = ?similar, "Similar post");
    Ok(HttpResponse::Ok().json(similar))
}

async fn similar(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    index_uid: IndexUid,
    mut query: SimilarQuery,
) -> Result<SimilarResult, ResponseError> {
    let features = index_scheduler.features();

    features.check_vector("Using the similar API")?;

    let retrieve_vectors = RetrieveVectors::new(query.retrieve_vectors, features)?;

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut query.filter, search_rules);
    }

    let index = index_scheduler.index(&index_uid)?;

    let (embedder_name, embedder, quantized) = SearchKind::embedder(
        &index_scheduler,
        index_uid.to_string(),
        &index,
        &query.embedder,
        None,
    )?;

    tokio::task::spawn_blocking(move || {
        perform_similar(
            &index,
            query,
            embedder_name,
            embedder,
            quantized,
            retrieve_vectors,
            index_scheduler.features(),
        )
    })
    .await?
}

#[derive(Debug, deserr::Deserr)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
pub struct SimilarQueryGet {
    #[deserr(error = DeserrQueryParamError<InvalidSimilarId>)]
    id: Param<String>,
    #[deserr(default = Param(DEFAULT_SEARCH_OFFSET()), error = DeserrQueryParamError<InvalidSimilarOffset>)]
    offset: Param<usize>,
    #[deserr(default = Param(DEFAULT_SEARCH_LIMIT()), error = DeserrQueryParamError<InvalidSimilarLimit>)]
    limit: Param<usize>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSimilarAttributesToRetrieve>)]
    attributes_to_retrieve: Option<CS<String>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSimilarRetrieveVectors>)]
    retrieve_vectors: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSimilarFilter>)]
    filter: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSimilarShowRankingScore>)]
    show_ranking_score: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSimilarShowRankingScoreDetails>)]
    show_ranking_score_details: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSimilarRankingScoreThreshold>, default)]
    pub ranking_score_threshold: Option<RankingScoreThresholdGet>,
    #[deserr(error = DeserrQueryParamError<InvalidEmbedder>)]
    pub embedder: String,
}

#[derive(Debug, Clone, Copy, PartialEq, deserr::Deserr)]
#[deserr(try_from(String) = TryFrom::try_from -> InvalidSimilarRankingScoreThreshold)]
pub struct RankingScoreThresholdGet(RankingScoreThresholdSimilar);

impl std::convert::TryFrom<String> for RankingScoreThresholdGet {
    type Error = InvalidSimilarRankingScoreThreshold;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let f: f64 = s.parse().map_err(|_| InvalidSimilarRankingScoreThreshold)?;
        Ok(RankingScoreThresholdGet(RankingScoreThresholdSimilar::try_from(f)?))
    }
}

impl TryFrom<SimilarQueryGet> for SimilarQuery {
    type Error = ResponseError;

    fn try_from(
        SimilarQueryGet {
            id,
            offset,
            limit,
            attributes_to_retrieve,
            retrieve_vectors,
            filter,
            show_ranking_score,
            show_ranking_score_details,
            embedder,
            ranking_score_threshold,
        }: SimilarQueryGet,
    ) -> Result<Self, Self::Error> {
        let filter = match filter {
            Some(f) => match serde_json::from_str(&f) {
                Ok(v) => Some(v),
                _ => Some(Value::String(f)),
            },
            None => None,
        };

        Ok(SimilarQuery {
            id: id.0.try_into().map_err(|code: InvalidSimilarId| {
                ResponseError::from_msg(code.to_string(), code.error_code())
            })?,
            offset: offset.0,
            limit: limit.0,
            filter,
            embedder,
            attributes_to_retrieve: attributes_to_retrieve.map(|o| o.into_iter().collect()),
            retrieve_vectors: retrieve_vectors.0,
            show_ranking_score: show_ranking_score.0,
            show_ranking_score_details: show_ranking_score_details.0,
            ranking_score_threshold: ranking_score_threshold.map(|x| x.0),
        })
    }
}

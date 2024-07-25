use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::locales::Locale;
use serde_json::Value;
use tracing::debug;

use crate::analytics::{Analytics, FacetSearchAggregator};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::routes::indexes::search::search_kind;
use crate::search::{
    add_search_rules, perform_facet_search, HybridQuery, MatchingStrategy, RankingScoreThreshold,
    SearchQuery, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER, DEFAULT_HIGHLIGHT_POST_TAG,
    DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET,
};
use crate::search_queue::SearchQueue;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(search)));
}

/// # Important
///
/// Intentionally don't use `deny_unknown_fields` to ignore search parameters sent by user
#[derive(Debug, Clone, Default, PartialEq, deserr::Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase)]
pub struct FacetSearchQuery {
    #[deserr(default, error = DeserrJsonError<InvalidFacetSearchQuery>)]
    pub facet_query: Option<String>,
    #[deserr(error = DeserrJsonError<InvalidFacetSearchFacetName>, missing_field_error = DeserrJsonError::missing_facet_search_facet_name)]
    pub facet_name: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchVector>)]
    pub vector: Option<Vec<f32>>,
    #[deserr(default, error = DeserrJsonError<InvalidHybridQuery>)]
    pub hybrid: Option<HybridQuery>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>, default)]
    pub matching_strategy: MatchingStrategy,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToSearchOn>, default)]
    pub attributes_to_search_on: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchRankingScoreThreshold>, default)]
    pub ranking_score_threshold: Option<RankingScoreThreshold>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchLocales>, default)]
    pub locales: Option<Vec<Locale>>,
}

pub async fn search(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: Data<SearchQueue>,
    index_uid: web::Path<String>,
    params: AwebJson<FacetSearchQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let query = params.into_inner();
    debug!(parameters = ?query, "Facet search");

    let mut aggregate = FacetSearchAggregator::from_query(&query, &req);

    let facet_query = query.facet_query.clone();
    let facet_name = query.facet_name.clone();
    let locales = query.locales.clone().map(|l| l.into_iter().map(Into::into).collect());
    let mut search_query = SearchQuery::from(query);

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut search_query.filter, search_rules);
    }

    let index = index_scheduler.index(&index_uid)?;
    let features = index_scheduler.features();
    let search_kind = search_kind(&search_query, &index_scheduler, &index, features)?;
    let _permit = search_queue.try_get_search_permit().await?;
    let search_result = tokio::task::spawn_blocking(move || {
        perform_facet_search(
            &index,
            search_query,
            facet_query,
            facet_name,
            search_kind,
            index_scheduler.features(),
            locales,
        )
    })
    .await?;

    if let Ok(ref search_result) = search_result {
        aggregate.succeed(search_result);
    }
    analytics.post_facet_search(aggregate);

    let search_result = search_result?;

    debug!(returns = ?search_result, "Facet search");
    Ok(HttpResponse::Ok().json(search_result))
}

impl From<FacetSearchQuery> for SearchQuery {
    fn from(value: FacetSearchQuery) -> Self {
        let FacetSearchQuery {
            facet_query: _,
            facet_name: _,
            q,
            vector,
            filter,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
        } = value;

        SearchQuery {
            q,
            offset: DEFAULT_SEARCH_OFFSET(),
            limit: DEFAULT_SEARCH_LIMIT(),
            page: None,
            hits_per_page: None,
            attributes_to_retrieve: None,
            retrieve_vectors: false,
            attributes_to_crop: None,
            crop_length: DEFAULT_CROP_LENGTH(),
            attributes_to_highlight: None,
            show_matches_position: false,
            show_ranking_score: false,
            show_ranking_score_details: false,
            filter,
            sort: None,
            distinct: None,
            facets: None,
            highlight_pre_tag: DEFAULT_HIGHLIGHT_PRE_TAG(),
            highlight_post_tag: DEFAULT_HIGHLIGHT_POST_TAG(),
            crop_marker: DEFAULT_CROP_MARKER(),
            matching_strategy,
            vector,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
        }
    }
}

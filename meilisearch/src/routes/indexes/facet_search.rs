use std::collections::{BTreeSet, HashSet};

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use log::debug;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use serde_json::Value;

use crate::analytics::{Analytics, FacetSearchAggregator};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::search::{
    add_search_rules, perform_facet_search, MatchingStrategy, SearchQuery, DEFAULT_CROP_LENGTH,
    DEFAULT_CROP_MARKER, DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG,
    DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET,
};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(search)));
}

// TODO improve the error messages
#[derive(Debug, Clone, Default, PartialEq, deserr::Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct FacetSearchQuery {
    #[deserr(default, error = DeserrJsonError<InvalidFacetSearchQuery>)]
    pub facet_query: Option<String>,
    #[deserr(error = DeserrJsonError<InvalidFacetSearchName>)]
    pub facet_name: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchVector>)]
    pub vector: Option<Vec<f32>>,
    #[deserr(default = DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSearchOffset>)]
    pub offset: usize,
    #[deserr(default = DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSearchLimit>)]
    pub limit: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchPage>)]
    pub page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHitsPerPage>)]
    pub hits_per_page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToCrop>)]
    pub attributes_to_crop: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropLength>, default = DEFAULT_CROP_LENGTH())]
    pub crop_length: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToHighlight>)]
    pub attributes_to_highlight: Option<HashSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowMatchesPosition>, default)]
    pub show_matches_position: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScore>, default)]
    pub show_ranking_score: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScoreDetails>, default)]
    pub show_ranking_score_details: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchSort>)]
    pub sort: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFacets>)]
    pub facets: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPreTag>, default = DEFAULT_HIGHLIGHT_PRE_TAG())]
    pub highlight_pre_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPostTag>, default = DEFAULT_HIGHLIGHT_POST_TAG())]
    pub highlight_post_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropMarker>, default = DEFAULT_CROP_MARKER())]
    pub crop_marker: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>, default)]
    pub matching_strategy: MatchingStrategy,
}

pub async fn search(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebJson<FacetSearchQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let query = params.into_inner();
    debug!("facet search called with params: {:?}", query);

    let mut aggregate = FacetSearchAggregator::from_query(&query, &req);

    let facet_query = query.facet_query.clone();
    let facet_name = query.facet_name.clone();
    let mut search_query = SearchQuery::from(query);

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut search_query, search_rules);
    }

    let index = index_scheduler.index(&index_uid)?;
    let features = index_scheduler.features()?;
    let search_result = tokio::task::spawn_blocking(move || {
        perform_facet_search(&index, search_query, facet_query, facet_name, features)
    })
    .await?;

    if let Ok(ref search_result) = search_result {
        aggregate.succeed(search_result);
    }
    analytics.post_facet_search(aggregate);

    let search_result = search_result?;

    debug!("returns: {:?}", search_result);
    Ok(HttpResponse::Ok().json(search_result))
}

impl From<FacetSearchQuery> for SearchQuery {
    fn from(value: FacetSearchQuery) -> Self {
        SearchQuery {
            q: value.q,
            offset: value.offset,
            limit: value.limit,
            page: value.page,
            hits_per_page: value.hits_per_page,
            attributes_to_retrieve: value.attributes_to_retrieve,
            attributes_to_crop: value.attributes_to_crop,
            crop_length: value.crop_length,
            attributes_to_highlight: value.attributes_to_highlight,
            show_matches_position: value.show_matches_position,
            show_ranking_score: value.show_ranking_score,
            show_ranking_score_details: value.show_ranking_score_details,
            filter: value.filter,
            sort: value.sort,
            facets: value.facets,
            highlight_pre_tag: value.highlight_pre_tag,
            highlight_post_tag: value.highlight_post_tag,
            crop_marker: value.crop_marker,
            matching_strategy: value.matching_strategy,
            vector: value.vector,
        }
    }
}

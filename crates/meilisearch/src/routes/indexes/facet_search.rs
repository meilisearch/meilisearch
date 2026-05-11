use std::collections::{BTreeMap, BinaryHeap, HashSet, VecDeque};

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::filter::filter_into_index_filter;
use index_scheduler::{IndexScheduler, RoFeatures};
use itertools::Itertools as _;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::locales::Locale;
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::{FacetValueHit, OrderBy};
use meilisearch_types::network::Network;
use serde::Serialize;
use serde_json::Value;
use tracing::debug;
use utoipa::ToSchema;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::routes::indexes::search::search_kind;
use crate::search::proxy::{json_proxy, ProxySearchError, ProxySearchParams};
use crate::search::{
    add_search_rules, parse_filter, perform_facet_search, prepare_search, FacetSearchResult,
    HybridQuery, MatchingStrategy, NetworkableQuery, Partition, RankingScoreThreshold, SearchQuery,
    SearchResult, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER, DEFAULT_HIGHLIGHT_POST_TAG,
    DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET,
};
use crate::search_queue::SearchQueue;

#[routes::routes(
    routes(""=>post(search)),
    tag = "Facet Search",
    tags(
        (
            name = "Facet Search",
            description = "The `/facet-search` route allows you to search for facet values. Facet search supports prefix search and typo tolerance. The returned hits are sorted lexicographically in ascending order. You can configure how facets are sorted using the sortFacetValuesBy property of the faceting index settings.",
        ),
    ),
)]
pub struct FacetSearchApi;

// # Important
//
// Intentionally don't use `deny_unknown_fields` to ignore search parameters sent by user
/// Request body for searching facet values
#[derive(Debug, Clone, Default, PartialEq, deserr::Deserr, Serialize, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase)]
#[serde(rename_all = "camelCase")]
pub struct FacetSearchQuery {
    /// Query string to search for facet values
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidFacetSearchQuery>)]
    pub facet_query: Option<String>,
    /// Name of the facet to search
    #[schema(required = true)]
    #[deserr(error = DeserrJsonError<InvalidFacetSearchFacetName>, missing_field_error = DeserrJsonError::missing_facet_search_facet_name)]
    pub facet_name: String,
    /// Query string to filter documents before facet search
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    /// Custom query vector for semantic search
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchVector>)]
    pub vector: Option<Vec<f32>>,
    /// Multimodal content for AI-powered search
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchMedia>)]
    pub media: Option<Value>,
    /// Hybrid search configuration that combines keyword search with semantic
    /// (vector) search. Set `semanticRatio` to balance between keyword
    /// matching (0.0) and semantic similarity (1.0). Requires an embedder to
    /// be configured in the index settings.
    #[deserr(default, error = DeserrJsonError<InvalidSearchHybridQuery>)]
    #[schema(required = false, value_type = Option<HybridQuery>)]
    pub hybrid: Option<HybridQuery>,
    /// Filter expression to apply before facet search
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    /// Strategy used to match query terms
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>, default)]
    pub matching_strategy: MatchingStrategy,
    /// Restrict search to specified attributes
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToSearchOn>, default)]
    pub attributes_to_search_on: Option<Vec<String>>,
    /// Minimum ranking score threshold (0.0 to 1.0) that documents must
    /// achieve to be considered when computing facet counts. Documents with
    /// scores below this threshold are excluded from facet value counts.
    #[deserr(default, error = DeserrJsonError<InvalidSearchRankingScoreThreshold>, default)]
    #[schema(required = false, value_type = Option<f64>)]
    pub ranking_score_threshold: Option<RankingScoreThreshold>,
    /// Languages to use for query processing
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchLocales>, default)]
    pub locales: Option<Vec<Locale>>,
    /// Return exhaustive facet count instead of an estimate
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidFacetSearchExhaustiveFacetCount>, default)]
    pub exhaustive_facet_count: Option<bool>,
    /// When `true`, runs the query on the whole network (all shards covered exactly once).
    ///
    /// When `false`, the query runs locally.
    ///
    /// When omitted or `null`, the default value depends on whether the sharding is enabled for the instance:
    ///
    /// - If the instance has sharding enabled (has a leader), defaults to `true`.
    /// - Otherwise defaults to `false`.
    ///
    /// **Enterprise Edition only.** This feature is available in the Enterprise Edition.
    ///
    /// It also requires the `network` [experimental feature](http://localhost:3000/reference/api/experimental-features/configure-experimental-features).
    ///
    /// Values: `true` = use the whole network; `false` = local, default = see above.
    ///
    /// When using the network, the index must exist with compatible settings on all remotes.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchUseNetwork>)]
    pub use_network: Option<bool>,
}

impl NetworkableQuery for FacetSearchQuery {
    fn use_network_field(&mut self) -> &mut Option<bool> {
        &mut self.use_network
    }

    fn has_remote(&self) -> bool {
        false
    }
}

#[derive(Default)]
pub struct FacetSearchAggregator {
    // requests
    total_received: usize,
    total_succeeded: usize,
    time_spent: BinaryHeap<usize>,

    // The set of all facetNames that were used
    facet_names: HashSet<String>,

    // As there been any other parameter than the facetName or facetQuery ones?
    additional_search_parameters_provided: bool,
}

impl FacetSearchAggregator {
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_query(query: &FacetSearchQuery) -> Self {
        let FacetSearchQuery {
            facet_query: _,
            facet_name,
            vector,
            q,
            media,
            filter,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
            exhaustive_facet_count,
            use_network,
        } = query;

        Self {
            total_received: 1,
            facet_names: Some(facet_name.clone()).into_iter().collect(),
            additional_search_parameters_provided: q.is_some()
                || vector.is_some()
                || media.is_some()
                || filter.is_some()
                || *matching_strategy != MatchingStrategy::default()
                || attributes_to_search_on.is_some()
                || hybrid.is_some()
                || ranking_score_threshold.is_some()
                || locales.is_some()
                || exhaustive_facet_count.is_some()
                || use_network.is_some(),
            ..Default::default()
        }
    }

    pub fn succeed(&mut self, result: &FacetSearchResult) {
        let FacetSearchResult {
            facet_hits: _,
            facet_query: _,
            processing_time_ms,
            remote_errors: _,
        } = result;
        self.total_succeeded = 1;
        self.time_spent.push(*processing_time_ms as usize);
    }
}

impl Aggregate for FacetSearchAggregator {
    fn event_name(&self) -> &'static str {
        "Facet Searched POST"
    }

    fn aggregate(mut self: Box<Self>, new: Box<Self>) -> Box<Self> {
        for time in new.time_spent {
            self.time_spent.push(time);
        }

        Box::new(Self {
            total_received: self.total_received.saturating_add(new.total_received),
            total_succeeded: self.total_succeeded.saturating_add(new.total_succeeded),
            time_spent: self.time_spent,
            facet_names: self.facet_names.union(&new.facet_names).cloned().collect(),
            additional_search_parameters_provided: self.additional_search_parameters_provided
                | new.additional_search_parameters_provided,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        let Self {
            total_received,
            total_succeeded,
            time_spent,
            facet_names,
            additional_search_parameters_provided,
        } = *self;
        // the index of the 99th percentage of value
        let percentile_99th = 0.99 * (total_succeeded as f64 - 1.) + 1.;
        // we get all the values in a sorted manner
        let time_spent = time_spent.into_sorted_vec();
        // We are only interested by the slowest value of the 99th fastest results
        let time_spent = time_spent.get(percentile_99th as usize);

        serde_json::json!({
            "requests": {
                "99th_response_time":  time_spent.map(|t| format!("{:.2}", t)),
                "total_succeeded": total_succeeded,
                "total_failed": total_received.saturating_sub(total_succeeded), // just to be sure we never panics
                "total_received": total_received,
            },
            "facets": {
                "total_distinct_facet_count": facet_names.len(),
                "additional_search_parameters_provided": additional_search_parameters_provided,
            },
        })
    }
}

/// Search in facets
///
/// Search for facet values within a given facet.
///
/// > Use this to build autocomplete or refinement UIs for facet filters.
#[routes::path(
    security(("Bearer" = ["search", "*"])),
    params(("index_uid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false)),
    request_body = FacetSearchQuery,
    responses(
        (status = 200, description = "The documents are returned.", body = SearchResult, content_type = "application/json", example = json!(
            {
              "hits": [
                {
                  "id": 2770,
                  "title": "American Pie 2",
                  "poster": "https://image.tmdb.org/t/p/w1280/q4LNgUnRfltxzp3gf1MAGiK5LhV.jpg",
                  "overview": "The whole gang are back and as close as ever. They decide to get even closer by spending the summer together at a beach house. They decide to hold the biggest…",
                  "release_date": 997405200
                },
                {
                  "id": 190859,
                  "title": "American Sniper",
                  "poster": "https://image.tmdb.org/t/p/w1280/svPHnYE7N5NAGO49dBmRhq0vDQ3.jpg",
                  "overview": "U.S. Navy SEAL Chris Kyle takes his sole mission—protect his comrades—to heart and becomes one of the most lethal snipers in American history. His pinpoint accuracy not only saves countless lives but also makes him a prime…",
                  "release_date": 1418256000
                }
              ],
              "offset": 0,
              "limit": 2,
              "estimatedTotalHits": 976,
              "processingTimeMs": 35,
              "query": "american "
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
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
pub async fn search(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: Data<SearchQueue>,
    index_uid: web::Path<String>,
    params: AwebJson<FacetSearchQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let progress = Progress::default();
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let permit = search_queue.try_get_search_permit().await?;

    let mut query = params.into_inner();
    debug!(parameters = ?query, "Facet search");

    // Tenant token search_rules.
    // NOTE: must be applied **BEFORE** proxying the query so that the tenant token sent to the original machine is taken into account
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut query.filter, search_rules);
    }

    let mut aggregate = FacetSearchAggregator::from_query(&query);
    let features = index_scheduler.features();
    let network = index_scheduler.network();

    let search_result = if query.must_use_network(&network, &features)? {
        search_federated(index_scheduler.clone(), query, index_uid, progress, features, network)
            .await
    } else {
        search_local(index_scheduler.clone(), query, index_uid, progress, features).await
    };

    permit.drop().await;

    if let Ok(ref search_result) = search_result {
        aggregate.succeed(search_result);
    }
    analytics.publish(aggregate, &req);

    let search_result = search_result?;

    debug!(returns = ?search_result, "Facet search");
    Ok(HttpResponse::Ok().json(search_result))
}

async fn search_federated(
    index_scheduler: Data<IndexScheduler>,
    mut query: FacetSearchQuery,
    index_uid: IndexUid,
    progress: Progress,
    features: RoFeatures,
    network: Network,
) -> Result<FacetSearchResult, ResponseError> {
    let before_search = std::time::Instant::now();
    let remote_availability = index_scheduler.remote_availability();
    let partition = Partition::new(network.clone(), remote_availability);

    let (local_queries, remote_queries): (Vec<_>, Vec<_>) =
        partition.into_partition(&query)?.partition(
            // true is left, false is right
            |(remote, _)| Some(remote) == network.local.as_ref(),
        );

    let timeout = std::env::var("MEILI_EXPERIMENTAL_REMOTE_SEARCH_TIMEOUT_SECONDS")
        .ok()
        .map(|p| p.parse().unwrap())
        .unwrap_or(25);

    let deadline = before_search + std::time::Duration::from_secs(timeout);

    let params = ProxySearchParams {
        deadline: Some(deadline),
        try_count: 3,
        client: index_scheduler.web_client().clone(),
    };

    let mut results: Vec<FacetSearchResult> = Vec::with_capacity(remote_queries.len() + 1);

    let mut errors: BTreeMap<String, ResponseError> = BTreeMap::new();

    const MAX_IN_FLIGHT_REQUESTS: usize = 40;
    let mut in_flight_requests = VecDeque::with_capacity(MAX_IN_FLIGHT_REQUESTS);

    for (remote_name, filter) in remote_queries {
        let Some(remote) = network.remotes.get(&remote_name) else {
            errors.insert(
                remote_name.clone(),
                ProxySearchError::UnknownRemote { remote: remote_name }.as_response_error(),
            );
            continue;
        };

        let path_and_query = match meilisearch_types::network::route::facet_search_path(&index_uid)
        {
            Ok(path_and_query) => path_and_query,
            Err(err) => {
                errors.insert(
                    remote_name,
                    ProxySearchError::InvalidRemoteUrl { cause: err.to_string() }
                        .as_response_error(),
                );
                continue;
            }
        };
        query.filter = filter;
        let request = match json_proxy(
            path_and_query,
            http_client::reqwest::Method::POST,
            remote,
            &query,
            &params,
            false, // no metadata on facet-search
        ) {
            Ok(request) => request,
            Err(err) => {
                errors.insert(remote_name, err.as_response_error());
                continue;
            }
        };

        if in_flight_requests.len() == MAX_IN_FLIGHT_REQUESTS {
            // unwrap: MAX_IN_FLIGHT_REQUESTS > 0
            let task: tokio::task::JoinHandle<(
                Result<FacetSearchResult, ProxySearchError>,
                String,
            )> = in_flight_requests.pop_front().unwrap();
            match task.await.unwrap() {
                (Ok(result), _) => results.push(result),
                (Err(err), remote_name) => {
                    errors.insert(remote_name, err.as_response_error());
                    continue;
                }
            }
        }
        in_flight_requests.push_back(tokio::spawn(async move { (request.await, remote_name) }));
    }

    let (mut local_results, order) =
        search_multi_local(local_queries, index_scheduler, query, index_uid, progress, features)
            .await?;

    for task in in_flight_requests {
        match task.await.unwrap() {
            (Ok(result), _) => results.push(result),
            (Err(err), remote_name) => {
                errors.insert(remote_name, err.as_response_error());
            }
        }
    }

    let facet_query = local_results.facet_query.take();
    let processing_time_ms = local_results.processing_time_ms;
    results.push(local_results);

    let facet_hits = match order {
        OrderBy::Lexicographic => lexicographic_merge(results),
        OrderBy::Count => count_merge(results),
    };

    Ok(FacetSearchResult {
        facet_hits,
        facet_query,
        processing_time_ms,
        remote_errors: Some(errors),
    })
}

fn count_merge(mut results: Vec<FacetSearchResult>) -> Vec<FacetValueHit> {
    // sort lexicographically so we can merge results, then sort again by the new hit count
    results
        .iter_mut()
        .for_each(|v| v.facet_hits.sort_unstable_by(|left, right| left.value.cmp(&right.value)));
    let mut hits = lexicographic_merge(results);
    hits.sort_unstable_by_key(|hit| std::cmp::Reverse(hit.count));
    hits
}

// Precondition: all `FacetSearchResult::facet_hits` in `results` are sorted lexicographically
fn lexicographic_merge(results: Vec<FacetSearchResult>) -> Vec<FacetValueHit> {
    itertools::kmerge_by(
        results.into_iter().map(|results| results.facet_hits.into_iter()),
        |left: &FacetValueHit, right: &FacetValueHit| left.value <= right.value,
    )
    .coalesce(|left, right| {
        if left.value == right.value {
            Ok(FacetValueHit { value: right.value, count: left.count + right.count })
        } else {
            Err((left, right))
        }
    })
    .collect()
}

async fn search_multi_local(
    local_queries: Vec<(String, Option<serde_json::Value>)>,
    index_scheduler: Data<IndexScheduler>,
    query: FacetSearchQuery,
    index_uid: IndexUid,
    progress: Progress,
    features: RoFeatures,
) -> Result<(FacetSearchResult, OrderBy), ResponseError> {
    let facet_query = query.facet_query.clone();
    let facet_name = query.facet_name.clone();
    let locales = query.locales.clone().map(|l| l.into_iter().map(Into::into).collect());
    let search_query = SearchQuery::from(query);

    let progress_clone = progress.clone();
    let search_result = tokio::task::spawn_blocking(move || {
        let index = index_scheduler.index(&index_uid)?;
        let rtxn = index.read_txn()?;
        let deadline = index.search_deadline(&rtxn)?;
        let search_kind =
            search_kind(&search_query, &index_scheduler, index_uid.to_string(), &index)?;

        let filters: Result<Vec<_>, ResponseError> = local_queries
            .iter() // no into_iter: we need to keep the original filters live as the IndexFilters reference them
            .map(|(_, filter)| {
                Ok(match filter {
                    Some(filter) => {
                        let filter = parse_filter(filter, Code::InvalidSearchFilter, features)?;
                        filter
                            .map(|f| {
                                filter_into_index_filter(
                                    f,
                                    &index,
                                    &rtxn,
                                    &index_scheduler,
                                    &progress_clone,
                                    &index_uid,
                                )
                            })
                            .transpose()?
                    }
                    None => None,
                })
            })
            .collect();
        let filters = filters?;

        let (search, _, _, _) = prepare_search(
            &index,
            &rtxn,
            &search_query,
            None,
            &search_kind,
            deadline,
            features,
            &progress_clone,
        )?;

        perform_facet_search(
            &index,
            &rtxn,
            search,
            filters.into_iter(),
            facet_query,
            facet_name,
            search_kind,
            locales,
        )
    })
    .await;
    search_result?
}

async fn search_local(
    index_scheduler: Data<IndexScheduler>,
    query: FacetSearchQuery,
    index_uid: IndexUid,
    progress: Progress,
    features: RoFeatures,
) -> Result<FacetSearchResult, ResponseError> {
    let facet_query = query.facet_query.clone();
    let facet_name = query.facet_name.clone();
    let locales = query.locales.clone().map(|l| l.into_iter().map(Into::into).collect());
    let search_query = SearchQuery::from(query);

    let progress_clone = progress.clone();
    let search_result = tokio::task::spawn_blocking(move || {
        let index = index_scheduler.index(&index_uid)?;
        let rtxn = index.read_txn()?;
        let deadline = index.search_deadline(&rtxn)?;
        let search_kind =
            search_kind(&search_query, &index_scheduler, index_uid.to_string(), &index)?;
        let filter = match &search_query.filter {
            Some(filter) => {
                let filter = parse_filter(filter, Code::InvalidSearchFilter, features)?;
                filter
                    .map(|f| {
                        filter_into_index_filter(
                            f,
                            &index,
                            &rtxn,
                            &index_scheduler,
                            &progress_clone,
                            &index_uid,
                        )
                    })
                    .transpose()?
            }
            None => None,
        };

        let (search, _, _, _) = prepare_search(
            &index,
            &rtxn,
            &search_query,
            // Filter is passed as an iterator to facet search
            None,
            &search_kind,
            deadline,
            features,
            &progress_clone,
        )?;

        perform_facet_search(
            &index,
            &rtxn,
            search,
            std::iter::once(filter),
            facet_query,
            facet_name,
            search_kind,
            locales,
        )
        .map(|(results, _)| results)
    })
    .await;
    search_result?
}

impl From<FacetSearchQuery> for SearchQuery {
    fn from(value: FacetSearchQuery) -> Self {
        let FacetSearchQuery {
            facet_query: _,
            facet_name: _,
            q,
            vector,
            media,
            filter,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
            exhaustive_facet_count,
            use_network,
        } = value;

        // If exhaustive_facet_count is true, we need to set the page to 0
        // because the facet search is not exhaustive by default.
        let page = if exhaustive_facet_count.is_some_and(|exhaustive| exhaustive) {
            // setting the page to 0 will force the search to be exhaustive when computing the number of hits,
            // but it will skip the bucket sort saving time.
            Some(0)
        } else {
            None
        };

        SearchQuery {
            q,
            media,
            offset: DEFAULT_SEARCH_OFFSET(),
            limit: DEFAULT_SEARCH_LIMIT(),
            page,
            hits_per_page: None,
            attributes_to_retrieve: None,
            retrieve_vectors: false,
            attributes_to_crop: None,
            crop_length: DEFAULT_CROP_LENGTH(),
            attributes_to_highlight: None,
            show_matches_position: false,
            show_ranking_score: false,
            show_ranking_score_details: false,
            show_performance_details: false,
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
            personalize: None,
            use_network,
        }
    }
}

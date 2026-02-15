use actix_http::StatusCode;
use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::locales::Locale;
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::{self, TotalProcessingTimeStep};
use meilisearch_types::serde_cs::vec::CS;
use serde_json::Value;
use tracing::debug;
use utoipa::{IntoParams, OpenApi};
use uuid::Uuid;

use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::personalization::PersonalizationService;
use crate::routes::indexes::search_analytics::{SearchAggregator, SearchGET, SearchPOST};
use crate::routes::parse_include_metadata_header;
use crate::search::{
    add_search_rules, network_partition, perform_federated_search, perform_search, Federation,
    HybridQuery, MatchingStrategy, Personalize, RankingScoreThreshold, RetrieveVectors, SearchKind,
    SearchParams, SearchQuery, SearchResult, SemanticRatio, DEFAULT_CROP_LENGTH,
    DEFAULT_CROP_MARKER, DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG,
    DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET, DEFAULT_SEMANTIC_RATIO,
};
use crate::search_queue::SearchQueue;

#[derive(OpenApi)]
#[openapi(
    paths(search_with_url_query, search_with_post),
    tags(
        (
            name = "Search",
            description = "Meilisearch exposes two routes to perform searches:

- A POST route: this is the preferred route when using API authentication, as it allows [preflight request](https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request) caching and better performance.
- A GET route: the usage of this route is discouraged, unless you have good reason to do otherwise (specific caching abilities for example)",
        ),
    ),
)]
pub struct SearchApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(search_with_url_query)))
            .route(web::post().to(SeqHandler(search_with_post))),
    );
}

#[derive(Debug, deserr::Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct SearchQueryGet {
    /// The search query string. Meilisearch will return documents that match
    /// this query. Supports prefix search (words matching the beginning of
    /// the query) and typo tolerance. Leave empty to match all documents.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchQ>)]
    q: Option<String>,
    /// A vector of floating-point numbers for semantic/vector search. The
    /// dimensions must match the embedder configuration. When provided,
    /// documents are ranked by vector similarity. Can be combined with `q`
    /// for hybrid search.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchVector>)]
    #[param(required = false, value_type = Vec<f32>, explode = false)]
    vector: Option<CS<f32>>,
    /// Number of search results to skip. Use together with `limit` for
    /// pagination. For example, to get results 21-40, set `offset=20` and
    /// `limit=20`. Defaults to `0`. Cannot be used with `page`/`hitsPerPage`.
    #[deserr(default = Param(DEFAULT_SEARCH_OFFSET()), error = DeserrQueryParamError<InvalidSearchOffset>)]
    #[param(required = false, value_type = usize, default = DEFAULT_SEARCH_OFFSET)]
    offset: Param<usize>,
    /// Maximum number of search results to return. Use together with `offset`
    /// for pagination. Defaults to `20`. Cannot be used with
    /// `page`/`hitsPerPage`.
    #[deserr(default = Param(DEFAULT_SEARCH_LIMIT()), error = DeserrQueryParamError<InvalidSearchLimit>)]
    #[param(required = false, value_type = usize, default = DEFAULT_SEARCH_LIMIT)]
    limit: Param<usize>,
    /// Request a specific page of results (1-indexed). Use together with
    /// `hitsPerPage` for page-based pagination. Cannot be used with
    /// `offset`/`limit`.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchPage>)]
    #[param(required = false, value_type = Option<usize>)]
    page: Option<Param<usize>>,
    /// Number of results per page when using page-based pagination. Use
    /// together with `page`. Cannot be used with `offset`/`limit`.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchHitsPerPage>)]
    #[param(required = false, value_type = Option<usize>)]
    hits_per_page: Option<Param<usize>>,
    /// Comma-separated list of attributes to include in the returned
    /// documents. Use `*` to return all attributes. By default, returns
    /// attributes from the `displayedAttributes` setting.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToRetrieve>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    attributes_to_retrieve: Option<CS<String>>,
    /// When `true`, includes vector embeddings in the response for documents
    /// that have them. Defaults to `false`.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchRetrieveVectors>)]
    #[param(required = false, value_type = bool, default)]
    retrieve_vectors: Param<bool>,
    /// Comma-separated list of attributes whose values should be cropped to
    /// fit within `cropLength`. Useful for displaying long text attributes
    /// in search results. Format: `attribute` or `attribute:length`.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToCrop>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    attributes_to_crop: Option<CS<String>>,
    /// Maximum number of words to keep when cropping attribute values. The
    /// cropped text will be centered around the matching terms. Defaults to
    /// `10`.
    #[deserr(default = Param(DEFAULT_CROP_LENGTH()), error = DeserrQueryParamError<InvalidSearchCropLength>)]
    #[param(required = false, value_type = usize, default = DEFAULT_CROP_LENGTH)]
    crop_length: Param<usize>,
    /// Comma-separated list of attributes whose matching terms should be
    /// highlighted with `highlightPreTag` and `highlightPostTag`. Use `*` to
    /// highlight all searchable attributes.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToHighlight>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    attributes_to_highlight: Option<CS<String>>,
    /// Filter expression to narrow down search results. Uses SQL-like syntax.
    /// Example: `genres = action AND rating > 4`. Only attributes in
    /// `filterableAttributes` can be used.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchFilter>)]
    filter: Option<String>,
    /// Comma-separated list of attributes to sort by. Format: `attribute:asc`
    /// or `attribute:desc`. Only attributes in `sortableAttributes` can be
    /// used. Custom ranking rules can also affect sort order.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchSort>)]
    sort: Option<String>,
    /// Attribute used to ensure only one document with each unique value is
    /// returned. Useful for deduplication. Only attributes in
    /// `filterableAttributes` can be used.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchDistinct>)]
    distinct: Option<String>,
    /// When `true`, returns the position (start and length) of each matched
    /// term in the original document attributes. Useful for custom
    /// highlighting implementations.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowMatchesPosition>)]
    #[param(required = false, value_type = bool)]
    show_matches_position: Param<bool>,
    /// When `true`, includes a `_rankingScore` field (0.0 to 1.0) in each
    /// document indicating how well it matches the query. Higher scores mean
    /// better matches.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowRankingScore>)]
    #[param(required = false, value_type = bool)]
    show_ranking_score: Param<bool>,
    /// When `true`, includes a `_rankingScoreDetails` object showing the
    /// contribution of each ranking rule to the final score. Useful for
    /// debugging relevancy.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowRankingScoreDetails>)]
    #[param(required = false, value_type = bool)]
    show_ranking_score_details: Param<bool>,
    /// When `true`, includes a `_performanceDetails` object showing the
    /// performance details of the search.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowPerformanceDetails>)]
    #[param(required = false, value_type = bool)]
    show_performance_details: Param<bool>,
    /// Comma-separated list of attributes for which to return facet
    /// distribution (value counts). Only attributes in `filterableAttributes`
    /// can be used. Returns the count of documents matching each facet value.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchFacets>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    facets: Option<CS<String>>,
    /// HTML tag or string to insert before highlighted matching terms.
    /// Defaults to `<em>`.
    #[deserr(default = DEFAULT_HIGHLIGHT_PRE_TAG(), error = DeserrQueryParamError<InvalidSearchHighlightPreTag>)]
    #[param(required = false, default = DEFAULT_HIGHLIGHT_PRE_TAG)]
    highlight_pre_tag: String,
    /// HTML tag or string to insert after highlighted matching terms.
    /// Defaults to `</em>`.
    #[deserr(default = DEFAULT_HIGHLIGHT_POST_TAG(), error = DeserrQueryParamError<InvalidSearchHighlightPostTag>)]
    #[param(required = false, default = DEFAULT_HIGHLIGHT_POST_TAG)]
    highlight_post_tag: String,
    /// String used to indicate truncated content when cropping. Defaults to
    /// `…` (ellipsis).
    #[deserr(default = DEFAULT_CROP_MARKER(), error = DeserrQueryParamError<InvalidSearchCropMarker>)]
    #[param(required = false, default = DEFAULT_CROP_MARKER)]
    crop_marker: String,
    /// Strategy for matching query terms. `last` (default): all terms must
    /// match, removing terms from the end if needed. `all`: all terms must
    /// match exactly. `frequency`: prioritizes matching frequent terms.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchMatchingStrategy>)]
    matching_strategy: MatchingStrategy,
    /// Comma-separated list of attributes to search in. By default, searches
    /// all `searchableAttributes`. Use this to restrict search to specific
    /// fields for better performance or relevance.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToSearchOn>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    pub attributes_to_search_on: Option<CS<String>>,
    /// Name of the embedder to use for hybrid/semantic search. Must match an
    /// embedder configured in the index settings.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchEmbedder>)]
    pub hybrid_embedder: Option<String>,
    /// Balance between keyword search (0.0) and semantic/vector search (1.0)
    /// in hybrid search. A value of 0.5 gives equal weight to both. Defaults
    /// to `0.5`.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchSemanticRatio>)]
    #[param(required = false, value_type = f32)]
    pub hybrid_semantic_ratio: Option<SemanticRatioGet>,
    /// Minimum ranking score (0.0 to 1.0) a document must have to be
    /// included in results. Documents with lower scores are excluded. Useful
    /// for filtering out poor matches.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchRankingScoreThreshold>)]
    #[param(required = false, value_type = f32)]
    pub ranking_score_threshold: Option<RankingScoreThresholdGet>,
    /// Comma-separated list of language locales to use for tokenization and
    /// processing. Useful for multilingual content. Example: `en,fr,de`.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchLocales>)]
    #[param(required = false, value_type = Vec<Locale>, explode = false)]
    pub locales: Option<CS<Locale>>,
    /// User-specific context for personalized search results. The format
    /// depends on your personalization configuration.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchPersonalizeUserContext>)]
    pub personalize_user_context: Option<String>,
    /// When true, allows the search to use the network (e.g. for semantic or hybrid search).
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchUseNetwork>)]
    #[param(required = false, value_type = Option<bool>)]
    use_network: Option<Param<bool>>,
}

#[derive(Debug, Clone, Copy, PartialEq, deserr::Deserr)]
#[deserr(try_from(String) = TryFrom::try_from -> InvalidSearchRankingScoreThreshold)]
pub struct RankingScoreThresholdGet(RankingScoreThreshold);

impl std::convert::TryFrom<String> for RankingScoreThresholdGet {
    type Error = InvalidSearchRankingScoreThreshold;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let f: f64 = s.parse().map_err(|_| InvalidSearchRankingScoreThreshold)?;
        Ok(RankingScoreThresholdGet(RankingScoreThreshold::try_from(f)?))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, deserr::Deserr)]
#[deserr(try_from(String) = TryFrom::try_from -> InvalidSearchSemanticRatio)]
pub struct SemanticRatioGet(SemanticRatio);

impl std::convert::TryFrom<String> for SemanticRatioGet {
    type Error = InvalidSearchSemanticRatio;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let f: f32 = s.parse().map_err(|_| InvalidSearchSemanticRatio)?;
        Ok(SemanticRatioGet(SemanticRatio::try_from(f)?))
    }
}

impl std::ops::Deref for SemanticRatioGet {
    type Target = SemanticRatio;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<SearchQueryGet> for SearchQuery {
    type Error = ResponseError;

    fn try_from(other: SearchQueryGet) -> Result<Self, Self::Error> {
        let filter = match other.filter {
            Some(f) => match serde_json::from_str(&f) {
                Ok(v) => Some(v),
                _ => Some(Value::String(f)),
            },
            None => None,
        };

        let hybrid = match (other.hybrid_embedder, other.hybrid_semantic_ratio) {
            (None, None) => None,
            (None, Some(_)) => {
                return Err(ResponseError::from_msg(
                    "`hybridEmbedder` is mandatory when `hybridSemanticRatio` is present".into(),
                    meilisearch_types::error::Code::InvalidSearchHybridQuery,
                ));
            }
            (Some(embedder), None) => {
                Some(HybridQuery { semantic_ratio: DEFAULT_SEMANTIC_RATIO(), embedder })
            }
            (Some(embedder), Some(semantic_ratio)) => {
                Some(HybridQuery { semantic_ratio: *semantic_ratio, embedder })
            }
        };

        if other.vector.is_some() && hybrid.is_none() {
            return Err(ResponseError::from_msg(
                "`hybridEmbedder` is mandatory when `vector` is present".into(),
                meilisearch_types::error::Code::MissingSearchHybrid,
            ));
        }

        let personalize =
            other.personalize_user_context.map(|user_context| Personalize { user_context });

        let use_network = other.use_network.map(|use_network| use_network.0);

        Ok(Self {
            q: other.q,
            // `media` not supported for `GET`
            media: None,
            vector: other.vector.map(CS::into_inner),
            offset: other.offset.0,
            limit: other.limit.0,
            page: other.page.as_deref().copied(),
            hits_per_page: other.hits_per_page.as_deref().copied(),
            attributes_to_retrieve: other.attributes_to_retrieve.map(|o| o.into_iter().collect()),
            retrieve_vectors: other.retrieve_vectors.0,
            attributes_to_crop: other.attributes_to_crop.map(|o| o.into_iter().collect()),
            crop_length: other.crop_length.0,
            attributes_to_highlight: other.attributes_to_highlight.map(|o| o.into_iter().collect()),
            filter,
            sort: other.sort.map(|attr| fix_sort_query_parameters(&attr)),
            distinct: other.distinct,
            show_matches_position: other.show_matches_position.0,
            show_ranking_score: other.show_ranking_score.0,
            show_ranking_score_details: other.show_ranking_score_details.0,
            show_performance_details: other.show_performance_details.0,
            facets: other.facets.map(|o| o.into_iter().collect()),
            highlight_pre_tag: other.highlight_pre_tag,
            highlight_post_tag: other.highlight_post_tag,
            crop_marker: other.crop_marker,
            matching_strategy: other.matching_strategy,
            attributes_to_search_on: other.attributes_to_search_on.map(|o| o.into_iter().collect()),
            hybrid,
            ranking_score_threshold: other.ranking_score_threshold.map(|o| o.0),
            locales: other.locales.map(|o| o.into_iter().collect()),
            personalize,
            use_network,
        })
    }
}

// TODO: TAMO: split on :asc, and :desc, instead of doing some weird things

/// Transform the sort query parameter into something that matches the post expected format.
pub fn fix_sort_query_parameters(sort_query: &str) -> Vec<String> {
    let mut sort_parameters = Vec::new();
    let mut merge = false;
    for current_sort in sort_query.trim_matches('"').split(',').map(|s| s.trim()) {
        if current_sort.starts_with("_geoPoint(") {
            sort_parameters.push(current_sort.to_string());
            merge = true;
        } else if merge && !sort_parameters.is_empty() {
            let s = sort_parameters.last_mut().unwrap();
            s.push(',');
            s.push_str(current_sort);
            if current_sort.ends_with("):desc") || current_sort.ends_with("):asc") {
                merge = false;
            }
        } else {
            sort_parameters.push(current_sort.to_string());
            merge = false;
        }
    }
    sort_parameters
}

/// Search with GET
///
/// Search for documents matching a query in the given index.
///
/// > Equivalent to the [search with POST route](https://www.meilisearch.com/docs/api-reference/search/search-with-post) in the Meilisearch API.
#[utoipa::path(
    get,
    path = "/{indexUid}/search",
    tags = ["Search"],
    security(("Bearer" = ["search", "*"])),
    params(
        ("indexUid" = String, Path, example = "movies", description = "Unique identifier of the index.", nullable = false),
        SearchQueryGet
    ),
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
pub async fn search_with_url_query(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    personalization_service: web::Data<crate::personalization::PersonalizationService>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<SearchQueryGet, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let request_uid = Uuid::now_v7();
    debug!(request_uid = ?request_uid, parameters = ?params, "Search get");
    let progress = Progress::default();
    progress.update_progress(TotalProcessingTimeStep::WaitForPermit);
    let permit = search_queue.try_get_search_permit().await?;
    progress.update_progress(TotalProcessingTimeStep::Search);
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let mut query: SearchQuery = params.into_inner().try_into()?;

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut query.filter, search_rules);
    }

    let mut aggregate = SearchAggregator::<SearchGET>::from_query(&query);

    let include_metadata = parse_include_metadata_header(&req);

    let search_result = search(
        query,
        index_scheduler.clone(),
        index_uid,
        request_uid,
        include_metadata,
        &progress,
        &personalization_service,
        StatusCode::NOT_FOUND,
    )
    .await;

    permit.drop().await;

    if let Ok(search_result) = search_result.as_ref() {
        aggregate.succeed(search_result);
    }
    analytics.publish(aggregate, &req);

    let search_result = search_result?;

    debug!(request_uid = ?request_uid, returns = ?search_result, progress = ?progress.accumulated_durations(), "Search get");

    Ok(HttpResponse::Ok().json(search_result))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn search(
    mut query: SearchQuery,
    index_scheduler: Data<IndexScheduler>,
    index_uid: IndexUid,
    request_uid: Uuid,
    include_metadata: bool,
    progress: &Progress,
    service: &PersonalizationService,
    index_not_found_http_code: StatusCode,
) -> Result<SearchResult, ResponseError> {
    // Extract personalization and query string before moving query
    let personalize = query.personalize.take();
    // Save the query string for personalization if requested
    let personalize_query = personalize.is_some().then(|| query.q.clone()).flatten();

    let features = index_scheduler.features();
    if query.use_network.is_some() {
        features.check_network("passing `useNetwork` in a search query")?
    }

    let (mut search_result, deadline) = if query
        .use_network
        // avoid accidental recursion
        .take()
        // false by default for now
        .unwrap_or_default()
    {
        let network = index_scheduler.network();
        let mut federation = Federation::default();
        let queries =
            network_partition(&mut federation, &query, None, &index_uid, network).collect();
        let search_result = perform_federated_search(
            &index_scheduler,
            queries,
            federation,
            features,
            false,
            request_uid,
            include_metadata,
            progress,
        )
        .await;

        let (search_result, deadline) = search_result?;
        let search_result =
            search_result.into_search_result(query.q.unwrap_or_default(), index_uid.as_str());

        (search_result, deadline)
    } else {
        let index = index_scheduler.index(&index_uid).map_err(|err| match &err {
            index_scheduler::Error::IndexNotFound(_) => {
                let mut err = ResponseError::from(err);
                err.code = index_not_found_http_code;
                err
            }
            _ => ResponseError::from(err),
        })?;

        let search_kind = search_kind(&query, &index_scheduler, index_uid.to_string(), &index)?;
        let retrieve_vector = RetrieveVectors::new(query.retrieve_vectors);

        let progress_clone = progress.clone();
        let search_result = tokio::task::spawn_blocking(move || {
            perform_search(
                SearchParams {
                    index_uid: index_uid.to_string(),
                    query,
                    search_kind,
                    retrieve_vectors: retrieve_vector,
                    features,
                    request_uid,
                    include_metadata,
                },
                &index,
                &progress_clone,
            )
        })
        .await;

        search_result??
    };

    // Apply personalization if requested
    if let Some(personalize) = personalize {
        search_result.hits = service
            .rerank_search_results(
                std::mem::take(&mut search_result.hits),
                &personalize,
                personalize_query.as_deref(),
                deadline,
                progress,
            )
            .await?;
    }

    Ok(search_result)
}

/// Search with POST
///
/// Search for documents matching a query in the given index.
///
/// > Equivalent to the [search with GET route](https://www.meilisearch.com/docs/api-reference/search/search-with-get) in the Meilisearch API.
#[utoipa::path(
    post,
    path = "/{indexUid}/search",
    tags = ["Search"],
    security(("Bearer" = ["search", "*"])),
    params(
        ("indexUid", example = "movies", description = "Unique identifier of the index.", nullable = false),
    ),
    request_body = SearchQuery,
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
pub async fn search_with_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    personalization_service: web::Data<crate::personalization::PersonalizationService>,
    index_uid: web::Path<String>,
    params: AwebJson<SearchQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let request_uid = Uuid::now_v7();

    let progress = Progress::default();
    progress.update_progress(TotalProcessingTimeStep::WaitForPermit);
    let permit = search_queue.try_get_search_permit().await?;
    progress.update_progress(TotalProcessingTimeStep::Search);

    let mut query = params.into_inner();
    debug!(request_uid = ?request_uid, parameters = ?query, "Search post");

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut query.filter, search_rules);
    }

    let mut aggregate = SearchAggregator::<SearchPOST>::from_query(&query);

    let include_metadata = parse_include_metadata_header(&req);

    let search_result = search(
        query,
        index_scheduler.clone(),
        index_uid,
        request_uid,
        include_metadata,
        &progress,
        &personalization_service,
        StatusCode::NOT_FOUND,
    )
    .await;

    permit.drop().await;

    if let Ok(search_result) = search_result.as_ref() {
        aggregate.succeed(search_result);
    }
    analytics.publish(aggregate, &req);

    let search_result = search_result?;

    debug!(request_uid = ?request_uid, returns = ?search_result, progress = ?progress.accumulated_durations(), "Search post");

    Ok(HttpResponse::Ok().json(search_result))
}

pub fn search_kind(
    query: &SearchQuery,
    index_scheduler: &IndexScheduler,
    index_uid: String,
    index: &milli::Index,
) -> Result<SearchKind, ResponseError> {
    let is_placeholder_query =
        if let Some(q) = query.q.as_deref() { q.trim().is_empty() } else { true };
    let non_placeholder_query = !is_placeholder_query;
    let is_media = query.media.is_some();
    // handle with care, the order of cases matters, the semantics is subtle
    match (is_media, non_placeholder_query, &query.hybrid, query.vector.as_deref()) {
        // media + vector => error
        (true, _, _, Some(_)) => Err(MeilisearchHttpError::MediaAndVector.into()),
        // media + !hybrid => error
        (true, _, None, _) => Err(MeilisearchHttpError::MissingSearchHybrid.into()),
        // vector + !hybrid => error
        (_, _, None, Some(_)) => Err(MeilisearchHttpError::MissingSearchHybrid.into()),
        // hybrid S0 => keyword
        (_, _, Some(HybridQuery { semantic_ratio, embedder: _ }), _) if **semantic_ratio == 0.0 => {
            Ok(SearchKind::KeywordOnly)
        }
        // !q + !vector => placeholder search
        (false, false, _, None) => Ok(SearchKind::KeywordOnly),
        // hybrid S100 => semantic
        (_, _, Some(HybridQuery { semantic_ratio, embedder }), v) if **semantic_ratio == 1.0 => {
            SearchKind::semantic(index_scheduler, index_uid, index, embedder, v.map(|v| v.len()))
        }
        // q + hybrid => hybrid
        (_, true, Some(HybridQuery { semantic_ratio, embedder }), v) => SearchKind::hybrid(
            index_scheduler,
            index_uid,
            index,
            embedder,
            **semantic_ratio,
            v.map(|v| v.len()),
        ),
        // !q + hybrid => semantic
        (_, false, Some(HybridQuery { semantic_ratio: _, embedder }), v) => {
            SearchKind::semantic(index_scheduler, index_uid, index, embedder, v.map(|v| v.len()))
        }
        // q => keyword
        (false, true, None, None) => Ok(SearchKind::KeywordOnly),
    }
}

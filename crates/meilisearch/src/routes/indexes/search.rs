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
use utoipa::IntoParams;
use uuid::Uuid;

use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::personalization::PersonalizationService;
use crate::routes::indexes::search_analytics::{SearchAggregator, SearchGET, SearchPOST};
use crate::routes::parse_include_metadata_header;
use crate::search::{
    add_search_rules, perform_federated_search, perform_search, Federation, HybridQuery,
    MatchingStrategy, Partition, Personalize, RankingScoreThreshold, RetrieveVectors, SearchKind,
    SearchParams, SearchQuery, SearchResult, SemanticRatio, DEFAULT_CROP_LENGTH,
    DEFAULT_CROP_MARKER, DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG,
    DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET, DEFAULT_SEMANTIC_RATIO,
};
use crate::search_queue::SearchQueue;

#[routes::routes(
    routes(""=>[get(search_with_url_query), post(search_with_post)]),
    tag = "Search",
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

#[derive(Debug, deserr::Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct SearchQueryGet {
    /// Search query string. Meilisearch returns documents matching this query. Supports
    /// prefix search and typo tolerance. Empty = placeholder search (all documents).
    /// Only the first ten words are considered. Normalized (lowercase, accents ignored).
    /// Use double quotes for phrase search; minus (`-`) before a word to exclude it.
    /// **GET route:** must be URL-encoded.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchQ>)]
    q: Option<String>,
    /// Custom query vector for semantic search. Dimensions must match the embedder.
    /// When provided, requires `hybridEmbedder`. Ranks by vector similarity; can be
    /// combined with `q` for hybrid search.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchVector>)]
    #[param(required = false, value_type = Vec<f32>, explode = false)]
    vector: Option<CS<f32>>,
    /// Number of documents to skip. Use with `limit` for pagination (e.g. offset=20,
    /// limit=20 for results 21–40). Default 0. Ignored if `page` or `hitsPerPage` is set.
    /// Response uses `estimatedTotalHits` only.
    #[deserr(default = Param(DEFAULT_SEARCH_OFFSET()), error = DeserrQueryParamError<InvalidSearchOffset>)]
    #[param(required = false, value_type = usize, default = DEFAULT_SEARCH_OFFSET)]
    offset: Param<usize>,
    /// Maximum number of documents to return. Default 20. Use with `offset`. Ignored if
    /// `page` or `hitsPerPage` is set. Cannot exceed index `maxTotalHits`.
    #[deserr(default = Param(DEFAULT_SEARCH_LIMIT()), error = DeserrQueryParamError<InvalidSearchLimit>)]
    #[param(required = false, value_type = usize, default = DEFAULT_SEARCH_LIMIT)]
    limit: Param<usize>,
    /// Request a specific results page (1-indexed). Use with `hitsPerPage`. When set,
    /// response includes `totalHits` and `totalPages`. Page/hitsPerPage override
    /// offset/limit.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchPage>)]
    #[param(required = false, value_type = Option<usize>)]
    page: Option<Param<usize>>,
    /// Number of documents per page. Use with `page`. When set, response includes
    /// `totalHits` and `totalPages`. Set to 0 to get totalHits without documents.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchHitsPerPage>)]
    #[param(required = false, value_type = Option<usize>)]
    hits_per_page: Option<Param<usize>>,
    /// Comma-separated attributes to include in results. Use `*` for all. Defaults to
    /// index `displayedAttributes`. Attributes not in displayedAttributes are omitted.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToRetrieve>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    attributes_to_retrieve: Option<CS<String>>,
    /// When true, include document and query vectors in the response (`_vectors`).
    /// Vectors must be in displayedAttributes. Default false.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchRetrieveVectors>)]
    #[param(required = false, value_type = bool, default)]
    retrieve_vectors: Param<bool>,
    /// Comma-separated attributes to crop. Cropped text in `_formatted`. Optional
    /// per-attribute length: `attribute:length`. Use `*` to crop all in attributesToRetrieve.
    /// Crop is centered around matching terms. **GET:** comma-separated list.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToCrop>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    attributes_to_crop: Option<CS<String>>,
    /// Maximum words in cropped values. Default 10. Applies when attributesToCrop is set.
    #[deserr(default = Param(DEFAULT_CROP_LENGTH()), error = DeserrQueryParamError<InvalidSearchCropLength>)]
    #[param(required = false, value_type = usize, default = DEFAULT_CROP_LENGTH)]
    crop_length: Param<usize>,
    /// Comma-separated attributes to highlight. Highlighted text in `_formatted`. Use `*`
    /// for all in attributesToRetrieve. Default tags `<em>`/`</em>`; override with
    /// highlightPreTag/highlightPostTag.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToHighlight>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    attributes_to_highlight: Option<CS<String>>,
    /// Filter expression. Attributes must be in filterableAttributes. **GET route**
    /// accepts string filter expressions only (e.g. `genres = action AND rating > 4`).
    /// Must be URL-encoded. Geo: `_geoRadius(lat,lng,distance_in_meters)`, etc.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchFilter>)]
    filter: Option<String>,
    /// Comma-separated sort: `attribute:asc` or `attribute:desc`. Only sortableAttributes.
    /// Geo: `_geoPoint(lat,lng):asc` or `:desc` (response includes `_geoDistance`).
    /// **GET:** comma-separated string; first attribute has precedence.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchSort>)]
    sort: Option<String>,
    /// Return one document per distinct value of this attribute (deduplication).
    /// Attribute must be in filterableAttributes. Overrides index distinctAttribute.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchDistinct>)]
    distinct: Option<String>,
    /// When true, add `_matchesPosition` with byte offset (start, length) of each
    /// matched term. Useful for custom highlighting. Measured in bytes.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowMatchesPosition>)]
    #[param(required = false, value_type = bool)]
    show_matches_position: Param<bool>,
    /// When true, add `_rankingScore` (0.0–1.0) to each document. Higher = more relevant.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowRankingScore>)]
    #[param(required = false, value_type = bool)]
    show_ranking_score: Param<bool>,
    /// When true, add `_rankingScoreDetails` with each ranking rule's contribution.
    /// Useful for debugging relevancy.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowRankingScoreDetails>)]
    #[param(required = false, value_type = bool)]
    show_ranking_score_details: Param<bool>,
    /// When true, add performance timing details to the response.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowPerformanceDetails>)]
    #[param(required = false, value_type = bool)]
    show_performance_details: Param<bool>,
    /// Comma-separated attributes for facet distribution (count per value). Use `*`
    /// for all filterableAttributes. Response includes facetDistribution and facetStats
    /// (min/max for numeric facets). Limited by index maxValuesPerFacet (default 100).
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchFacets>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    facets: Option<CS<String>>,
    /// String inserted before highlighted terms. Default `<em>`. Null or empty = none.
    #[deserr(default = DEFAULT_HIGHLIGHT_PRE_TAG(), error = DeserrQueryParamError<InvalidSearchHighlightPreTag>)]
    #[param(required = false, default = DEFAULT_HIGHLIGHT_PRE_TAG)]
    highlight_pre_tag: String,
    /// String inserted after highlighted terms. Default `</em>`. Match highlightPreTag
    /// to avoid malformed output.
    #[deserr(default = DEFAULT_HIGHLIGHT_POST_TAG(), error = DeserrQueryParamError<InvalidSearchHighlightPostTag>)]
    #[param(required = false, default = DEFAULT_HIGHLIGHT_POST_TAG)]
    highlight_post_tag: String,
    /// String marking crop boundaries. Default `"…"`. Null or empty = no markers.
    #[deserr(default = DEFAULT_CROP_MARKER(), error = DeserrQueryParamError<InvalidSearchCropMarker>)]
    #[param(required = false, default = DEFAULT_CROP_MARKER)]
    crop_marker: String,
    /// Match strategy: `last` (default) = require all terms, drop from end if needed;
    /// `all` = only documents with all terms; `frequency` = drop most frequent term first.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchMatchingStrategy>)]
    matching_strategy: MatchingStrategy,
    /// Comma-separated attributes to search in. Default `*` (all searchableAttributes).
    /// Attributes must be in searchableAttributes. Order does not affect relevancy.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToSearchOn>)]
    #[param(required = false, value_type = Vec<String>, explode = false)]
    pub attributes_to_search_on: Option<CS<String>>,
    /// Embedder name for hybrid/semantic search. Must match an embedder in index settings.
    /// Required when `vector` or `hybridSemanticRatio` is set.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchEmbedder>)]
    pub hybrid_embedder: Option<String>,
    /// Balance keyword (0.0) vs semantic (1.0) in hybrid search. Default 0.5. Requires
    /// hybridEmbedder when set.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchSemanticRatio>)]
    #[param(required = false, value_type = f32)]
    pub hybrid_semantic_ratio: Option<SemanticRatioGet>,
    /// Exclude documents with ranking score below this (0.0–1.0). Excluded hits do not
    /// count toward estimatedTotalHits, totalHits, or facets. With page/hitsPerPage may
    /// impact performance.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchRankingScoreThreshold>)]
    #[param(required = false, value_type = f32)]
    pub ranking_score_threshold: Option<RankingScoreThresholdGet>,
    /// Comma-separated query language locales (e.g. en,fr,de). Overrides auto-detection.
    /// Use when auto-detection is wrong; see also index localizedAttributes.
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchLocales>)]
    #[param(required = false, value_type = Vec<Locale>, explode = false)]
    pub locales: Option<CS<Locale>>,
    /// User context string for personalized search (e.g. preferences, behavior).
    /// Requires personalization enabled (e.g. Cohere key for self-hosted).
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchPersonalizeUserContext>)]
    pub personalize_user_context: Option<String>,
    /// When `true`, runs the query on the whole network (all shards covered, documents
    /// deduplicated across remotes). When `false` or omitted, the query runs locally.
    ///
    /// **Enterprise Edition only.** This feature is available in the Enterprise Edition.
    /// It also requires the `network` experimental feature.
    ///
    /// Values: `true` = use the whole network; `false` or omitted = local (default).
    ///
    /// When using the network, the index must exist with compatible settings on all remotes;
    /// documents with the same id are assumed identical for deduplication.
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
/// > Equivalent to the [search with POST route](/reference/api/search/search-with-post) in the Meilisearch API.
#[routes::path(
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
        let queries = Partition::new(network)
            .into_query_partition(&mut federation, &query, None, &index_uid)?
            .collect();
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
/// > Equivalent to the [search with GET route](/reference/api/search/search-with-get) in the Meilisearch API.
#[routes::path(
    security(("Bearer" = ["search", "*"])),
    params(
        ("indexUid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false),
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

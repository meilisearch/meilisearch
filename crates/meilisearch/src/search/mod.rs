use core::fmt;
use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Not as _;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use deserr::Deserr;
use either::Either;
use index_scheduler::{IndexScheduler, RoFeatures};
use indexmap::IndexMap;
use meilisearch_auth::IndexSearchRules;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::locales::Locale;
use meilisearch_types::milli::index::{self, EmbeddingsWithMetadata, SearchParameters};
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::score_details::{ScoreDetails, ScoringStrategy};
use meilisearch_types::milli::vector::parsed_vectors::ExplicitVectors;
use meilisearch_types::milli::vector::Embedder;
use meilisearch_types::milli::{
    AttributeState, Deadline, FacetValueHit, InternalError, OrderBy, PatternMatch,
    SearchForFacetValues, SearchStep,
};
use meilisearch_types::settings::DEFAULT_PAGINATION_MAX_TOTAL_HITS;
use meilisearch_types::{milli, Document};
use milli::tokenizer::{Language, TokenizerBuilder};
use milli::{
    AscDesc, FieldId, FieldsIdsMap, Filter, FormatOptions, Index, LocalizedAttributesRule,
    MatchBounds, MatcherBuilder, SortError, TermsMatchingStrategy, DEFAULT_VALUES_PER_FACET,
};
use permissive_json_pointer::contained_in;
use regex::Regex;
use serde::de::DeserializeSeed as _;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
#[cfg(test)]
mod mod_test;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::error::MeilisearchHttpError;
use crate::search::value_paths_visitor::ValuePathsVisitor;

mod federated;
pub use federated::{
    perform_federated_search, FederatedSearch, FederatedSearchResult, Federation,
    FederationOptions, MergeFacets, Partition, PROXY_SEARCH_HEADER, PROXY_SEARCH_HEADER_VALUE,
};

mod hydration;
mod value_paths_visitor;
use hydration::hydrate_documents;
mod ranking_rules;

type MatchesPosition = BTreeMap<String, Vec<MatchBounds>>;

pub const DEFAULT_SEARCH_OFFSET: fn() -> usize = || 0;
pub const DEFAULT_SEARCH_LIMIT: fn() -> usize = || 20;
pub const DEFAULT_CROP_LENGTH: fn() -> usize = || 10;
pub const DEFAULT_CROP_MARKER: fn() -> String = || "…".to_string();
pub const DEFAULT_HIGHLIGHT_PRE_TAG: fn() -> String = || "<em>".to_string();
pub const DEFAULT_HIGHLIGHT_POST_TAG: fn() -> String = || "</em>".to_string();
pub const DEFAULT_SEMANTIC_RATIO: fn() -> SemanticRatio = || SemanticRatio(0.5);
pub const INCLUDE_METADATA_HEADER: &str = "Meili-Include-Metadata";

/// Configuration for [personalized search](https://www.meilisearch.com/docs/learn/personalization/making_personalized_search_queries) results.
///
/// When enabled, results are tailored to the user profile described in `userContext`.
#[derive(Clone, Default, PartialEq, Deserr, ToSchema, Debug)]
#[deserr(error = DeserrJsonError<InvalidSearchPersonalize>, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct Personalize {
    /// String describing the user (e.g. preferences, behavior).
    ///
    /// Used to return different results for different profiles.
    #[deserr(error = DeserrJsonError<InvalidSearchPersonalizeUserContext>)]
    pub user_context: String,
}

#[derive(Clone, Default, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct SearchQuery {
    /// Sets the search terms.
    ///
    /// Meilisearch returns documents that match this query.
    ///
    /// The query supports [prefix search](https://www.meilisearch.com/docs/learn/engine/prefix) and [typo tolerance](https://www.meilisearch.com/docs/learn/relevancy/typo_tolerance_settings).
    ///
    /// Meilisearch only considers the first ten words; terms are normalized (lowercase, accents ignored).
    ///
    /// Omit or leave empty for a placeholder search: no query terms are applied, so Meilisearch returns all searchable documents in the index, ordered by [ranking rules](https://www.meilisearch.com/docs/learn/relevancy/ranking_rules).
    ///
    /// Enclose terms in double quotes (`"`) for phrase search: only documents containing that exact sequence of words are returned (e.g. `"Winter Feast"`).
    ///
    /// Use a minus sign (`-`) before a word or phrase to exclude it from results.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    /// Number of documents to skip at the start of the results.
    ///
    /// Use together with `limit` for [pagination](https://www.meilisearch.com/docs/guides/front_end/pagination) (e.g. offset=20 and limit=20 returns results 21–40).
    ///
    /// This parameter is ignored when `page` or `hitsPerPage` is set; in that case the response includes `totalHits` and `totalPages` instead of `estimatedTotalHits`.
    #[deserr(default = DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSearchOffset>)]
    #[schema(required = false, default = DEFAULT_SEARCH_OFFSET)]
    pub offset: usize,
    /// Maximum number of documents to return in the response.
    ///
    /// Use with `offset` for [pagination](https://www.meilisearch.com/docs/guides/front_end/pagination).
    ///
    /// This parameter is ignored when `page` or `hitsPerPage` is set. The value cannot exceed the index [maxTotalHits](https://www.meilisearch.com/docs/reference/api/settings/update-pagination#body-max-total-hits-one-of-0) setting.
    #[deserr(default = DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSearchLimit>)]
    #[schema(required = false, default = DEFAULT_SEARCH_LIMIT)]
    pub limit: usize,
    /// Request a specific results page (1-indexed).
    ///
    /// Use together with `hitsPerPage`.
    ///
    /// When this parameter is set, the response includes `totalHits` and `totalPages` instead of `estimatedTotalHits`.
    ///
    /// `page` and `hitsPerPage` take precedence over `offset` and `limit`.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchPage>)]
    pub page: Option<usize>,
    /// Maximum number of documents per page for [pagination](https://www.meilisearch.com/docs/guides/front_end/pagination).
    ///
    /// This value determines `totalPages`; use it together with `page`.
    ///
    /// When set, the response includes `totalHits` and `totalPages`.
    ///
    /// Set to 0 to obtain the exhaustive `totalHits` count without returning any documents.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchHitsPerPage>)]
    pub hits_per_page: Option<usize>,
    /// List of attributes to include in each returned document.
    ///
    /// Use `["*"]` to return all attributes; if not set, the index [displayed attributes](https://www.meilisearch.com/docs/learn/relevancy/displayed_searchable_attributes) list is used.
    ///
    /// Attributes that are not in [displayedAttributes](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-displayed-attributes-one-of-0) are omitted from the response.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    /// Attributes whose values should be cropped to a short excerpt.
    ///
    /// The cropped text appears in each hit's `_formatted` object.
    ///
    /// Length is controlled by `cropLength`, or you can override it per attribute with the `attribute:length` syntax.
    ///
    /// Use `["*"]` to crop all attributes in `attributesToRetrieve`.
    ///
    /// When possible, the crop is centered around the matching terms.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToCrop>)]
    pub attributes_to_crop: Option<Vec<String>>,
    /// Maximum number of words to include in cropped values.
    ///
    /// This parameter only applies when `attributesToCrop` is set.
    ///
    /// Both query terms and [stop words](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-stop-words-one-of-0) count toward this length.
    #[deserr(error = DeserrJsonError<InvalidSearchCropLength>, default = DEFAULT_CROP_LENGTH())]
    #[schema(required = false, default = DEFAULT_CROP_LENGTH)]
    pub crop_length: usize,
    /// String used to mark crop boundaries in cropped text.
    ///
    /// If null or empty, no markers are inserted.
    ///
    /// Markers are only added where content was actually removed.
    #[deserr(error = DeserrJsonError<InvalidSearchCropMarker>, default = DEFAULT_CROP_MARKER())]
    #[schema(required = false, default = DEFAULT_CROP_MARKER)]
    pub crop_marker: String,
    /// Attributes in which matching query terms should be highlighted.
    ///
    /// The highlighted text appears in each hit's `_formatted` object.
    ///
    /// Use `["*"]` to highlight in all attributes from `attributesToRetrieve`.
    ///
    /// By default, matches are wrapped in `<em>` and `</em>`; you can override this with `highlightPreTag` and `highlightPostTag`.
    ///
    /// Highlighting also applies to [synonyms](https://www.meilisearch.com/docs/learn/relevancy/synonyms) and [stop words](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-stop-words-one-of-0).
    ///
    /// Supported value types are string, number, array, and object.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToHighlight>)]
    pub attributes_to_highlight: Option<HashSet<String>>,
    /// String to insert before each highlighted term.
    ///
    /// Can be any string (e.g. `<strong>`, `*`).
    ///
    /// If null or empty, nothing is inserted at the start of a match.
    #[deserr(error = DeserrJsonError<InvalidSearchHighlightPreTag>, default = DEFAULT_HIGHLIGHT_PRE_TAG())]
    #[schema(required = false, default = DEFAULT_HIGHLIGHT_PRE_TAG)]
    pub highlight_pre_tag: String,
    /// String to insert after each highlighted term.
    ///
    /// Should be used together with `highlightPreTag` to avoid malformed output (e.g. unclosed HTML tags).
    #[deserr(error = DeserrJsonError<InvalidSearchHighlightPostTag>, default = DEFAULT_HIGHLIGHT_POST_TAG())]
    #[schema(required = false, default = DEFAULT_HIGHLIGHT_POST_TAG)]
    pub highlight_post_tag: String,
    /// When true, each hit includes a `_matchesPosition` object with the byte offset (`start` and `length`) of each matched term.
    ///
    /// This is useful when you need custom highlighting.
    ///
    /// Note that positions are given in bytes, not characters.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowMatchesPosition>)]
    pub show_matches_position: bool,
    /// A [filter](https://www.meilisearch.com/docs/learn/filtering_and_sorting/filter_search_results) expression to narrow results.
    ///
    /// All attributes used in the expression must be in [filterableAttributes](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-filterable-attributes-one-of-0).
    ///
    /// You can pass a string (e.g. `"(genres = horror OR genres = mystery) AND director = 'Jordan Peele'"`) or an array (e.g. `[["genres = horror", "genres = mystery"], "director = 'Jordan Peele'"]`).
    ///
    /// For [geo search](https://www.meilisearch.com/docs/learn/filtering_and_sorting/geosearch), use `_geoRadius(lat, lng, distance_in_meters)`, `_geoBoundingBox([lat,lng],[lat,lng])`, or `_geoPolygon([lat,lng], ...)` (GeoJSON only for polygon).
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    /// Sort results by one or more attributes and their order.
    ///
    /// Use the format `["attribute:asc", "attribute:desc"]`; only attributes in [sortableAttributes](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-sortable-attributes-one-of-0) can be used.
    ///
    /// For [geo search](https://www.meilisearch.com/docs/learn/filtering_and_sorting/geosearch), use `_geoPoint(lat,lng):asc` or `:desc`; the response then includes `_geoDistance` in meters.
    ///
    /// The first attribute in the list has precedence.
    ///
    /// See [sorting search results](https://www.meilisearch.com/docs/learn/filtering_and_sorting/sort_search_results).
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchSort>)]
    pub sort: Option<Vec<String>>,
    /// Return only one document per distinct value of the given attribute (e.g. deduplicate by product_id).
    ///
    /// The attribute must be in [filterableAttributes](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-filterable-attributes-one-of-0).
    ///
    /// This overrides the index [distinctAttribute](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-distinct-attribute-one-of-0) setting for this request.
    ///
    /// See [distinct attribute](https://www.meilisearch.com/docs/learn/relevancy/distinct_attribute).
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchDistinct>)]
    pub distinct: Option<String>,
    /// Return the count of matches per facet value for the listed attributes.
    ///
    /// The response includes `facetDistribution` and, for numeric facets, `facetStats` (min/max).
    ///
    /// Use `["*"]` to request counts for all [filterableAttributes](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-filterable-attributes-one-of-0).
    ///
    /// The number of values returned per facet is limited by the index [maxValuesPerFacet](https://www.meilisearch.com/docs/reference/api/settings/update-faceting#body-max-values-per-facet-one-of-0) setting; attributes not in filterableAttributes are ignored.
    ///
    /// More info: [faceting](https://www.meilisearch.com/docs/learn/filtering_and_sorting/search_with_facet_filters).
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchFacets>)]
    pub facets: Option<Vec<String>>,
    /// How to match query terms when there are not enough results to satisfy `limit`.
    ///
    /// **`last`**: Returns documents containing all query terms first. If there are not enough such results, Meilisearch removes one query term at a time, starting from the end of the query (e.g. for "big fat cat", then "big fat", then "big").
    ///
    /// **`all`**: Only returns documents that contain all query terms. Meilisearch does not relax the query even if fewer than `limit` documents match.
    ///
    /// **`frequency`**: Returns documents containing all query terms first. If there are not enough, removes one term at a time starting with the word that is most frequent in the dataset, giving more weight to rarer terms (e.g. in "white cotton shirt", prioritizes documents containing "white" if "shirt" is very common).
    ///
    /// Default: `last`.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>)]
    pub matching_strategy: MatchingStrategy,
    /// Restrict the search to the listed attributes only.
    ///
    /// Each attribute must be in the index [searchable attributes](https://www.meilisearch.com/docs/learn/relevancy/displayed_searchable_attributes) list.
    ///
    /// The order of attributes in this parameter does not affect relevancy.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToSearchOn>)]
    pub attributes_to_search_on: Option<Vec<String>>,
    /// Exclude from the results any document whose [ranking score](https://www.meilisearch.com/docs/learn/relevancy/ranking_score) is below this value (between 0.0 and 1.0).
    ///
    /// Excluded hits do not count toward `estimatedTotalHits`, `totalHits`, or facet distribution.
    ///
    /// When used together with `page` and `hitsPerPage`, this parameter may reduce performance because Meilisearch must score all matching documents.
    #[deserr(default, error = DeserrJsonError<InvalidSearchRankingScoreThreshold>)]
    #[schema(required = false, value_type = Option<f64>)]
    pub ranking_score_threshold: Option<RankingScoreThreshold>,
    /// Explicitly specify the language(s) of the query.
    ///
    /// Pass an array of [supported ISO-639 locales](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-localized-attributes-one-of-0).
    ///
    /// This overrides auto-detection; use it when auto-detection is wrong for the query or the documents.
    ///
    /// See also the [localizedAttributes](https://www.meilisearch.com/docs/reference/api/settings/list-all-settings#response-localized-attributes-one-of-0) settings and [Language](https://www.meilisearch.com/docs/learn/resources/language).
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchLocales>)]
    pub locales: Option<Vec<Locale>>,
    /// [Hybrid search](https://www.meilisearch.com/docs/learn/ai_powered_search/getting_started_with_ai_search): combines keyword and semantic search.
    ///
    /// The `embedder` field (required) must match an embedder name in index settings.
    ///
    /// The `semanticRatio` field controls the balance: 0.0 means keyword-only results, 1.0 means semantic-only.
    ///
    /// When `q` is empty and `semanticRatio` is greater than 0, Meilisearch performs a pure semantic search.
    #[deserr(default, error = DeserrJsonError<InvalidSearchHybridQuery>)]
    #[schema(required = false, value_type = Option<HybridQuery>)]
    pub hybrid: Option<HybridQuery>,
    /// Custom query vector for [vector or hybrid search](https://www.meilisearch.com/docs/learn/ai_powered_search/getting_started_with_ai_search).
    ///
    /// The array length must match the dimensions of the embedder configured in the index.
    ///
    /// This parameter is mandatory when using a [user-provided embedder](https://www.meilisearch.com/docs/learn/ai_powered_search/search_with_user_provided_embeddings).
    ///
    /// When used with `hybrid`, documents are ranked by vector similarity.
    ///
    /// You can also use it to override an embedder's automatic vector generation.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchVector>)]
    pub vector: Option<Vec<f32>>,
    /// When true, the response includes document and query embeddings in each hit's `_vectors` field.
    ///
    /// The `_vectors` field must be listed in [displayedAttributes](https://www.meilisearch.com/docs/reference/api/settings/update-all-settings#body-displayed-attributes-one-of-0) for it to appear.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchRetrieveVectors>)]
    pub retrieve_vectors: bool,
    /// For [multimodal search](https://www.meilisearch.com/docs/learn/ai_powered_search/image_search_with_multimodal_embeddings): provide data (e.g. image, text) that populates a single search fragment configured in index settings.
    ///
    /// A search fragment is a named slot that defines which media or fields are sent to the embedder.
    ///
    /// An embedder is required; this parameter is incompatible with `vector`.
    ///
    /// POST only.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchMedia>)]
    pub media: Option<serde_json::Value>,
    /// [Personalized search](https://www.meilisearch.com/docs/learn/personalization/making_personalized_search_queries): provide an object with a `userContext` field (a string describing the user, e.g. preferences or behavior).
    ///
    /// Results are then tailored to that profile.
    ///
    /// Personalization must be [enabled](https://www.meilisearch.com/docs/reference/api/experimental-features/configure-experimental-features) (e.g. Cohere key for self-hosted instances).
    #[deserr(default, error = DeserrJsonError<InvalidSearchPersonalize>, default)]
    #[schema(required = false, value_type = Option<Personalize>)]
    pub personalize: Option<Personalize>,
    /// When `true`, runs the query on the whole network (all shards covered, documents deduplicated across remotes).
    ///
    /// When `false` or omitted, the query runs locally.
    ///
    /// **Enterprise Edition only.** This feature is available in the Enterprise Edition.
    ///
    /// It also requires the `network` [experimental feature](http://localhost:3000/reference/api/experimental-features/configure-experimental-features).
    ///
    /// Values: `true` = use the whole network; `false` or omitted = local (default).
    ///
    /// When using the network, the index must exist with compatible settings on all remotes.
    ///
    /// Documents with the same id are assumed identical for deduplication.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchUseNetwork>)]
    pub use_network: Option<bool>,
    /// When true, each document includes a `_rankingScore` between 0.0 and 1.0; a higher value means the document is more relevant.
    ///
    /// See [ranking score](https://www.meilisearch.com/docs/learn/relevancy/ranking_score).
    ///
    /// The `sort` ranking rule does not affect the value of `_rankingScore`.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScore>)]
    pub show_ranking_score: bool,
    /// When true, each document includes `_rankingScoreDetails`, which breaks down the score contribution of each [ranking rule](https://www.meilisearch.com/docs/learn/relevancy/ranking_rules).
    ///
    /// Useful for debugging relevancy.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScoreDetails>)]
    pub show_ranking_score_details: bool,
    /// When true, the response includes a `performanceDetails` object with a timing breakdown of the query processing.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowPerformanceDetails>)]
    pub show_performance_details: bool,
}

impl From<SearchParameters> for SearchQuery {
    fn from(parameters: SearchParameters) -> Self {
        let SearchParameters {
            hybrid,
            limit,
            sort,
            distinct,
            matching_strategy,
            attributes_to_search_on,
            ranking_score_threshold,
        } = parameters;

        SearchQuery {
            q: None,
            offset: DEFAULT_SEARCH_OFFSET(),
            limit: limit.unwrap_or_else(DEFAULT_SEARCH_LIMIT),
            page: None,
            hits_per_page: None,
            attributes_to_retrieve: None,
            attributes_to_crop: None,
            crop_length: DEFAULT_CROP_LENGTH(),
            crop_marker: DEFAULT_CROP_MARKER(),
            attributes_to_highlight: None,
            highlight_pre_tag: DEFAULT_HIGHLIGHT_PRE_TAG(),
            highlight_post_tag: DEFAULT_HIGHLIGHT_POST_TAG(),
            show_matches_position: false,
            filter: None,
            sort,
            distinct,
            facets: None,
            matching_strategy: matching_strategy.map(MatchingStrategy::from).unwrap_or_default(),
            attributes_to_search_on,
            ranking_score_threshold: ranking_score_threshold.map(RankingScoreThreshold::from),
            locales: None,
            hybrid: hybrid.map(|index::HybridQuery { semantic_ratio, embedder }| HybridQuery {
                semantic_ratio: SemanticRatio::try_from(semantic_ratio)
                    .ok()
                    .unwrap_or_else(DEFAULT_SEMANTIC_RATIO),
                embedder,
            }),
            vector: None,
            retrieve_vectors: false,
            media: None,
            personalize: None,
            // TODO: support `use_network` in chat route (not trivial)
            use_network: None,
            show_ranking_score: false,
            show_ranking_score_details: false,
            show_performance_details: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserr, ToSchema, Serialize)]
#[deserr(try_from(f64) = TryFrom::try_from -> InvalidSearchRankingScoreThreshold)]
pub struct RankingScoreThreshold(f64);

impl std::convert::TryFrom<f64> for RankingScoreThreshold {
    type Error = InvalidSearchRankingScoreThreshold;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        // the suggested "fix" is: `!(0.0..=1.0).contains(&f)`` which is allegedly less readable
        #[allow(clippy::manual_range_contains)]
        if f > 1.0 || f < 0.0 {
            Err(InvalidSearchRankingScoreThreshold)
        } else {
            Ok(RankingScoreThreshold(f))
        }
    }
}

impl From<index::RankingScoreThreshold> for RankingScoreThreshold {
    fn from(threshold: index::RankingScoreThreshold) -> Self {
        let threshold = threshold.as_f64();
        assert!((0.0..=1.0).contains(&threshold));
        RankingScoreThreshold(threshold)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserr)]
#[deserr(try_from(f64) = TryFrom::try_from -> InvalidSimilarRankingScoreThreshold)]
pub struct RankingScoreThresholdSimilar(f64);

impl std::convert::TryFrom<f64> for RankingScoreThresholdSimilar {
    type Error = InvalidSimilarRankingScoreThreshold;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        // the suggested "fix" is: `!(0.0..=1.0).contains(&f)`` which is allegedly less readable
        #[allow(clippy::manual_range_contains)]
        if f > 1.0 || f < 0.0 {
            Err(InvalidSimilarRankingScoreThreshold)
        } else {
            Ok(Self(f))
        }
    }
}

// Since this structure is logged A LOT we're going to reduce the number of things it logs to the bare minimum.
// - Only what IS used, we know everything else is set to None so there is no need to print it
// - Re-order the most important field to debug first
impl fmt::Debug for SearchQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            q,
            offset,
            limit,
            page,
            hits_per_page,
            attributes_to_retrieve,
            attributes_to_crop,
            crop_length,
            crop_marker,
            attributes_to_highlight,
            highlight_pre_tag,
            highlight_post_tag,
            show_matches_position,
            filter,
            sort,
            distinct,
            facets,
            matching_strategy,
            attributes_to_search_on,
            ranking_score_threshold,
            locales,
            hybrid,
            vector,
            retrieve_vectors,
            media,
            personalize,
            use_network,
            show_ranking_score,
            show_ranking_score_details,
            show_performance_details,
        } = self;

        let mut debug = f.debug_struct("SearchQuery");

        // First, everything related to the number of documents to retrieve
        debug.field("limit", &limit).field("offset", &offset);
        if let Some(page) = page {
            debug.field("page", &page);
        }
        if let Some(hits_per_page) = hits_per_page {
            debug.field("hits_per_page", &hits_per_page);
        }

        // Then, everything related to the queries
        if let Some(q) = q {
            debug.field("q", &q);
        }
        if *retrieve_vectors {
            debug.field("retrieve_vectors", &retrieve_vectors);
        }
        if let Some(v) = vector {
            if v.len() < 10 {
                debug.field("vector", &v);
            } else {
                debug.field(
                    "vector",
                    &format!("[{}, {}, {}, ... {} dimensions]", v[0], v[1], v[2], v.len()),
                );
            }
        }
        if let Some(media) = media {
            debug.field("media", media);
        }
        if let Some(hybrid) = hybrid {
            debug.field("hybrid", &hybrid);
        }
        if let Some(attributes_to_search_on) = attributes_to_search_on {
            debug.field("attributes_to_search_on", &attributes_to_search_on);
        }
        if let Some(filter) = filter {
            debug.field("filter", &filter);
        }
        if let Some(sort) = sort {
            debug.field("sort", &sort);
        }
        if let Some(distinct) = distinct {
            debug.field("distinct", &distinct);
        }
        if let Some(facets) = facets {
            debug.field("facets", &facets);
        }
        debug.field("matching_strategy", &matching_strategy);

        // Then everything related to the formatting
        debug.field("crop_length", &crop_length);
        if *show_matches_position {
            debug.field("show_matches_position", show_matches_position);
        }
        if *show_ranking_score {
            debug.field("show_ranking_score", show_ranking_score);
        }
        if *show_ranking_score_details {
            debug.field("self.show_ranking_score_details", show_ranking_score_details);
        }
        if *show_performance_details {
            debug.field("show_performance_details", show_performance_details);
        }
        debug.field("crop_length", &crop_length);
        if let Some(facets) = facets {
            debug.field("facets", &facets);
        }
        if let Some(attributes_to_retrieve) = attributes_to_retrieve {
            debug.field("attributes_to_retrieve", &attributes_to_retrieve);
        }
        if let Some(attributes_to_crop) = attributes_to_crop {
            debug.field("attributes_to_crop", &attributes_to_crop);
        }
        if let Some(attributes_to_highlight) = attributes_to_highlight {
            debug.field("attributes_to_highlight", &attributes_to_highlight);
        }
        debug.field("highlight_pre_tag", &highlight_pre_tag);
        debug.field("highlight_post_tag", &highlight_post_tag);
        debug.field("crop_marker", &crop_marker);
        if let Some(ranking_score_threshold) = ranking_score_threshold {
            debug.field("ranking_score_threshold", &ranking_score_threshold);
        }

        if let Some(locales) = locales {
            debug.field("locales", &locales);
        }

        if let Some(personalize) = personalize {
            debug.field("personalize", &personalize);
        }

        if let Some(use_network) = use_network {
            debug.field("use_network", use_network);
        }

        debug.finish()
    }
}

/// Hybrid search configuration for combining keyword and semantic search
#[derive(Debug, Clone, Default, PartialEq, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError<InvalidSearchHybridQuery>, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct HybridQuery {
    /// Balance between keyword search (0.0) and semantic search (1.0). Defaults to 0.5.
    #[deserr(default, error = DeserrJsonError<InvalidSearchSemanticRatio>)]
    #[schema(default, value_type = f32)]
    #[serde(default)]
    pub semantic_ratio: SemanticRatio,
    /// Name of the embedder configured in index settings.
    ///
    /// Used for semantic part of the search.
    #[deserr(error = DeserrJsonError<InvalidSearchEmbedder>)]
    pub embedder: String,
}

#[derive(Clone)]
pub enum SearchKind {
    KeywordOnly,
    SemanticOnly { embedder_name: String, embedder: Arc<Embedder>, quantized: bool },
    Hybrid { embedder_name: String, embedder: Arc<Embedder>, quantized: bool, semantic_ratio: f32 },
}

impl SearchKind {
    pub(crate) fn semantic(
        index_scheduler: &index_scheduler::IndexScheduler,
        index_uid: String,
        index: &Index,
        embedder_name: &str,
        vector_len: Option<usize>,
    ) -> Result<Self, ResponseError> {
        let (embedder_name, embedder, quantized) = Self::embedder(
            index_scheduler,
            index_uid,
            index,
            embedder_name,
            vector_len,
            Route::Search,
        )?;
        Ok(Self::SemanticOnly { embedder_name, embedder, quantized })
    }

    pub(crate) fn hybrid(
        index_scheduler: &index_scheduler::IndexScheduler,
        index_uid: String,
        index: &Index,
        embedder_name: &str,
        semantic_ratio: f32,
        vector_len: Option<usize>,
    ) -> Result<Self, ResponseError> {
        let (embedder_name, embedder, quantized) = Self::embedder(
            index_scheduler,
            index_uid,
            index,
            embedder_name,
            vector_len,
            Route::Search,
        )?;
        Ok(Self::Hybrid { embedder_name, embedder, quantized, semantic_ratio })
    }

    pub(crate) fn embedder(
        index_scheduler: &index_scheduler::IndexScheduler,
        index_uid: String,
        index: &Index,
        embedder_name: &str,
        vector_len: Option<usize>,
        route: Route,
    ) -> Result<(String, Arc<Embedder>, bool), ResponseError> {
        let rtxn = index.read_txn()?;
        let embedder_configs = index.embedding_configs().embedding_configs(&rtxn)?;
        let embedders = index_scheduler.embedders(index_uid, embedder_configs)?;

        let (embedder, quantized) = embedders
            .get(embedder_name)
            .ok_or(match route {
                Route::Search | Route::MultiSearch => {
                    milli::UserError::InvalidSearchEmbedder(embedder_name.to_owned())
                }
                Route::Similar => {
                    milli::UserError::InvalidSimilarEmbedder(embedder_name.to_owned())
                }
            })
            .map(|runtime| (runtime.embedder.clone(), runtime.is_quantized))
            .map_err(milli::Error::from)?;

        if let Some(vector_len) = vector_len {
            if vector_len != embedder.dimensions() {
                return Err(meilisearch_types::milli::Error::UserError(
                    meilisearch_types::milli::UserError::InvalidVectorDimensions {
                        expected: embedder.dimensions(),
                        found: vector_len,
                    },
                )
                .into());
            }
        }

        Ok((embedder_name.to_owned(), embedder, quantized))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserr, Serialize)]
#[deserr(try_from(f32) = TryFrom::try_from -> InvalidSearchSemanticRatio)]
pub struct SemanticRatio(f32);

impl Default for SemanticRatio {
    fn default() -> Self {
        DEFAULT_SEMANTIC_RATIO()
    }
}

impl std::convert::TryFrom<f32> for SemanticRatio {
    type Error = InvalidSearchSemanticRatio;

    fn try_from(f: f32) -> Result<Self, Self::Error> {
        // the suggested "fix" is: `!(0.0..=1.0).contains(&f)`` which is allegedly less readable
        #[allow(clippy::manual_range_contains)]
        if f > 1.0 || f < 0.0 {
            Err(InvalidSearchSemanticRatio)
        } else {
            Ok(SemanticRatio(f))
        }
    }
}

impl std::ops::Deref for SemanticRatio {
    type Target = f32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SearchQuery {
    pub fn is_finite_pagination(&self) -> bool {
        self.page.or(self.hits_per_page).is_some()
    }
}

/// A `SearchQuery` + an index UID and optional FederationOptions.
// This struct contains the fields of `SearchQuery` inline.
// This is because neither deserr nor serde support `flatten` when using `deny_unknown_fields.
// The `From<SearchQueryWithIndex>` implementation ensures both structs remain up to date.
#[derive(Debug, Clone, Serialize, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct SearchQueryWithIndex {
    /// Index unique identifier
    #[deserr(error = DeserrJsonError<InvalidIndexUid>, missing_field_error = DeserrJsonError::missing_index_uid)]
    pub index_uid: IndexUid,
    /// Query string
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    /// Number of documents to skip
    #[deserr(default, error = DeserrJsonError<InvalidSearchOffset>)]
    pub offset: Option<usize>,
    /// Maximum number of documents returned
    #[deserr(default, error = DeserrJsonError<InvalidSearchLimit>)]
    pub limit: Option<usize>,
    /// Request a specific page of results
    #[deserr(default, error = DeserrJsonError<InvalidSearchPage>)]
    pub page: Option<usize>,
    /// Maximum number of documents returned for a page
    #[deserr(default, error = DeserrJsonError<InvalidSearchHitsPerPage>)]
    pub hits_per_page: Option<usize>,
    /// Attributes to display in the returned documents
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    /// Attributes whose values have to be cropped
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToCrop>)]
    pub attributes_to_crop: Option<Vec<String>>,
    /// Maximum length of cropped value in words
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropLength>, default = DEFAULT_CROP_LENGTH())]
    pub crop_length: usize,
    /// String marking crop boundaries
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropMarker>, default = DEFAULT_CROP_MARKER())]
    pub crop_marker: String,
    /// Highlight matching terms contained in an attribute
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToHighlight>)]
    pub attributes_to_highlight: Option<HashSet<String>>,
    /// String inserted at the start of a highlighted term
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPreTag>, default = DEFAULT_HIGHLIGHT_PRE_TAG())]
    pub highlight_pre_tag: String,
    /// String inserted at the end of a highlighted term
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPostTag>, default = DEFAULT_HIGHLIGHT_POST_TAG())]
    pub highlight_post_tag: String,
    /// Return matching terms location
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowMatchesPosition>, default)]
    pub show_matches_position: bool,
    /// Filter queries by an attribute's value
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    /// Sort search results by an attribute's value
    #[deserr(default, error = DeserrJsonError<InvalidSearchSort>)]
    pub sort: Option<Vec<String>>,
    /// Restrict search to documents with unique values of specified
    /// attribute
    #[deserr(default, error = DeserrJsonError<InvalidSearchDistinct>)]
    pub distinct: Option<String>,
    /// Display the count of matches per facet
    #[deserr(default, error = DeserrJsonError<InvalidSearchFacets>)]
    pub facets: Option<Vec<String>>,
    /// Strategy used to match query terms within documents
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>, default)]
    pub matching_strategy: MatchingStrategy,
    /// Restrict search to the specified attributes
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToSearchOn>, default)]
    pub attributes_to_search_on: Option<Vec<String>>,
    /// Exclude results below the specified ranking score
    #[deserr(default, error = DeserrJsonError<InvalidSearchRankingScoreThreshold>, default)]
    #[schema(value_type = Option<f64>)]
    pub ranking_score_threshold: Option<RankingScoreThreshold>,
    /// Languages to use for query tokenization
    #[deserr(default, error = DeserrJsonError<InvalidSearchLocales>, default)]
    pub locales: Option<Vec<Locale>>,
    /// Hybrid search configuration combining keyword and semantic search.
    /// Set `semanticRatio` to balance between keyword matching (0.0) and
    /// semantic similarity (1.0). Requires an embedder to be configured.
    #[deserr(default, error = DeserrJsonError<InvalidSearchHybridQuery>)]
    #[schema(value_type = Option<HybridQuery>)]
    pub hybrid: Option<HybridQuery>,
    /// Search using a custom query vector
    #[deserr(default, error = DeserrJsonError<InvalidSearchVector>)]
    pub vector: Option<Vec<f32>>,
    /// Return document and query vector data
    #[deserr(default, error = DeserrJsonError<InvalidSearchRetrieveVectors>)]
    pub retrieve_vectors: bool,
    /// Perform AI-powered search queries with multimodal content
    #[deserr(default, error = DeserrJsonError<InvalidSearchMedia>)]
    pub media: Option<serde_json::Value>,
    /// Personalize search results
    #[deserr(default, error = DeserrJsonError<InvalidSearchPersonalize>, default)]
    #[serde(skip)]
    pub personalize: Option<Personalize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchUseNetwork>, default)]
    pub use_network: Option<bool>,
    /// Display the global ranking score of a document
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScore>, default)]
    pub show_ranking_score: bool,
    /// Adds a detailed global ranking score field
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScoreDetails>, default)]
    pub show_ranking_score_details: bool,
    /// Adds a detailed performance details field
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowPerformanceDetails>, default)]
    pub show_performance_details: Option<bool>,
    /// Federation options for multi-index search
    #[deserr(default)]
    #[schema(value_type = Option<FederationOptions>)]
    pub federation_options: Option<FederationOptions>,
}

impl SearchQueryWithIndex {
    pub fn has_pagination(&self) -> Option<&'static str> {
        if self.offset.is_some() {
            Some("offset")
        } else if self.limit.is_some() {
            Some("limit")
        } else if self.page.is_some() {
            Some("page")
        } else if self.hits_per_page.is_some() {
            Some("hitsPerPage")
        } else {
            None
        }
    }

    pub fn has_facets(&self) -> Option<&[String]> {
        self.facets.as_deref().filter(|v| !v.is_empty())
    }

    pub fn has_personalize(&self) -> bool {
        self.personalize.is_some()
    }

    pub fn has_remote_and_use_network(&self) -> bool {
        self.federation_options.as_ref().and_then(|opt| opt.remote.as_ref()).is_some()
            && self.use_network == Some(true)
    }

    pub fn has_show_performance_details(&self) -> bool {
        self.show_performance_details.is_some()
    }

    fn has_distinct(&self) -> bool {
        self.distinct.is_some()
    }

    pub fn from_index_query_federation(
        index_uid: IndexUid,
        query: SearchQuery,
        federation_options: Option<FederationOptions>,
    ) -> Self {
        let SearchQuery {
            q,
            offset,
            limit,
            page,
            hits_per_page,
            attributes_to_retrieve,
            attributes_to_crop,
            crop_length,
            crop_marker,
            attributes_to_highlight,
            highlight_pre_tag,
            highlight_post_tag,
            show_matches_position,
            filter,
            sort,
            distinct,
            facets,
            matching_strategy,
            attributes_to_search_on,
            ranking_score_threshold,
            locales,
            hybrid,
            vector,
            retrieve_vectors,
            media,
            personalize,
            use_network,
            show_ranking_score,
            show_ranking_score_details,
            show_performance_details,
        } = query;

        SearchQueryWithIndex {
            index_uid,
            q,
            offset: if offset == DEFAULT_SEARCH_OFFSET() { None } else { Some(offset) },
            limit: if limit == DEFAULT_SEARCH_LIMIT() { None } else { Some(limit) },
            page,
            hits_per_page,
            attributes_to_retrieve,
            attributes_to_crop,
            crop_length,
            crop_marker,
            attributes_to_highlight,
            highlight_pre_tag,
            highlight_post_tag,
            show_matches_position,
            filter,
            sort,
            distinct,
            facets,
            matching_strategy,
            attributes_to_search_on,
            ranking_score_threshold,
            locales,
            hybrid,
            vector,
            retrieve_vectors,
            media,
            personalize,
            use_network,
            show_ranking_score,
            show_ranking_score_details,
            show_performance_details: show_performance_details.then_some(true),
            federation_options,
        }
    }

    pub fn into_index_query_federation(self) -> (IndexUid, SearchQuery, Option<FederationOptions>) {
        let SearchQueryWithIndex {
            index_uid,
            q,
            offset,
            limit,
            page,
            hits_per_page,
            attributes_to_retrieve,
            attributes_to_crop,
            crop_length,
            crop_marker,
            attributes_to_highlight,
            highlight_pre_tag,
            highlight_post_tag,
            show_matches_position,
            filter,
            sort,
            distinct,
            facets,
            matching_strategy,
            attributes_to_search_on,
            ranking_score_threshold,
            locales,
            hybrid,
            vector,
            retrieve_vectors,
            media,
            personalize,
            use_network,
            show_ranking_score,
            show_ranking_score_details,
            show_performance_details,
            federation_options,
        } = self;
        (
            index_uid,
            SearchQuery {
                q,
                offset: offset.unwrap_or(DEFAULT_SEARCH_OFFSET()),
                limit: limit.unwrap_or(DEFAULT_SEARCH_LIMIT()),
                page,
                hits_per_page,
                attributes_to_retrieve,
                attributes_to_crop,
                crop_length,
                crop_marker,
                attributes_to_highlight,
                highlight_pre_tag,
                highlight_post_tag,
                show_matches_position,
                filter,
                sort,
                distinct,
                facets,
                matching_strategy,
                attributes_to_search_on,
                ranking_score_threshold,
                locales,
                hybrid,
                vector,
                retrieve_vectors,
                media,
                personalize,
                use_network,
                show_ranking_score,
                show_ranking_score_details,
                show_performance_details: show_performance_details.unwrap_or_default(),
                // do not use ..Default::default() here,
                // rather add any missing field from `SearchQuery` to `SearchQueryWithIndex`
            },
            federation_options,
        )
    }
}

/// Request body for similar document search
#[derive(Debug, Clone, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct SimilarQuery {
    /// Document ID to find similar documents for
    #[deserr(error = DeserrJsonError<InvalidSimilarId>)]
    #[schema(required = true, value_type = String)]
    pub id: serde_json::Value,
    /// Number of documents to skip
    #[schema(required = false)]
    #[deserr(default = DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSimilarOffset>)]
    pub offset: usize,
    /// Maximum number of documents returned
    #[schema(required = false)]
    #[deserr(default = DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSimilarLimit>)]
    pub limit: usize,
    /// Filter queries by an attribute's value
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSimilarFilter>)]
    pub filter: Option<Value>,
    /// Name of the embedder to use for semantic similarity
    #[schema(required = true)]
    #[deserr(error = DeserrJsonError<InvalidSimilarEmbedder>)]
    pub embedder: String,
    /// Attributes to display in the returned documents
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSimilarAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    /// Return document vector data
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSimilarRetrieveVectors>)]
    pub retrieve_vectors: bool,
    /// Display the global ranking score of a document
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSimilarShowRankingScore>, default)]
    pub show_ranking_score: bool,
    /// Adds a detailed global ranking score field
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSimilarShowRankingScoreDetails>, default)]
    pub show_ranking_score_details: bool,
    /// Adds a detailed performance details field
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidSimilarShowPerformanceDetails>, default)]
    pub show_performance_details: bool,
    /// Excludes results with low ranking scores
    #[deserr(default, error = DeserrJsonError<InvalidSimilarRankingScoreThreshold>, default)]
    #[schema(required = false, value_type = f64)]
    pub ranking_score_threshold: Option<RankingScoreThresholdSimilar>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExternalDocumentId(String);

impl AsRef<str> for ExternalDocumentId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl ExternalDocumentId {
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl TryFrom<String> for ExternalDocumentId {
    type Error = milli::UserError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        serde_json::Value::String(value).try_into()
    }
}

impl TryFrom<Value> for ExternalDocumentId {
    type Error = milli::UserError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Ok(Self(milli::documents::validate_document_id_value(value)?))
    }
}

/// Strategy used to match query terms within documents
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Deserr, ToSchema, Serialize)]
#[deserr(rename_all = camelCase)]
#[serde(rename_all = "camelCase")]
pub enum MatchingStrategy {
    /// Remove query words from last to first
    #[default]
    Last,
    /// All query words are mandatory
    All,
    /// Remove query words from the most frequent to the least
    Frequency,
}

impl From<MatchingStrategy> for TermsMatchingStrategy {
    fn from(other: MatchingStrategy) -> Self {
        match other {
            MatchingStrategy::Last => Self::Last,
            MatchingStrategy::All => Self::All,
            MatchingStrategy::Frequency => Self::Frequency,
        }
    }
}

impl From<index::MatchingStrategy> for MatchingStrategy {
    fn from(other: index::MatchingStrategy) -> Self {
        match other {
            index::MatchingStrategy::Last => Self::Last,
            index::MatchingStrategy::All => Self::All,
            index::MatchingStrategy::Frequency => Self::Frequency,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserr)]
#[deserr(rename_all = camelCase)]
pub enum FacetValuesSort {
    /// Facet values are sorted in alphabetical order, ascending from A to
    /// Z.
    #[default]
    Alpha,
    /// Facet values are sorted by decreasing count.
    /// The count is the number of records containing this facet value in
    /// the results of the query.
    Count,
}

impl From<FacetValuesSort> for OrderBy {
    fn from(val: FacetValuesSort) -> Self {
        match val {
            FacetValuesSort::Alpha => OrderBy::Lexicographic,
            FacetValuesSort::Count => OrderBy::Count,
        }
    }
}

/// A single search result hit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct SearchHit {
    /// Document fields as stored in the index.
    ///
    /// According to `attributesToRetrieve`.
    #[serde(flatten)]
    #[schema(additional_properties, inline, value_type = HashMap<String, Value>)]
    pub document: Document,

    /// Extra document fields for internal use.
    ///
    /// They are never de/serialized and must be manually
    /// mounted to/unmounted from `_federation` in a federated context.
    ///
    /// - Unmounting: See `SearchByIndex::execute`
    /// - Mounting: See `MergedSearchHit::remote`
    #[serde(default, skip)]
    #[schema(ignore)]
    pub extra_document: Document,

    /// Document with highlighted and cropped attributes.
    ///
    /// Present when `attributesToHighlight` or `attributesToCrop` was set.
    #[serde(default, rename = "_formatted", skip_serializing_if = "Document::is_empty")]
    #[schema(additional_properties, value_type = HashMap<String, Value>)]
    pub formatted: Document,
    /// Byte offsets (`start`, `length`) of each matched term per attribute.
    ///
    /// Present when `showMatchesPosition` was true.
    #[serde(default, rename = "_matchesPosition", skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<BTreeMap<String, Vec<MatchBounds>>>)]
    pub matches_position: Option<MatchesPosition>,
    /// Global [ranking score](https://www.meilisearch.com/docs/learn/relevancy/ranking_score) from 0.0 to 1.0.
    ///
    /// Present when `showRankingScore` was true.
    #[serde(default, rename = "_rankingScore", skip_serializing_if = "Option::is_none")]
    pub ranking_score: Option<f64>,
    /// Per-rule score breakdown (words, typo, proximity, etc.).
    ///
    /// Present when `showRankingScoreDetails` was true.
    #[serde(default, rename = "_rankingScoreDetails", skip_serializing_if = "Option::is_none")]
    pub ranking_score_details: Option<serde_json::Map<String, serde_json::Value>>,
}

impl SearchHit {
    fn facet_values<F>(&self, field_name: &str, mut visit: F)
    where
        F: FnMut(FacetValue),
    {
        permissive_json_pointer::visit_leaf_values(&self.document, field_name, &mut |value| {
            for value in FacetValue::from_value(value) {
                visit(value);
            }
        });
        permissive_json_pointer::visit_leaf_values(
            &self.extra_document,
            field_name,
            &mut |value| {
                for value in FacetValue::from_value(value) {
                    visit(value);
                }
            },
        );
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FacetValue {
    Normalized(String),
    Number(serde_json::Number),
}

impl FacetValue {
    pub fn from_value(field: &serde_json::Value) -> impl Iterator<Item = FacetValue> + '_ {
        match field {
            Value::Array(values) => {
                either::Either::Left(values.iter().flat_map(Self::from_leaf_value))
            }
            value => either::Either::Right(Self::from_leaf_value(value).into_iter()),
        }
    }

    fn from_leaf_value(field: &serde_json::Value) -> Option<FacetValue> {
        match field {
            Value::Bool(b) => Some(FacetValue::Normalized(b.to_string())),
            Value::Number(number) => Some(FacetValue::Number(number.clone())),
            Value::String(s) => {
                let normalized = milli::normalize_facet(s);
                Some(FacetValue::Normalized(normalized))
            }
            _ => None,
        }
    }
}

/// Metadata about a search query (included when requested via header).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct SearchMetadata {
    /// Unique identifier for the query.
    pub query_uid: Uuid,
    /// UID of the index that was searched.
    pub index_uid: String,
    /// [Primary key](https://www.meilisearch.com/docs/learn/getting_started/primary_key) of the index.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<String>,
    /// Remote that processed the query (federated search only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
}

/// Search response containing matching documents and metadata.
#[derive(Serialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct SearchResult {
    /// Matching documents.
    ///
    /// Each hit contains document fields and, when requested, `_formatted`, `_matchesPosition`, `_rankingScore`, `_rankingScoreDetails`, `_geoDistance`.
    pub hits: Vec<SearchHit>,
    /// Query string that produced this response.
    pub query: String,
    /// Query embedding used for the search.
    ///
    /// Present when vector or hybrid search was used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_vector: Option<Vec<f32>>,
    /// Time taken to process the query, in milliseconds.
    pub processing_time_ms: u128,
    /// Pagination info.
    ///
    /// Either offset/limit with `estimatedTotalHits`, or page/hitsPerPage with `totalHits` and `totalPages`.
    #[serde(flatten)]
    pub hits_info: HitsInfo,
    /// Count of matching documents per facet value for each requested facet.
    ///
    /// Present when `facets` was set.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<BTreeMap<String, Value>>)]
    pub facet_distribution: Option<BTreeMap<String, IndexMap<String, u64>>>,
    /// Minimum and maximum numeric values per facet.
    ///
    /// Present for numeric facets when `facets` was set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_stats: Option<BTreeMap<String, FacetStats>>,
    /// UUID v7 identifying this search request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_uid: Option<Uuid>,
    /// Query and index metadata.
    ///
    /// Present when requested via the `Meili-Include-Metadata` header.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<SearchMetadata>,
    /// Timing breakdown per processing step.
    ///
    /// Present when `showPerformanceDetails` was true.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Value>)]
    pub performance_details: Option<IndexMap<String, String>>,

    /// Errors from remote shards. Federated search only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_errors: Option<BTreeMap<String, ResponseError>>,

    /// Exhaustive number of semantic search matches.
    ///
    /// AI-powered (hybrid/semantic) searches only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_hit_count: Option<u32>,

    // These fields are only used for analytics purposes
    #[serde(skip)]
    pub degraded: bool,
    #[serde(skip)]
    pub used_negative_operator: bool,
}

impl fmt::Debug for SearchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let SearchResult {
            hits,
            query,
            query_vector,
            processing_time_ms,
            hits_info,
            facet_distribution,
            facet_stats,
            request_uid,
            metadata,
            semantic_hit_count,
            degraded,
            used_negative_operator,
            remote_errors,
            performance_details: _, // not part of the debug output because it's an Option and is always displayed in a dedicated log.
        } = self;

        let mut debug = f.debug_struct("SearchResult");
        // The most important thing when looking at a search result is the time it took to process
        debug.field("processing_time_ms", &processing_time_ms);
        debug.field("hits", &format!("[{} hits returned]", hits.len()));
        debug.field("query", &query);
        if query_vector.is_some() {
            debug.field("query_vector", &"[...]");
        }
        debug.field("hits_info", &hits_info);
        if *used_negative_operator {
            debug.field("used_negative_operator", used_negative_operator);
        }
        if *degraded {
            debug.field("degraded", degraded);
        }
        if let Some(facet_distribution) = facet_distribution {
            debug.field("facet_distribution", &facet_distribution);
        }
        if let Some(facet_stats) = facet_stats {
            debug.field("facet_stats", &facet_stats);
        }
        if let Some(semantic_hit_count) = semantic_hit_count {
            debug.field("semantic_hit_count", &semantic_hit_count);
        }
        if let Some(request_uid) = request_uid {
            debug.field("request_uid", &request_uid);
        }
        if let Some(metadata) = metadata {
            debug.field("metadata", &metadata);
        }
        if let Some(remote_errors) = remote_errors {
            debug.field("remote_errors", &remote_errors);
        }

        debug.finish()
    }
}

/// Response containing similar documents
#[derive(Serialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SimilarResult {
    /// Results of the query
    pub hits: Vec<SearchHit>,
    /// Document ID that was used as reference
    pub id: String,
    /// Processing time of the query in milliseconds
    pub processing_time_ms: u128,
    /// Pagination information
    #[serde(flatten)]
    pub hits_info: HitsInfo,
    /// Performance details of the query
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Value>)]
    pub performance_details: Option<IndexMap<String, String>>,
}

/// Search result with index identifier for multi-search responses
#[derive(Serialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct SearchResultWithIndex {
    /// Identifier of the queried index
    pub index_uid: String,
    /// Search results for this index
    #[serde(flatten)]
    pub result: SearchResult,
}

/// Pagination information for search results.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, ToSchema)]
#[serde(untagged)]
pub enum HitsInfo {
    /// Page-based pagination with exact totals.
    ///
    /// Used when `page` or `hitsPerPage` was set in the request.
    #[serde(rename_all = "camelCase")]
    #[schema(rename_all = "camelCase")]
    Pagination {
        /// Number of results per page.
        hits_per_page: usize,
        /// Current page index (1-based).
        page: usize,
        /// Exhaustive total number of result pages.
        total_pages: usize,
        /// Exhaustive total number of matching documents.
        total_hits: usize,
    },
    /// Offset-based pagination with estimated total.
    ///
    /// Used when only `offset` and `limit` were set.
    #[serde(rename_all = "camelCase")]
    #[schema(rename_all = "camelCase")]
    OffsetLimit {
        /// Maximum number of documents returned.
        limit: usize,
        /// Number of documents skipped.
        offset: usize,
        /// Estimated total number of matches (not exhaustive).
        ///
        /// Prioritizes relevancy and performance.
        estimated_total_hits: usize,
    },
}

/// Minimum and maximum numeric values for a facet.
///
/// Present when the facet attribute has numeric values among matching documents.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, ToSchema)]
pub struct FacetStats {
    /// Lowest numeric value for this facet among matching documents.
    pub min: f64,
    /// Highest numeric value for this facet among matching documents.
    pub max: f64,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FacetSearchResult {
    pub facet_hits: Vec<FacetValueHit>,
    pub facet_query: Option<String>,
    pub processing_time_ms: u128,
}

/// Incorporate search rules in search query
pub fn add_search_rules(filter: &mut Option<Value>, rules: IndexSearchRules) {
    *filter = fuse_filters(filter.take(), rules.filter);
}

pub fn fuse_filters(left: Option<Value>, right: Option<Value>) -> Option<Value> {
    match (left, right) {
        (None, right) => right,
        (left, None) => left,
        (Some(left), Some(right)) => {
            let left = match left {
                Value::Array(filter) => filter,
                filter => vec![filter],
            };
            let right = match right {
                Value::Array(rules_filter) => rules_filter,
                rules_filter => vec![rules_filter],
            };

            Some(Value::Array([left, right].concat()))
        }
    }
}

pub fn prepare_search<'t>(
    index: &'t Index,
    rtxn: &'t RoTxn,
    query: &'t SearchQuery,
    search_kind: &SearchKind,
    deadline: Deadline,
    features: RoFeatures,
    progress: &'t Progress,
) -> Result<(milli::Search<'t>, bool, usize, usize), ResponseError> {
    if query.media.is_some() {
        features.check_multimodal("passing `media` in a search query")?;
    }
    let mut search = index.search(rtxn, progress);
    search.deadline(deadline);
    if let Some(ranking_score_threshold) = query.ranking_score_threshold {
        search.ranking_score_threshold(ranking_score_threshold.0);
    }

    if let Some(distinct) = &query.distinct {
        search.distinct(distinct.clone());
    }

    match search_kind {
        SearchKind::KeywordOnly => {
            if let Some(q) = &query.q {
                search.query(q);
            }
        }
        SearchKind::SemanticOnly { embedder_name, embedder, quantized } => {
            let vector = match query.vector.clone() {
                Some(vector) => vector,
                None => {
                    let _step = progress.update_progress_scoped(SearchStep::Embed);
                    let span = tracing::trace_span!(target: "search::vector", "embed_one");
                    let _entered = span.enter();

                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

                    let q = query.q.as_deref();
                    let media = query.media.as_ref();

                    let search_query = match (q, media) {
                        (Some(text), None) => milli::vector::SearchQuery::Text(text),
                        (q, media) => milli::vector::SearchQuery::Media { q, media },
                    };

                    embedder
                        .embed_search(search_query, Some(deadline))
                        .map_err(milli::vector::Error::from)
                        .map_err(milli::Error::from)?
                }
            };
            search.semantic(
                embedder_name.clone(),
                embedder.clone(),
                *quantized,
                Some(vector),
                query.media.clone(),
            );
        }
        SearchKind::Hybrid { embedder_name, embedder, quantized, semantic_ratio: _ } => {
            if let Some(q) = &query.q {
                search.query(q);
            }
            // will be embedded in hybrid search if necessary
            search.semantic(
                embedder_name.clone(),
                embedder.clone(),
                *quantized,
                query.vector.clone(),
                query.media.clone(),
            );
        }
    }

    if let Some(ref searchable) = query.attributes_to_search_on {
        search.searchable_attributes(searchable);
    }

    let is_finite_pagination = query.is_finite_pagination();
    search.terms_matching_strategy(query.matching_strategy.into());

    let max_total_hits = index
        .pagination_max_total_hits(rtxn)
        .map_err(milli::Error::from)?
        .map(|x| x as usize)
        .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS);

    search.retrieve_vectors(query.retrieve_vectors);
    search.exhaustive_number_hits(is_finite_pagination);
    search.max_total_hits(Some(max_total_hits));
    search.scoring_strategy(
        if query.show_ranking_score
            || query.show_ranking_score_details
            || query.ranking_score_threshold.is_some()
        {
            ScoringStrategy::Detailed
        } else {
            ScoringStrategy::Skip
        },
    );

    // compute the offset on the limit depending on the pagination mode.
    let (offset, limit) = if is_finite_pagination {
        let limit = query.hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
        let page = query.page.unwrap_or(1);

        // page 0 gives a limit of 0 forcing Meilisearch to return no document.
        page.checked_sub(1).map_or((0, 0), |p| (limit * p, limit))
    } else {
        (query.offset, query.limit)
    };

    // Make sure that a user can't get more documents than the hard limit,
    // we align that on the offset too.
    let offset = min(offset, max_total_hits);
    let limit = min(limit, max_total_hits.saturating_sub(offset));

    search.offset(offset);
    search.limit(limit);

    if let Some(ref filter) = query.filter {
        if let Some(facets) = parse_filter(filter, Code::InvalidSearchFilter, features)? {
            search.filter(facets);
        }
    }

    if let Some(ref sort) = query.sort {
        let sort = match sort.iter().map(|s| AscDesc::from_str(s)).collect() {
            Ok(sorts) => sorts,
            Err(asc_desc_error) => {
                return Err(SortError::from(asc_desc_error).into_search_error().into());
            }
        };

        search.sort_criteria(sort);
    }

    if let Some(ref locales) = query.locales {
        search.locales(locales.iter().copied().map(Into::into).collect());
    }

    Ok((search, is_finite_pagination, max_total_hits, offset))
}

pub struct SearchParams {
    pub index_uid: String,
    pub query: SearchQuery,
    pub search_kind: SearchKind,
    pub retrieve_vectors: RetrieveVectors,
    pub features: RoFeatures,
    pub request_uid: Uuid,
    pub include_metadata: bool,
}

pub fn perform_search(
    params: SearchParams,
    index_scheduler: &IndexScheduler,
    index: &Index,
    progress: &Progress,
) -> Result<(SearchResult, Deadline), ResponseError> {
    let SearchParams {
        index_uid,
        query,
        search_kind,
        retrieve_vectors,
        features,
        request_uid,
        include_metadata,
    } = params;
    let before_search = Instant::now();
    let index_uid_for_metadata = index_uid.clone();
    let rtxn = index.read_txn()?;
    let deadline = index.search_deadline(&rtxn)?;

    let (search, is_finite_pagination, max_total_hits, offset) =
        prepare_search(index, &rtxn, &query, &search_kind, deadline.clone(), features, progress)?;

    let (
        milli::SearchResult {
            documents_ids,
            matching_words,
            candidates,
            document_scores,
            degraded,
            used_negative_operator,
            query_vector,
        },
        semantic_hit_count,
    ) = search_from_kind(index_uid.clone(), search_kind, search)?;

    let metadata = if include_metadata {
        let query_uid = Uuid::now_v7();
        let primary_key = index.primary_key(&rtxn)?.map(|pk| pk.to_string());
        Some(SearchMetadata {
            query_uid,
            index_uid: index_uid_for_metadata,
            primary_key,
            remote: None, // Local searches don't have a remote
        })
    } else {
        None
    };

    let SearchQuery {
        q,
        offset: _,
        limit,
        page,
        hits_per_page,
        attributes_to_retrieve,
        attributes_to_crop,
        crop_length,
        crop_marker,
        attributes_to_highlight,
        highlight_pre_tag,
        highlight_post_tag,
        show_matches_position,
        filter: _,
        sort,
        distinct: _,
        facets,
        matching_strategy: _,
        attributes_to_search_on: _,
        ranking_score_threshold: _,
        locales,
        hybrid: _,
        vector: _,
        retrieve_vectors: _,
        media: _,
        personalize: _,
        use_network: _,
        show_ranking_score,
        show_ranking_score_details,
        show_performance_details: _,
    } = query;

    let format = AttributesFormat {
        attributes_to_retrieve,
        extra_attributes_to_retrieve: Default::default(),
        retrieve_vectors,
        attributes_to_highlight,
        attributes_to_crop,
        crop_length,
        crop_marker,
        highlight_pre_tag,
        highlight_post_tag,
        show_matches_position,
        sort,
        show_ranking_score,
        show_ranking_score_details,
        locales: locales.map(|l| l.iter().copied().map(Into::into).collect()),
    };

    let mut documents = make_hits(
        index,
        &rtxn,
        format,
        matching_words,
        documents_ids.iter().copied().zip(document_scores.iter()),
        progress,
    )?;

    // Document join: hydrate documents based on the foreign keys
    if features.runtime_features().foreign_keys {
        let foreign_keys = index.foreign_keys(&rtxn)?;
        hydrate_documents(&mut documents, &foreign_keys, index_scheduler)?;
    }

    let number_of_hits = min(candidates.len() as usize, max_total_hits);
    let hits_info = if is_finite_pagination {
        let hits_per_page = hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
        // If hit_per_page is 0, then pages can't be computed and so we respond 0.
        let total_pages = (number_of_hits + hits_per_page.saturating_sub(1))
            .checked_div(hits_per_page)
            .unwrap_or(0);

        HitsInfo::Pagination {
            hits_per_page,
            page: page.unwrap_or(1),
            total_pages,
            total_hits: number_of_hits,
        }
    } else {
        HitsInfo::OffsetLimit { limit, offset, estimated_total_hits: number_of_hits }
    };

    let (facet_distribution, facet_stats) = facets
        .map(move |facets| {
            let _step = progress.update_progress_scoped(SearchStep::FacetDistribution);
            compute_facet_distribution_stats(&facets, index, &rtxn, candidates, Route::Search)
        })
        .transpose()?
        .map(|ComputedFacets { distribution, stats }| (distribution, stats))
        .unzip();

    let performance_details =
        query.show_performance_details.then(|| progress.accumulated_durations());
    let result = SearchResult {
        hits: documents,
        hits_info,
        query: q.unwrap_or_default(),
        query_vector,
        processing_time_ms: before_search.elapsed().as_millis(),
        facet_distribution,
        facet_stats,
        degraded,
        used_negative_operator,
        semantic_hit_count,
        request_uid: Some(request_uid),
        metadata,
        remote_errors: None,
        performance_details,
    };
    Ok((result, deadline))
}

/// Computed facet data from a search
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ComputedFacets {
    /// Count of documents for each facet value
    #[schema(value_type = BTreeMap<String, BTreeMap<String, u64>>)]
    pub distribution: BTreeMap<String, IndexMap<String, u64>>,
    /// Numeric statistics for each facet
    pub stats: BTreeMap<String, FacetStats>,
}

impl ComputedFacets {
    pub fn remove_hits(&mut self, hits: &[SearchHit]) {
        if hits.is_empty() {
            return;
        }
        for (field_name, distribution) in &mut self.distribution {
            let normalized_to_original: BTreeMap<_, _> = distribution
                .keys()
                .enumerate()
                .filter_map(|(index, facet_value)| {
                    let normalized = milli::normalize_facet(facet_value);
                    if normalized == facet_value.as_str() {
                        None
                    } else {
                        Some((normalized, index))
                    }
                })
                .collect();

            let mut must_remove = false;

            for hit in hits {
                hit.facet_values(field_name, |value| {
                    let count = match value {
                        FacetValue::Normalized(s) => {
                            if let Some(original) = normalized_to_original.get(&s) {
                                distribution.get_index_mut(*original).map(|(_, v)| v)
                            } else {
                                distribution.get_mut(&s)
                            }
                        }
                        FacetValue::Number(number) => distribution.get_mut(&number.to_string()),
                    };

                    let Some(count) = count else {
                        return;
                    };

                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        must_remove = true;
                    }
                });
            }

            if must_remove {
                distribution.retain(|_, v| *v != 0);
            }
        }
    }
}

pub enum Route {
    Search,
    MultiSearch,
    Similar,
}

fn compute_facet_distribution_stats<S: AsRef<str>>(
    facets: &[S],
    index: &Index,
    rtxn: &RoTxn,
    candidates: roaring::RoaringBitmap,
    route: Route,
) -> Result<ComputedFacets, ResponseError> {
    let mut facet_distribution = index.facets_distribution(rtxn);

    let max_values_by_facet = index
        .max_values_per_facet(rtxn)
        .map_err(milli::Error::from)?
        .map(|x| x as usize)
        .unwrap_or(DEFAULT_VALUES_PER_FACET);

    facet_distribution.max_values_per_facet(max_values_by_facet);

    let sort_facet_values_by = index.sort_facet_values_by(rtxn).map_err(milli::Error::from)?;

    // add specific facet if there is no placeholder
    if facets.iter().all(|f| f.as_ref() != "*") {
        let fields: Vec<_> =
            facets.iter().map(|n| (n, sort_facet_values_by.get(n.as_ref()))).collect();
        facet_distribution.facets(fields);
    }

    let distribution = facet_distribution
        .candidates(candidates)
        .default_order_by(sort_facet_values_by.get("*"))
        .execute()
        .map_err(|error| match (error, route) {
            (
                error @ milli::Error::UserError(milli::UserError::InvalidFacetsDistribution {
                    ..
                }),
                Route::MultiSearch,
            ) => ResponseError::from_msg(error.to_string(), Code::InvalidMultiSearchFacets),
            (error, _) => error.into(),
        })?;
    let stats = facet_distribution.compute_stats()?;
    let stats = stats.into_iter().map(|(k, (min, max))| (k, FacetStats { min, max })).collect();
    Ok(ComputedFacets { distribution, stats })
}

pub fn search_from_kind(
    index_uid: String,
    search_kind: SearchKind,
    search: milli::Search<'_>,
) -> Result<(milli::SearchResult, Option<u32>), MeilisearchHttpError> {
    let (milli_result, semantic_hit_count) = match &search_kind {
        SearchKind::KeywordOnly => {
            let results = search
                .execute()
                .map_err(|e| MeilisearchHttpError::from_milli(e, Some(index_uid.to_string())))?;
            (results, None)
        }
        SearchKind::SemanticOnly { .. } => {
            let results = search
                .execute()
                .map_err(|e| MeilisearchHttpError::from_milli(e, Some(index_uid.to_string())))?;
            let semantic_hit_count = results.document_scores.len() as u32;
            (results, Some(semantic_hit_count))
        }
        SearchKind::Hybrid { semantic_ratio, .. } => search
            .execute_hybrid(*semantic_ratio)
            .map_err(|e| MeilisearchHttpError::from_milli(e, Some(index_uid)))?,
    };
    Ok((milli_result, semantic_hit_count))
}

struct AttributesFormat {
    /// Subset of the index's `displayedAttributes`.
    ///
    /// - If `None`, all `displayedAttributes` will be returned.
    /// - Fields in `attributes_to_retrieve` that are not in `displayedAttributes` will not be retrieved.
    attributes_to_retrieve: Option<BTreeSet<String>>,

    /// Extra set of fields that will be stored in `extra_attributes` when making hits.
    ///
    /// This allows recovering fields that should not be shown to the end-user but that Meilisearch needs for e.g. distinct in
    /// federated contexts.
    ///
    /// - If empty, `hit.extra_attributes` will not be populated.
    /// - Fields in `extra_attributes_to_retrieve` will be retrieved in `hit.extra_attributes` **even** if missing in `displayedAttributes`.
    /// - Fields in `attributes_to_retrieve` that are in `displayedAttributes` will **not** be collected in `extra_attributes`.
    /// - `_vectors` cannot be retrieved in this way.
    ///
    /// Due to these properties, it is possible to populate `extra_attributes_to_retrieve` without checking the `displayedAttributes`.
    extra_attributes_to_retrieve: BTreeSet<String>,
    retrieve_vectors: RetrieveVectors,
    attributes_to_highlight: Option<HashSet<String>>,
    attributes_to_crop: Option<Vec<String>>,
    crop_length: usize,
    crop_marker: String,
    highlight_pre_tag: String,
    highlight_post_tag: String,
    show_matches_position: bool,
    sort: Option<Vec<String>>,
    show_ranking_score: bool,
    show_ranking_score_details: bool,
    locales: Option<Vec<Language>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrieveVectors {
    /// Remove the `_vectors` field
    ///
    /// this is the behavior when the `retrieveVectors` is `false`
    Hide,
    /// Retrieve vectors from the DB and merge them into the `_vectors`
    /// field
    ///
    /// this is the behavior when the `retrieveVectors` is `true`
    Retrieve,
}

impl RetrieveVectors {
    pub fn new(retrieve_vector: bool) -> Self {
        if retrieve_vector {
            Self::Retrieve
        } else {
            Self::Hide
        }
    }
}

struct HitMaker<'a> {
    index: &'a Index,
    rtxn: &'a RoTxn<'a>,
    fields_ids_map: FieldsIdsMap,
    displayed_ids: BTreeSet<FieldId>,
    vectors_fid: Option<FieldId>,
    retrieve_vectors: RetrieveVectors,
    to_retrieve_ids: BTreeSet<FieldId>,
    extra_ids: Vec<String>,
    formatter_builder: MatcherBuilder<'a>,
    formatted_options: BTreeMap<FieldId, FormatOptions>,
    show_ranking_score: bool,
    show_ranking_score_details: bool,
    sort: Option<Vec<String>>,
    show_matches_position: bool,
    locales: Option<Vec<Language>>,
    attribute_state: AttributeState,
}

impl<'a> HitMaker<'a> {
    pub fn tokenizer<'b>(
        dictionary: Option<&'b [&'b str]>,
        separators: Option<&'b [&'b str]>,
    ) -> milli::tokenizer::Tokenizer<'b> {
        let mut tokenizer_builder = TokenizerBuilder::default();
        tokenizer_builder.create_char_map(true);

        if let Some(separators) = separators {
            tokenizer_builder.separators(separators);
        }

        if let Some(dictionary) = dictionary {
            tokenizer_builder.words_dict(dictionary);
        }

        tokenizer_builder.into_tokenizer()
    }

    pub fn formatter_builder(
        matching_words: milli::MatchingWords,
        tokenizer: milli::tokenizer::Tokenizer<'_>,
    ) -> MatcherBuilder<'_> {
        let formatter_builder = MatcherBuilder::new(matching_words, tokenizer);

        formatter_builder
    }

    pub fn new(
        index: &'a Index,
        rtxn: &'a RoTxn<'a>,
        format: AttributesFormat,
        mut formatter_builder: MatcherBuilder<'a>,
    ) -> milli::Result<Self> {
        formatter_builder.crop_marker(format.crop_marker);
        formatter_builder.highlight_prefix(format.highlight_pre_tag);
        formatter_builder.highlight_suffix(format.highlight_post_tag);

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let displayed_ids = index
            .displayed_fields_ids(rtxn)?
            .map(|fields| fields.into_iter().collect::<BTreeSet<_>>());

        let vectors_fid = fields_ids_map.id(milli::constants::RESERVED_VECTORS_FIELD_NAME);

        let vectors_is_hidden = match (&displayed_ids, vectors_fid) {
            // displayed_ids is a wildcard, so `_vectors` can be displayed regardless of its fid
            (None, _) => false,
            // vectors has no fid, so check its explicit name
            (Some(_), None) => {
                // unwrap as otherwise we'd go to the first one
                let displayed_names = index.displayed_fields(rtxn)?.unwrap();
                !displayed_names.contains(&milli::constants::RESERVED_VECTORS_FIELD_NAME)
            }
            // displayed_ids is a finit list, so hide if `_vectors` is not part of it
            (Some(map), Some(vectors_fid)) => map.contains(&vectors_fid),
        };

        let displayed_ids =
            displayed_ids.unwrap_or_else(|| fields_ids_map.iter().map(|(id, _)| id).collect());

        let retrieve_vectors = if let RetrieveVectors::Retrieve = format.retrieve_vectors {
            if vectors_is_hidden {
                RetrieveVectors::Hide
            } else {
                RetrieveVectors::Retrieve
            }
        } else {
            format.retrieve_vectors
        };

        let fids = |attrs: &BTreeSet<String>| {
            let mut ids = BTreeSet::new();
            for attr in attrs {
                if attr == "*" {
                    ids.clone_from(&displayed_ids);
                    break;
                }

                if let Some(id) = fields_ids_map.id(attr) {
                    ids.insert(id);
                }
            }
            ids
        };
        let to_retrieve_ids: BTreeSet<_> = format
            .attributes_to_retrieve
            .as_ref()
            .map(fids)
            .unwrap_or_else(|| displayed_ids.clone())
            .intersection(&displayed_ids)
            .copied()
            .collect();

        let fids_no_wildcard = |attrs: &BTreeSet<String>| {
            let mut ids = BTreeSet::new();
            for attr in attrs {
                if let Some(id) = fields_ids_map.id(attr) {
                    ids.insert(id);
                }
            }
            ids
        };

        let extra_ids: Vec<_> = fids_no_wildcard(&format.extra_attributes_to_retrieve)
            .difference(&to_retrieve_ids)
            .copied()
            .filter_map(|fid| fields_ids_map.name(fid).map(|field| field.to_owned()))
            .collect();

        let attr_to_highlight = format.attributes_to_highlight.unwrap_or_default();
        let attr_to_crop = format.attributes_to_crop.unwrap_or_default();
        let formatted_options = compute_formatted_options(
            &attr_to_highlight,
            &attr_to_crop,
            format.crop_length,
            &to_retrieve_ids,
            &fields_ids_map,
            &displayed_ids,
        );

        let attribute_state = AttributeState::from_criteria(index.criteria(rtxn)?);

        Ok(Self {
            index,
            rtxn,
            fields_ids_map,
            displayed_ids,
            extra_ids,
            vectors_fid,
            retrieve_vectors,
            to_retrieve_ids,
            formatter_builder,
            formatted_options,
            show_ranking_score: format.show_ranking_score,
            show_ranking_score_details: format.show_ranking_score_details,
            show_matches_position: format.show_matches_position,
            sort: format.sort,
            locales: format.locales,
            attribute_state,
        })
    }

    pub fn make_hit(
        &self,
        id: u32,
        score: &[ScoreDetails],
        progress: &Progress,
    ) -> milli::Result<SearchHit> {
        let _step = progress.update_progress_scoped(SearchStep::Format);
        let obkv = self.index.document(self.rtxn, id)?;

        let add_vectors_fid =
            self.vectors_fid.filter(|_fid| self.retrieve_vectors == RetrieveVectors::Retrieve);

        // Select the attributes to retrieve
        // Note that to_retrieve_ids is already an intersection with the displayed attributes
        let attributes_to_retrieve: Vec<_> = self
            .to_retrieve_ids
            .iter()
            // skip the vectors_fid if RetrieveVectors::Hide
            .filter(|fid| match self.vectors_fid {
                Some(vectors_fid) => {
                    !(self.retrieve_vectors == RetrieveVectors::Hide && **fid == vectors_fid)
                }
                None => true,
            })
            // need to retrieve the existing `_vectors` field if the `RetrieveVectors::Retrieve`
            .chain(add_vectors_fid.iter())
            // Convert the field into their names
            .map(|&fid| self.fields_ids_map.name(fid).expect("Missing field name"))
            .collect();

        // Generate a document with all the attributes to retrieve
        let mut document = make_document(obkv, &self.fields_ids_map, &attributes_to_retrieve)?;

        let extra_document = self
            .extra_ids
            .is_empty()
            .not()
            .then(|| make_document(obkv, &self.fields_ids_map, &self.extra_ids))
            .transpose()?
            .unwrap_or_default();

        if self.retrieve_vectors == RetrieveVectors::Retrieve {
            // Clippy is wrong
            #[allow(clippy::manual_unwrap_or_default)]
            let mut vectors = match document.remove("_vectors") {
                Some(Value::Object(map)) => map,
                _ => Default::default(),
            };
            for (name, EmbeddingsWithMetadata { embeddings, regenerate, has_fragments: _ }) in
                self.index.embeddings(self.rtxn, id)?
            {
                let embeddings =
                    ExplicitVectors { embeddings: Some(embeddings.into()), regenerate };
                vectors.insert(
                    name,
                    serde_json::to_value(embeddings).map_err(InternalError::SerdeJson)?,
                );
            }
            document.insert("_vectors".into(), vectors.into());
        }

        let localized_attributes =
            self.index.localized_attributes_rules(self.rtxn)?.unwrap_or_default();

        // If you need to format fields, pay the cost create the document from the displayed fields
        // TODO make the format field use the obkv and only format necessary fields
        let (matches_position, formatted) = if !self.show_matches_position
            && self.formatted_options.is_empty()
        {
            (None, Document::new())
        } else {
            let extract_field = |&fid| self.fields_ids_map.name(fid).expect("Missing field name");
            let selectors: Vec<_> = if self.show_matches_position {
                self.displayed_ids.iter().map(extract_field).collect()
            } else {
                self.formatted_options.keys().map(extract_field).collect()
            };

            let document = make_document(obkv, &self.fields_ids_map, &selectors)?;

            format_fields(
                document,
                &self.fields_ids_map,
                &self.formatter_builder,
                &self.formatted_options,
                self.show_matches_position,
                &self.displayed_ids,
                self.locales.as_deref(),
                &localized_attributes,
            )?
        };

        if let Some(sort) = self.sort.as_ref() {
            insert_geo_distance(sort, &mut document);
        }

        let ranking_score =
            self.show_ranking_score.then(|| ScoreDetails::global_score(score.iter()));
        let ranking_score_details = self
            .show_ranking_score_details
            .then(|| ScoreDetails::to_json_map(self.attribute_state, score.iter()));

        let hit = SearchHit {
            document,
            extra_document,
            formatted,
            matches_position,
            ranking_score_details,
            ranking_score,
        };

        Ok(hit)
    }
}

fn make_hits<'a>(
    index: &Index,
    rtxn: &RoTxn<'_>,
    format: AttributesFormat,
    matching_words: milli::MatchingWords,
    documents_ids_scores: impl Iterator<Item = (u32, &'a Vec<ScoreDetails>)> + 'a,
    progress: &Progress,
) -> milli::Result<Vec<SearchHit>> {
    let mut documents = Vec::new();

    let dictionary = index.dictionary(rtxn)?;
    let dictionary: Option<Vec<_>> =
        dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
    let separators = index.allowed_separators(rtxn)?;
    let separators: Option<Vec<_>> =
        separators.as_ref().map(|x| x.iter().map(String::as_str).collect());

    let tokenizer = HitMaker::tokenizer(dictionary.as_deref(), separators.as_deref());

    let formatter_builder = HitMaker::formatter_builder(matching_words, tokenizer);

    let hit_maker = HitMaker::new(index, rtxn, format, formatter_builder)?;

    for (id, score) in documents_ids_scores {
        documents.push(hit_maker.make_hit(id, score, progress)?);
    }
    Ok(documents)
}

pub fn perform_facet_search(
    index: &Index,
    search_query: SearchQuery,
    facet_query: Option<String>,
    facet_name: String,
    search_kind: SearchKind,
    features: RoFeatures,
    locales: Option<Vec<Language>>,
) -> Result<FacetSearchResult, ResponseError> {
    let before_search = Instant::now();
    let progress = Progress::default();
    let rtxn = index.read_txn()?;
    let deadline = index.search_deadline(&rtxn)?;

    if !index.facet_search(&rtxn)? {
        return Err(ResponseError::from_msg(
            "The facet search is disabled for this index".to_string(),
            Code::FacetSearchDisabled,
        ));
    }

    // In the faceted search context, we want to use the intersection between the locales provided by the user
    // and the locales of the facet string.
    // If the facet string is not localized, we **ignore** the locales provided by the user because the facet data has no locale.
    // If the user does not provide locales, we use the locales of the facet string.
    let localized_attributes = index.localized_attributes_rules(&rtxn)?.unwrap_or_default();
    let localized_attributes_locales = localized_attributes
        .into_iter()
        .find(|attr| attr.match_str(&facet_name) == PatternMatch::Match);
    let locales = localized_attributes_locales.map(|attr| {
        attr.locales
            .into_iter()
            .filter(|locale| locales.as_ref().is_none_or(|locales| locales.contains(locale)))
            .collect()
    });

    let (search, _, _, _) =
        prepare_search(index, &rtxn, &search_query, &search_kind, deadline, features, &progress)?;
    let mut facet_search = SearchForFacetValues::new(
        facet_name,
        search,
        matches!(search_kind, SearchKind::Hybrid { .. }),
    );
    if let Some(facet_query) = &facet_query {
        facet_search.query(facet_query);
    }
    if let Some(max_facets) = index.max_values_per_facet(&rtxn)? {
        facet_search.max_values(max_facets as usize);
    }

    if let Some(locales) = locales {
        facet_search.locales(locales);
    }

    Ok(FacetSearchResult {
        facet_hits: facet_search.execute()?,
        facet_query,
        processing_time_ms: before_search.elapsed().as_millis(),
    })
}

#[allow(clippy::too_many_arguments)]
pub fn perform_similar(
    index: &Index,
    query: SimilarQuery,
    embedder_name: String,
    embedder: Arc<Embedder>,
    quantized: bool,
    retrieve_vectors: RetrieveVectors,
    features: RoFeatures,
    progress: &Progress,
) -> Result<SimilarResult, ResponseError> {
    let before_search = Instant::now();
    let rtxn = index.read_txn()?;

    let SimilarQuery {
        id,
        offset,
        limit,
        filter: _,
        embedder: _,
        attributes_to_retrieve,
        retrieve_vectors: _,
        show_ranking_score,
        show_ranking_score_details,
        show_performance_details,
        ranking_score_threshold,
    } = query;

    let id: ExternalDocumentId = id.try_into().map_err(|error| {
        let msg = format!("Invalid value at `.id`: {error}");
        ResponseError::from_msg(msg, Code::InvalidSimilarId)
    })?;

    // using let-else rather than `?` so that the borrow checker identifies we're always returning here,
    // preventing a use-after-move
    let Some(internal_id) = index.external_documents_ids().get(&rtxn, &id)? else {
        return Err(ResponseError::from_msg(
            MeilisearchHttpError::DocumentNotFound(id.into_inner()).to_string(),
            Code::NotFoundSimilarId,
        ));
    };

    let mut similar = milli::Similar::new(
        internal_id,
        offset,
        limit,
        index,
        &rtxn,
        embedder_name,
        embedder,
        quantized,
        progress,
    );

    if let Some(ref filter) = query.filter {
        if let Some(facets) = parse_filter(filter, Code::InvalidSimilarFilter, features)? {
            similar.filter(facets);
        }
    }

    if let Some(ranking_score_threshold) = ranking_score_threshold {
        similar.ranking_score_threshold(ranking_score_threshold.0);
    }

    let milli::SearchResult {
        documents_ids,
        matching_words: _,
        candidates,
        document_scores,
        degraded: _,
        used_negative_operator: _,
        query_vector: _,
    } = similar.execute().map_err(|err| match err {
        milli::Error::UserError(milli::UserError::InvalidFilter(_)) => {
            ResponseError::from_msg(err.to_string(), Code::InvalidSimilarFilter)
        }
        err => err.into(),
    })?;

    let format = AttributesFormat {
        attributes_to_retrieve,
        extra_attributes_to_retrieve: Default::default(),
        retrieve_vectors,
        attributes_to_highlight: None,
        attributes_to_crop: None,
        crop_length: DEFAULT_CROP_LENGTH(),
        crop_marker: DEFAULT_CROP_MARKER(),
        highlight_pre_tag: DEFAULT_HIGHLIGHT_PRE_TAG(),
        highlight_post_tag: DEFAULT_HIGHLIGHT_POST_TAG(),
        show_matches_position: false,
        sort: None,
        show_ranking_score,
        show_ranking_score_details,
        locales: None,
    };

    let hits = make_hits(
        index,
        &rtxn,
        format,
        Default::default(),
        documents_ids.iter().copied().zip(document_scores.iter()),
        progress,
    )?;

    let max_total_hits = index
        .pagination_max_total_hits(&rtxn)
        .map_err(milli::Error::from)?
        .map(|x| x as usize)
        .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS);

    let number_of_hits = min(candidates.len() as usize, max_total_hits);
    let hits_info = HitsInfo::OffsetLimit { limit, offset, estimated_total_hits: number_of_hits };

    let performance_details = show_performance_details.then(|| progress.accumulated_durations());

    let result = SimilarResult {
        hits,
        hits_info,
        id: id.into_inner(),
        processing_time_ms: before_search.elapsed().as_millis(),
        performance_details,
    };
    Ok(result)
}

pub fn insert_geo_distance(sorts: &[String], document: &mut Document) {
    lazy_static::lazy_static! {
        static ref GEO_REGEX: Regex =
            Regex::new(r"_geoPoint\(\s*([[:digit:].\-]+)\s*,\s*([[:digit:].\-]+)\s*\)").unwrap();
    };
    if let Some(capture_group) = sorts.iter().find_map(|sort| GEO_REGEX.captures(sort)) {
        // TODO: TAMO: milli encountered an internal error, what do we want to do?
        let base = [capture_group[1].parse().unwrap(), capture_group[2].parse().unwrap()];
        let geo_point = &document.get("_geo").unwrap_or(&json!(null));
        if let Some((lat, lng)) =
            extract_geo_value(&geo_point["lat"]).zip(extract_geo_value(&geo_point["lng"]))
        {
            let distance = milli::distance_between_two_points(&base, &[lat, lng]);
            document.insert("_geoDistance".to_string(), json!(distance.round() as usize));
        }
    }
}

fn extract_geo_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn compute_formatted_options(
    attr_to_highlight: &HashSet<String>,
    attr_to_crop: &[String],
    query_crop_length: usize,
    to_retrieve_ids: &BTreeSet<FieldId>,
    fields_ids_map: &FieldsIdsMap,
    displayed_ids: &BTreeSet<FieldId>,
) -> BTreeMap<FieldId, FormatOptions> {
    let mut formatted_options = BTreeMap::new();

    add_highlight_to_formatted_options(
        &mut formatted_options,
        attr_to_highlight,
        fields_ids_map,
        displayed_ids,
    );

    add_crop_to_formatted_options(
        &mut formatted_options,
        attr_to_crop,
        query_crop_length,
        fields_ids_map,
        displayed_ids,
    );

    // Should not return `_formatted` if no valid attributes to highlight/crop
    if !formatted_options.is_empty() {
        add_non_formatted_ids_to_formatted_options(&mut formatted_options, to_retrieve_ids);
    }

    formatted_options
}

fn add_highlight_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    attr_to_highlight: &HashSet<String>,
    fields_ids_map: &FieldsIdsMap,
    displayed_ids: &BTreeSet<FieldId>,
) {
    for attr in attr_to_highlight {
        let new_format = FormatOptions { highlight: true, crop: None };

        if attr == "*" {
            for id in displayed_ids {
                formatted_options.insert(*id, new_format);
            }
            break;
        }

        if let Some(id) = fields_ids_map.id(attr) {
            if displayed_ids.contains(&id) {
                formatted_options.insert(id, new_format);
            }
        }
    }
}

fn add_crop_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    attr_to_crop: &[String],
    crop_length: usize,
    fields_ids_map: &FieldsIdsMap,
    displayed_ids: &BTreeSet<FieldId>,
) {
    for attr in attr_to_crop {
        let mut split = attr.rsplitn(2, ':');
        let (attr_name, attr_len) = match split.next().zip(split.next()) {
            Some((len, name)) => {
                let crop_len = len.parse::<usize>().unwrap_or(crop_length);
                (name, crop_len)
            }
            None => (attr.as_str(), crop_length),
        };

        if attr_name == "*" {
            for id in displayed_ids {
                formatted_options
                    .entry(*id)
                    .and_modify(|f| f.crop = Some(attr_len))
                    .or_insert(FormatOptions { highlight: false, crop: Some(attr_len) });
            }
        }

        if let Some(id) = fields_ids_map.id(attr_name) {
            if displayed_ids.contains(&id) {
                formatted_options
                    .entry(id)
                    .and_modify(|f| f.crop = Some(attr_len))
                    .or_insert(FormatOptions { highlight: false, crop: Some(attr_len) });
            }
        }
    }
}

fn add_non_formatted_ids_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    to_retrieve_ids: &BTreeSet<FieldId>,
) {
    for id in to_retrieve_ids {
        formatted_options.entry(*id).or_insert(FormatOptions { highlight: false, crop: None });
    }
}

fn make_document<S, I>(
    obkv: &obkv::KvReaderU16,
    field_ids_map: &FieldsIdsMap,
    selectors: impl IntoIterator<IntoIter = I>,
) -> milli::Result<Document>
where
    S: AsRef<str>,
    I: Clone + Iterator<Item = S>,
{
    let selectors = selectors.into_iter();
    let mut document = serde_json::Map::new();

    for (key, value_bytes) in obkv {
        let key = field_ids_map.name(key).expect("Missing field name");
        if !selectors.clone().any(|selector| contained_in(selector.as_ref(), key)) {
            // If the key is not part of the selection, skip this value
            continue;
        }

        let visitor = ValuePathsVisitor::new_from_path(selectors.clone(), key);
        let mut deserializer = serde_json::de::Deserializer::from_slice(value_bytes);
        let value = visitor.deserialize(&mut deserializer).map_err(InternalError::SerdeJson)?;
        document.insert(key.to_string(), value);
    }

    Ok(document)
}

#[allow(clippy::too_many_arguments)]
fn format_fields(
    mut document: Document,
    field_ids_map: &FieldsIdsMap,
    builder: &MatcherBuilder<'_>,
    formatted_options: &BTreeMap<FieldId, FormatOptions>,
    compute_matches: bool,
    displayable_ids: &BTreeSet<FieldId>,
    locales: Option<&[Language]>,
    localized_attributes: &[LocalizedAttributesRule],
) -> milli::Result<(Option<MatchesPosition>, Document)> {
    let mut matches_position = compute_matches.then(BTreeMap::new);

    // reduce the formatted option list to the attributes that should be formatted,
    // instead of all the attributes to display.
    let formatting_fields_options: Vec<_> = formatted_options
        .iter()
        .filter(|(_, option)| option.should_format())
        .map(|(fid, option)| (field_ids_map.name(*fid).unwrap(), option))
        .collect();

    // select the attributes to retrieve
    let displayable_names =
        displayable_ids.iter().map(|&fid| field_ids_map.name(fid).expect("Missing field name"));
    permissive_json_pointer::map_leaf_values(
        &mut document,
        displayable_names,
        |key, array_indices, value| {
            // To get the formatting option of each key we need to see all the rules that applies
            // to the value and merge them together. eg. If a user said he wanted to highlight `doggo`
            // and crop `doggo.name`. `doggo.name` needs to be highlighted + cropped while `doggo.age` is only
            // highlighted.
            // Warn: The time to compute the format list scales with the number of fields to format;
            // cumulated with map_leaf_values that iterates over all the nested fields, it gives a quadratic complexity:
            // d*f where d is the total number of fields to display and f is the total number of fields to format.
            let format = formatting_fields_options
                .iter()
                .filter(|(name, _option)| {
                    milli::is_faceted_by(name, key) || milli::is_faceted_by(key, name)
                })
                .map(|(_, option)| **option)
                .reduce(|acc, option| acc.merge(option));
            let mut infos = Vec::new();

            // if no locales has been provided, we try to find the locales in the localized_attributes.
            let locales = locales.or_else(|| {
                localized_attributes
                    .iter()
                    .find(|rule| rule.match_str(key) == PatternMatch::Match)
                    .map(LocalizedAttributesRule::locales)
            });

            *value = format_value(
                std::mem::take(value),
                builder,
                format,
                &mut infos,
                compute_matches,
                array_indices,
                locales,
            );

            if let Some(matches) = matches_position.as_mut() {
                if !infos.is_empty() {
                    matches.insert(key.to_owned(), infos);
                }
            }
        },
    );

    // We remove the fields that were not selected by the formatted_options.
    let selectors = formatted_options.keys().map(|&fid| field_ids_map.name(fid).unwrap());
    let document = permissive_json_pointer::select_values(document, selectors);

    Ok((matches_position, document))
}

fn format_value(
    value: Value,
    builder: &MatcherBuilder<'_>,
    format_options: Option<FormatOptions>,
    infos: &mut Vec<MatchBounds>,
    compute_matches: bool,
    array_indices: &[usize],
    locales: Option<&[Language]>,
) -> Value {
    match value {
        Value::String(old_string) => {
            let mut matcher = builder.build(&old_string, locales);
            if compute_matches {
                let matches = matcher.matches(array_indices);
                infos.extend_from_slice(&matches[..]);
            }

            match format_options {
                Some(format_options) => {
                    let value = matcher.format(format_options);
                    Value::String(value.into_owned())
                }
                None => Value::String(old_string),
            }
        }
        // `map_leaf_values` makes sure this is only called for leaf fields
        Value::Array(_) => unreachable!(),
        Value::Object(_) => unreachable!(),
        Value::Number(number) => {
            let s = number.to_string();

            let mut matcher = builder.build(&s, locales);
            if compute_matches {
                let matches = matcher.matches(array_indices);
                infos.extend_from_slice(&matches[..]);
            }

            match format_options {
                Some(format_options) => {
                    let value = matcher.format(format_options);
                    Value::String(value.into_owned())
                }
                None => Value::String(s),
            }
        }
        value => value,
    }
}

pub(crate) fn parse_filter(
    facets: &Value,
    filter_parsing_error_code: Code,
    features: RoFeatures,
) -> Result<Option<Filter<'_>>, ResponseError> {
    let filter = match facets {
        Value::String(expr) => Filter::from_str(expr).map_err(|e| e.into()),
        Value::Array(arr) => parse_filter_array(arr).map_err(|e| e.into()),
        v => Err(MeilisearchHttpError::InvalidExpression(&["String", "Array"], v.clone()).into()),
    };
    let filter = filter.map_err(|err: ResponseError| {
        ResponseError::from_msg(err.to_string(), filter_parsing_error_code)
    })?;

    if let Some(ref filter) = filter {
        // If the contains operator is used while the contains filter feature is not enabled, errors out
        if let Some((token, error)) =
            filter.use_contains_operator().zip(features.check_contains_filter().err())
        {
            return Err(ResponseError::from_msg(
                token.as_external_error(error).to_string(),
                Code::FeatureNotEnabled,
            ));
        }
    }

    if let Some(ref filter) = filter {
        if let Some((token, error)) =
            filter.use_shard_filter().zip(features.check_network("using a shard filter").err())
        {
            return Err(ResponseError::from_msg(
                token.as_external_error(error).to_string(),
                Code::FeatureNotEnabled,
            ));
        }
    }

    if let Some(ref filter) = filter {
        // If a vector filter is used while the multi modal feature is not enabled, errors out
        if let Some((token, error)) =
            filter.use_vector_filter().zip(features.check_multimodal("using a vector filter").err())
        {
            return Err(ResponseError::from_msg(
                token.as_external_error(error).to_string(),
                Code::FeatureNotEnabled,
            ));
        }
    }

    Ok(filter)
}

fn parse_filter_array(arr: &'_ [Value]) -> Result<Option<Filter<'_>>, MeilisearchHttpError> {
    let mut ands = Vec::new();
    for value in arr {
        match value {
            Value::String(s) => ands.push(Either::Right(s.as_str())),
            Value::Array(arr) => {
                let mut ors = Vec::new();
                for value in arr {
                    match value {
                        Value::String(s) => ors.push(s.as_str()),
                        v => {
                            return Err(MeilisearchHttpError::InvalidExpression(
                                &["String"],
                                v.clone(),
                            ));
                        }
                    }
                }
                ands.push(Either::Left(ors));
            }
            v => {
                return Err(MeilisearchHttpError::InvalidExpression(
                    &["String", "[String]"],
                    v.clone(),
                ));
            }
        }
    }

    Filter::from_array(ands).map_err(|e| MeilisearchHttpError::from_milli(e, None))
}

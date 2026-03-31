use core::fmt;
use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    filtered_universe, AttributeState, Deadline, FacetValueHit, FilterCondition, IndexFilter,
    IndexFilterCondition, InternalError, OrderBy, PatternMatch, SearchBuilder,
    SearchForFacetValues, SearchStep, Token,
};
use meilisearch_types::{milli, Document};
use milli::tokenizer::{Language, TokenizerBuilder};
use milli::{
    AscDesc, FieldId, FieldsIdsMap, Filter, FormatOptions, Index, LocalizedAttributesRule,
    MatchBounds, MatcherBuilder, SortError, TermsMatchingStrategy,
    DEFAULT_PAGINATION_MAX_TOTAL_HITS, DEFAULT_VALUES_PER_FACET,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::search::{parse_filter, SearchKind, SearchQuery, DEFAULT_SEARCH_LIMIT};

// pub fn prepare_search<'t>(
//     index: &'t Index,
//     rtxn: &'t RoTxn,
//     query: &'t SearchQuery,
//     search_kind: &SearchKind,
//     deadline: Deadline,
//     features: RoFeatures,
//     progress: &'t Progress,
// ) -> Result<(milli::Search<'t>, bool, usize, usize), ResponseError> {
//     if query.media.is_some() {
//         features.check_multimodal("passing `media` in a search query")?;
//     }
//     let mut search = index.search(rtxn, progress);
//     search.deadline(deadline);
//     if let Some(ranking_score_threshold) = query.ranking_score_threshold {
//         search.ranking_score_threshold(ranking_score_threshold.0);
//     }

//     if let Some(distinct) = &query.distinct {
//         search.distinct(distinct.clone());
//     }

//     match search_kind {
//         SearchKind::KeywordOnly => {
//             if let Some(q) = &query.q {
//                 search.query(q);
//             }
//         }
//         SearchKind::SemanticOnly { embedder_name, embedder, quantized } => {
//             let vector = match query.vector.clone() {
//                 Some(vector) => vector,
//                 None => {
//                     let _step = progress.update_progress_scoped(SearchStep::Embed);
//                     let span = tracing::trace_span!(target: "search::vector", "embed_one");
//                     let _entered = span.enter();

//                     let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

//                     let q = query.q.as_deref();
//                     let media = query.media.as_ref();

//                     let search_query = match (q, media) {
//                         (Some(text), None) => milli::vector::SearchQuery::Text(text),
//                         (q, media) => milli::vector::SearchQuery::Media { q, media },
//                     };

//                     embedder
//                         .embed_search(search_query, Some(deadline))
//                         .map_err(milli::vector::Error::from)
//                         .map_err(milli::Error::from)?
//                 }
//             };
//             search.semantic(
//                 embedder_name.clone(),
//                 embedder.clone(),
//                 *quantized,
//                 Some(vector),
//                 query.media.clone(),
//             );
//         }
//         SearchKind::Hybrid { embedder_name, embedder, quantized, semantic_ratio: _ } => {
//             if let Some(q) = &query.q {
//                 search.query(q);
//             }
//             // will be embedded in hybrid search if necessary
//             search.semantic(
//                 embedder_name.clone(),
//                 embedder.clone(),
//                 *quantized,
//                 query.vector.clone(),
//                 query.media.clone(),
//             );
//         }
//     }

//     if let Some(ref searchable) = query.attributes_to_search_on {
//         search.searchable_attributes(searchable);
//     }

//     let is_finite_pagination = query.is_finite_pagination();
//     search.terms_matching_strategy(query.matching_strategy.into());

//     let max_total_hits = index
//         .pagination_max_total_hits(rtxn)
//         .map_err(milli::Error::from)?
//         .map(|x| x as usize)
//         .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS);

//     search.retrieve_vectors(query.retrieve_vectors);
//     search.exhaustive_number_hits(is_finite_pagination);
//     search.max_total_hits(Some(max_total_hits));
//     search.scoring_strategy(
//         if query.show_ranking_score
//             || query.show_ranking_score_details
//             || query.ranking_score_threshold.is_some()
//         {
//             ScoringStrategy::Detailed
//         } else {
//             ScoringStrategy::Skip
//         },
//     );

//     // compute the offset on the limit depending on the pagination mode.
//     let (offset, limit) = if is_finite_pagination {
//         let limit = query.hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
//         let page = query.page.unwrap_or(1);

//         // page 0 gives a limit of 0 forcing Meilisearch to return no document.
//         page.checked_sub(1).map_or((0, 0), |p| (limit * p, limit))
//     } else {
//         (query.offset, query.limit)
//     };

//     // Make sure that a user can't get more documents than the hard limit,
//     // we align that on the offset too.
//     let offset = min(offset, max_total_hits);
//     let limit = min(limit, max_total_hits.saturating_sub(offset));

//     search.offset(offset);
//     search.limit(limit);

//     if let Some(ref filter) = query.filter {
//         if let Some(facets) = parse_filter(filter, Code::InvalidSearchFilter, features)? {
//             search.filter(facets);
//         }
//     }

//     if let Some(ref sort) = query.sort {
//         let sort = match sort.iter().map(|s| AscDesc::from_str(s)).collect() {
//             Ok(sorts) => sorts,
//             Err(asc_desc_error) => {
//                 return Err(SortError::from(asc_desc_error).into_search_error().into());
//             }
//         };

//         search.sort_criteria(sort);
//     }

//     if let Some(ref locales) = query.locales {
//         search.locales(locales.iter().copied().map(Into::into).collect());
//     }

//     Ok((search, is_finite_pagination, max_total_hits, offset))
// }

pub fn prepare_searches<'a, 'k, I>(
    queries: I,
    features: RoFeatures,
) -> Result<Vec<SearchBuilder<'a, Filter<'a>>>, ResponseError>
where
    I: IntoIterator<Item = (IndexUid, &'a SearchQuery, &'k SearchKind)>,
{
    let mut searches = Vec::new();
    for (index_uid, query, search_kind) in queries {
        let builder = search_builder(index_uid.to_string(), query, search_kind, features)?;
        searches.push(builder);
    }
    Ok(searches)
}

pub fn search_builder<'a>(
    index_uid: String,
    query: &'a SearchQuery,
    search_kind: &SearchKind,
    features: RoFeatures,
) -> Result<SearchBuilder<'a, Filter<'a>>, ResponseError> {
    if query.media.is_some() {
        features.check_multimodal("passing `media` in a search query")?;
    }
    /// TODO: avoid converting to string
    let mut builder = SearchBuilder::new(index_uid);
    if let Some(ranking_score_threshold) = query.ranking_score_threshold {
        builder.ranking_score_threshold(ranking_score_threshold.0);
    }

    if let Some(distinct) = &query.distinct {
        builder.distinct(distinct.clone());
    }

    match search_kind {
        SearchKind::KeywordOnly => {
            if let Some(q) = &query.q {
                builder.query(q);
            }
        }
        SearchKind::SemanticOnly { embedder_name, embedder, quantized } => {
            // we must precompute the vector
            let precompute_vector = true;
            builder.semantic(
                embedder_name.clone(),
                embedder.clone(),
                *quantized,
                query.vector.clone(),
                query.media.clone(),
                precompute_vector,
            );
        }
        SearchKind::Hybrid { embedder_name, embedder, quantized, semantic_ratio: _ } => {
            if let Some(q) = &query.q {
                builder.query(q);
            }
            // will be embedded in hybrid search if necessary
            let precompute_vector = false;
            builder.semantic(
                embedder_name.clone(),
                embedder.clone(),
                *quantized,
                query.vector.clone(),
                query.media.clone(),
                precompute_vector,
            );
        }
    }

    if let Some(ref searchable) = query.attributes_to_search_on {
        builder.searchable_attributes(searchable);
    }

    let is_finite_pagination = query.is_finite_pagination();
    builder.terms_matching_strategy(query.matching_strategy.into());

    builder.retrieve_vectors(query.retrieve_vectors);
    builder.exhaustive_number_hits(is_finite_pagination);
    builder.scoring_strategy(
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

    builder.offset(offset);
    builder.limit(limit);

    if let Some(ref filter) = query.filter {
        if let Some(facets) = parse_filter(filter, Code::InvalidSearchFilter, features)? {
            builder.filter(facets);
        }
    }

    if let Some(ref sort) = query.sort {
        let sort = match sort.iter().map(|s| AscDesc::from_str(s)).collect() {
            Ok(sorts) => sorts,
            Err(asc_desc_error) => {
                return Err(SortError::from(asc_desc_error).into_search_error().into());
            }
        };

        builder.sort_criteria(sort);
    }

    if let Some(ref locales) = query.locales {
        builder.locales(locales.iter().copied().map(Into::into).collect());
    }

    Ok(builder)
}

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt;
use std::vec::Vec;

use indexmap::IndexMap;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{
    InvalidMultiSearchFacetsByIndex, InvalidMultiSearchMaxValuesPerFacet,
    InvalidMultiSearchMergeFacets, InvalidMultiSearchQueryPosition, InvalidMultiSearchRemote,
    InvalidMultiSearchWeight, InvalidSearchLimit, InvalidSearchOffset,
};
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::order_by_map::OrderByMap;
use meilisearch_types::milli::OrderBy;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::super::{ComputedFacets, FacetStats, HitsInfo, SearchHit, SearchQueryWithIndex};

pub const DEFAULT_FEDERATED_WEIGHT: f64 = 1.0;

// fields in the response
pub const FEDERATION_HIT: &str = "_federation";
pub const INDEX_UID: &str = "indexUid";
pub const QUERIES_POSITION: &str = "queriesPosition";
pub const WEIGHTED_RANKING_SCORE: &str = "weightedRankingScore";
pub const WEIGHTED_SCORE_VALUES: &str = "weightedScoreValues";
pub const FEDERATION_REMOTE: &str = "remote";

#[derive(Debug, Default, Clone, PartialEq, Serialize, deserr::Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]

pub struct FederationOptions {
    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchWeight>)]
    #[schema(value_type = f64)]
    pub weight: Weight,

    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchRemote>)]
    pub remote: Option<String>,

    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchQueryPosition>)]
    pub query_position: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, deserr::Deserr)]
#[deserr(try_from(f64) = TryFrom::try_from -> InvalidMultiSearchWeight)]
pub struct Weight(f64);

impl Default for Weight {
    fn default() -> Self {
        Weight(DEFAULT_FEDERATED_WEIGHT)
    }
}

impl std::convert::TryFrom<f64> for Weight {
    type Error = InvalidMultiSearchWeight;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        if f < 0.0 {
            Err(InvalidMultiSearchWeight)
        } else {
            Ok(Weight(f))
        }
    }
}

impl std::ops::Deref for Weight {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, deserr::Deserr, Serialize, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub struct Federation {
    #[deserr(default = super::super::DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSearchLimit>)]
    pub limit: usize,
    #[deserr(default = super::super::DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSearchOffset>)]
    pub offset: usize,
    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchFacetsByIndex>)]
    pub facets_by_index: BTreeMap<IndexUid, Option<Vec<String>>>,
    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchMergeFacets>)]
    pub merge_facets: Option<MergeFacets>,
}

#[derive(Copy, Clone, Debug, deserr::Deserr, Serialize, Default, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidMultiSearchMergeFacets>, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub struct MergeFacets {
    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchMaxValuesPerFacet>)]
    pub max_values_per_facet: Option<usize>,
}

#[derive(Debug, deserr::Deserr, Serialize, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub struct FederatedSearch {
    pub queries: Vec<SearchQueryWithIndex>,
    #[deserr(default)]
    pub federation: Option<Federation>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct FederatedSearchResult {
    pub hits: Vec<SearchHit>,
    pub processing_time_ms: u128,
    #[serde(flatten)]
    pub hits_info: HitsInfo,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_hit_count: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<BTreeMap<String, BTreeMap<String, u64>>>)]
    pub facet_distribution: Option<BTreeMap<String, IndexMap<String, u64>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub facet_stats: Option<BTreeMap<String, FacetStats>>,
    #[serde(default, skip_serializing_if = "FederatedFacets::is_empty")]
    pub facets_by_index: FederatedFacets,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_errors: Option<BTreeMap<String, ResponseError>>,

    // These fields are only used for analytics purposes
    #[serde(skip)]
    pub degraded: bool,
    #[serde(skip)]
    pub used_negative_operator: bool,
}

impl fmt::Debug for FederatedSearchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let FederatedSearchResult {
            hits,
            processing_time_ms,
            hits_info,
            semantic_hit_count,
            degraded,
            used_negative_operator,
            facet_distribution,
            facet_stats,
            facets_by_index,
            remote_errors,
        } = self;

        let mut debug = f.debug_struct("SearchResult");
        // The most important thing when looking at a search result is the time it took to process
        debug.field("processing_time_ms", &processing_time_ms);
        debug.field("hits", &format!("[{} hits returned]", hits.len()));
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
        if !facets_by_index.is_empty() {
            debug.field("facets_by_index", &facets_by_index);
        }
        if let Some(remote_errors) = remote_errors {
            debug.field("remote_errors", &remote_errors);
        }

        debug.finish()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct FederatedFacets(pub BTreeMap<String, ComputedFacets>);

impl FederatedFacets {
    pub fn insert(&mut self, index: String, facets: Option<ComputedFacets>) {
        if let Some(facets) = facets {
            self.0.insert(index, facets);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn merge(
        self,
        MergeFacets { max_values_per_facet }: MergeFacets,
        facet_order: BTreeMap<String, (String, OrderBy)>,
    ) -> Option<ComputedFacets> {
        if self.is_empty() {
            return None;
        }

        let mut distribution: BTreeMap<String, _> = Default::default();
        let mut stats: BTreeMap<String, FacetStats> = Default::default();

        for facets_by_index in self.0.into_values() {
            for (facet, index_distribution) in facets_by_index.distribution {
                match distribution.entry(facet) {
                    Entry::Vacant(entry) => {
                        entry.insert(index_distribution);
                    }
                    Entry::Occupied(mut entry) => {
                        let distribution = entry.get_mut();

                        for (value, index_count) in index_distribution {
                            distribution
                                .entry(value)
                                .and_modify(|count| *count += index_count)
                                .or_insert(index_count);
                        }
                    }
                }
            }

            for (facet, index_stats) in facets_by_index.stats {
                match stats.entry(facet) {
                    Entry::Vacant(entry) => {
                        entry.insert(index_stats);
                    }
                    Entry::Occupied(mut entry) => {
                        let stats = entry.get_mut();

                        stats.min = f64::min(stats.min, index_stats.min);
                        stats.max = f64::max(stats.max, index_stats.max);
                    }
                }
            }
        }

        // fixup order
        for (facet, values) in &mut distribution {
            let order_by = facet_order.get(facet).map(|(_, order)| *order).unwrap_or_default();

            match order_by {
                OrderBy::Lexicographic => {
                    values.sort_unstable_by(|left, _, right, _| left.cmp(right))
                }
                OrderBy::Count => {
                    values.sort_unstable_by(|_, left, _, right| {
                        left.cmp(right)
                            // biggest first
                            .reverse()
                    })
                }
            }

            if let Some(max_values_per_facet) = max_values_per_facet {
                values.truncate(max_values_per_facet)
            };
        }

        Some(ComputedFacets { distribution, stats })
    }

    pub(crate) fn append(&mut self, FederatedFacets(remote_facets_by_index): FederatedFacets) {
        for (index, remote_facets) in remote_facets_by_index {
            let merged_facets = self.0.entry(index).or_default();

            for (remote_facet, remote_stats) in remote_facets.stats {
                match merged_facets.stats.entry(remote_facet) {
                    Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(remote_stats);
                    }
                    Entry::Occupied(mut occupied_entry) => {
                        let stats = occupied_entry.get_mut();
                        stats.min = f64::min(stats.min, remote_stats.min);
                        stats.max = f64::max(stats.max, remote_stats.max);
                    }
                }
            }

            for (remote_facet, remote_values) in remote_facets.distribution {
                let merged_facet = merged_facets.distribution.entry(remote_facet).or_default();
                for (remote_value, remote_count) in remote_values {
                    let count = merged_facet.entry(remote_value).or_default();
                    *count += remote_count;
                }
            }
        }
    }

    pub fn sort_and_truncate(&mut self, facet_order: BTreeMap<String, (OrderByMap, usize)>) {
        for (index, facets) in &mut self.0 {
            let Some((order_by, max_values_per_facet)) = facet_order.get(index) else {
                continue;
            };
            for (facet, values) in &mut facets.distribution {
                match order_by.get(facet) {
                    OrderBy::Lexicographic => {
                        values.sort_unstable_by(|left, _, right, _| left.cmp(right))
                    }
                    OrderBy::Count => {
                        values.sort_unstable_by(|_, left, _, right| {
                            left.cmp(right)
                                // biggest first
                                .reverse()
                        })
                    }
                }
                values.truncate(*max_values_per_facet);
            }
        }
    }
}

use std::collections::BTreeMap;

use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::network::{Network, Remote, RemoteAvailability};
use serde_json::Value;

use crate::routes::indexes::facet_search::FacetSearchQuery;
use crate::search::{Federation, FederationOptions, SearchQuery, SearchQueryWithIndex};

#[cfg(not(feature = "enterprise"))]
mod community_edition;
#[cfg(feature = "enterprise")]
mod enterprise_edition;
#[cfg(not(feature = "enterprise"))]
use community_edition as current_edition;
#[cfg(feature = "enterprise")]
use enterprise_edition as current_edition;

#[derive(Clone)]
pub enum Partition {
    ByRemote { remotes: BTreeMap<String, Remote> },
    ByShard { remote_for_shard: BTreeMap<String, String> },
}

/// A trait defining how to proxy a query.
///
/// Proxying a query entails two main things:
///
/// 1. Setting the remote for the query
/// 2. Adjusting the `filter` field to filter on the correct shard
pub trait ProxyQuery {
    type ProxiedQuery;

    /// Set the remote for this proxy query, returning the proxied query
    fn proxy_with_remote(&self, remote: String) -> Self::ProxiedQuery;

    /// Provide an exclusive reference to the `filter` field of a proxied query
    fn filter_field(query: &mut Self::ProxiedQuery) -> &mut Option<Value>;
}

impl ProxyQuery for SearchQueryWithIndex {
    /// Output type is the same, as SearchQueryWithIndex already allows for specifying a remote
    type ProxiedQuery = SearchQueryWithIndex;

    fn proxy_with_remote(&self, remote: String) -> Self::ProxiedQuery {
        let mut query = (*self).clone();
        query.federation_options.get_or_insert_default().remote = Some(remote);
        query
    }

    fn filter_field(query: &mut Self::ProxiedQuery) -> &mut Option<Value> {
        &mut query.filter
    }
}

impl ProxyQuery for &FacetSearchQuery {
    /// The only things that can change are the filter on shard and the remote, so recover this
    type ProxiedQuery = (String, Option<serde_json::Value>);

    fn proxy_with_remote(&self, remote: String) -> Self::ProxiedQuery {
        (remote, None)
    }

    fn filter_field(query: &mut Self::ProxiedQuery) -> &mut Option<Value> {
        &mut query.1
    }
}

impl Partition {
    pub fn new(network: Network, remote_availability: &RemoteAvailability) -> Self {
        if network.leader.is_some() {
            Partition::ByShard {
                remote_for_shard: current_edition::remote_for_shard(network, remote_availability),
            }
        } else {
            Partition::ByRemote { remotes: network.remotes }
        }
    }

    pub fn to_partition<'a, Q: ProxyQuery + 'a>(
        &'a self,
        query: Q,
    ) -> Result<impl Iterator<Item = Q::ProxiedQuery> + 'a, ResponseError> {
        Ok(match self {
            Partition::ByRemote { remotes } => either::Left(
                remotes.keys().map(move |remote| query.proxy_with_remote(remote.clone())),
            ),
            Partition::ByShard { remote_for_shard } => {
                either::Right(current_edition::partition_shards(
                    query,
                    remote_for_shard.iter().map(|(shard, remote)| (shard, remote.clone())),
                )?)
            }
        })
    }

    pub fn into_partition<Q: ProxyQuery>(
        self,
        query: Q,
    ) -> Result<impl Iterator<Item = Q::ProxiedQuery>, ResponseError> {
        Ok(match self {
            Partition::ByRemote { remotes } => {
                either::Left(remotes.into_keys().map(move |remote| query.proxy_with_remote(remote)))
            }
            Partition::ByShard { remote_for_shard } => either::Right(
                current_edition::partition_shards(query, remote_for_shard.into_iter())?,
            ),
        })
    }

    pub fn to_query_partition(
        &self,
        federation: &mut Federation,
        query: &SearchQuery,
        federation_options: Option<FederationOptions>,
        index_uid: &IndexUid,
    ) -> Result<impl Iterator<Item = SearchQueryWithIndex> + '_, ResponseError> {
        let query = fixup_query_federation(federation, query, federation_options, index_uid);
        self.to_partition(query)
    }

    pub fn into_query_partition(
        self,
        federation: &mut Federation,
        query: &SearchQuery,
        federation_options: Option<FederationOptions>,
        index_uid: &IndexUid,
    ) -> Result<impl Iterator<Item = SearchQueryWithIndex>, ResponseError> {
        let query = fixup_query_federation(federation, query, federation_options, index_uid);

        self.into_partition(query)
    }
}

fn fixup_query_federation(
    federation: &mut Federation,
    query: &SearchQuery,
    federation_options: Option<FederationOptions>,
    index_uid: &IndexUid,
) -> SearchQueryWithIndex {
    let federation_options = federation_options.unwrap_or_default();
    let mut query = SearchQueryWithIndex::from_index_query_federation(
        index_uid.clone(),
        query.clone(),
        Some(federation_options),
    );

    // Move query parameters that make sense at the federation level
    // from the `SearchQueryWithIndex` to the `Federation`
    let SearchQueryWithIndex {
        index_uid,
        q: _,
        vector: _,
        media: _,
        hybrid: _,
        offset,
        limit,
        page,
        hits_per_page,
        attributes_to_retrieve: _,
        retrieve_vectors: _,
        attributes_to_crop: _,
        crop_length: _,
        attributes_to_highlight: _,
        show_ranking_score: _,
        show_ranking_score_details: _,
        show_performance_details,
        use_network: _,
        show_matches_position: _,
        filter: _,
        sort: _,
        distinct,
        facets,
        highlight_pre_tag: _,
        highlight_post_tag: _,
        crop_marker: _,
        matching_strategy: _,
        attributes_to_search_on: _,
        ranking_score_threshold: _,
        locales: _,
        personalize: _,
        federation_options: _,
    } = &mut query;

    let Federation {
        limit: federation_limit,
        offset: federation_offset,
        page: federation_page,
        hits_per_page: federation_hits_per_page,
        facets_by_index: _,
        merge_facets: _,
        show_performance_details: federation_show_performance_details,
        distinct: federation_distinct,
    } = federation;

    if let Some(limit) = limit.take() {
        *federation_limit = limit;
    }
    if let Some(offset) = offset.take() {
        *federation_offset = offset;
    }
    if let Some(page) = page.take() {
        *federation_page = Some(page);
    }
    if let Some(hits_per_page) = hits_per_page.take() {
        *federation_hits_per_page = Some(hits_per_page);
    }
    if let Some(distinct) = distinct.take() {
        *federation_distinct = Some(distinct);
    }

    if let Some(show_performance_details) = show_performance_details.take() {
        *federation_show_performance_details = show_performance_details;
    }

    'facets: {
        if let Some(facets) = facets.take() {
            if facets.is_empty() {
                break 'facets;
            }
            let facets_by_index = federation.facets_by_index.entry(index_uid.clone()).or_default();
            *facets_by_index = Some(facets);
        }
    }

    query
}

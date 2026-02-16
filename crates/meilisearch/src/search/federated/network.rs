use std::collections::BTreeMap;

use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::network::{Network, Remote};

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

impl Partition {
    pub fn new(network: Network) -> Self {
        if network.leader.is_some() {
            Partition::ByShard { remote_for_shard: current_edition::remote_for_shard(network) }
        } else {
            Partition::ByRemote { remotes: network.remotes }
        }
    }

    pub fn to_query_partition(
        &self,
        federation: &mut Federation,
        query: &SearchQuery,
        federation_options: Option<FederationOptions>,
        index_uid: &IndexUid,
    ) -> Result<impl Iterator<Item = SearchQueryWithIndex> + '_, ResponseError> {
        let query = fixup_query_federation(federation, query, federation_options, index_uid);

        Ok(match self {
            Partition::ByRemote { remotes } => either::Left(remotes.keys().map(move |remote| {
                let mut query = query.clone();
                query.federation_options.get_or_insert_default().remote = Some(remote.clone());
                query
            })),
            Partition::ByShard { remote_for_shard } => {
                either::Right(current_edition::partition_shards(
                    query,
                    remote_for_shard.iter().map(|(shard, remote)| (shard, remote.clone())),
                )?)
            }
        })
    }

    pub fn into_query_partition(
        self,
        federation: &mut Federation,
        query: &SearchQuery,
        federation_options: Option<FederationOptions>,
        index_uid: &IndexUid,
    ) -> Result<impl Iterator<Item = SearchQueryWithIndex>, ResponseError> {
        let query = fixup_query_federation(federation, query, federation_options, index_uid);

        Ok(match self {
            Partition::ByRemote { remotes } => {
                either::Left(remotes.into_keys().map(move |remote| {
                    let mut query = query.clone();
                    query.federation_options.get_or_insert_default().remote = Some(remote);
                    query
                }))
            }
            Partition::ByShard { remote_for_shard } => either::Right(
                current_edition::partition_shards(query, remote_for_shard.into_iter())?,
            ),
        })
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
        distinct: _,
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

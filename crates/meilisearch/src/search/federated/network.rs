use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::network::Network;

use crate::search::{Federation, FederationOptions, SearchQuery, SearchQueryWithIndex};

pub fn network_partition<'a>(
    federation: &mut Federation,
    query: &'a SearchQuery,
    federation_options: Option<FederationOptions>,
    index_uid: &'a IndexUid,
    network: Network,
) -> impl Iterator<Item = SearchQueryWithIndex> + 'a {
    let federation_options = federation_options.unwrap_or_default();
    let mut query = SearchQueryWithIndex::from_index_query_federation(
        index_uid.clone(),
        query.clone(),
        Some(federation_options),
    );

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

    if let Some(limit) = limit.take() {
        federation.limit = limit;
    }
    if let Some(offset) = offset.take() {
        federation.offset = offset;
    }
    if let Some(page) = page.take() {
        federation.page = Some(page);
    }
    if let Some(hits_per_page) = hits_per_page.take() {
        federation.hits_per_page = Some(hits_per_page);
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

    network.remotes.into_keys().map(move |remote| {
        let mut query = query.clone();
        query.federation_options.get_or_insert_default().remote = Some(remote);
        query
    })
}

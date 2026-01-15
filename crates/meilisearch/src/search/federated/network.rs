use crate::search::{Federation, FederationOptions, SearchQuery, SearchQueryWithIndex};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::network::Network;

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

    if let Some(limit) = query.limit.take() {
        federation.limit = limit;
    }
    if let Some(offset) = query.offset.take() {
        federation.offset = offset;
    }
    if let Some(page) = query.page.take() {
        federation.page = Some(page);
    }
    if let Some(hits_per_page) = query.hits_per_page.take() {
        federation.hits_per_page = Some(hits_per_page);
    }

    'facets: {
        if let Some(facets) = query.facets.take() {
            if facets.is_empty() {
                break 'facets;
            }
            let facets_by_index = federation.facets_by_index.entry(index_uid.clone()).or_default();
            *facets_by_index = Some(facets);
        }
    }

    network.remotes.into_keys().map(move |remote| {
        query.federation_options.get_or_insert_default().remote = Some(remote);
        query.clone()
    })
}

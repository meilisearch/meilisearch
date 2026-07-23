use std::collections::BTreeMap;

use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::IndexFilter;
use meilisearch_types::network::{Network, Remote, RemoteAvailability};

use crate::routes::indexes::facet_search::FacetSearchQuery;
use crate::search::federated::types::PreprocessedQuery;
use crate::search::SearchQueryWithIndex;

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
    fn filter_field(query: &mut Self::ProxiedQuery) -> &mut Option<IndexFilter>;
}

impl ProxyQuery for PreprocessedQuery<SearchQueryWithIndex> {
    /// Output type is the same, as SearchQueryWithIndex already allows for specifying a remote
    type ProxiedQuery = PreprocessedQuery<SearchQueryWithIndex>;

    fn proxy_with_remote(&self, remote: String) -> Self::ProxiedQuery {
        let mut query = (*self).clone();
        query.query.federation_options.get_or_insert_default().remote = Some(remote);
        query
    }

    fn filter_field(query: &mut Self::ProxiedQuery) -> &mut Option<IndexFilter> {
        &mut query.filter
    }
}

impl ProxyQuery for &PreprocessedQuery<(IndexUid, FacetSearchQuery)> {
    /// The only things that can change are the filter on shard and the remote, so recover this
    type ProxiedQuery = (String, Option<IndexFilter>);

    fn proxy_with_remote(&self, remote: String) -> Self::ProxiedQuery {
        (remote, None)
    }

    fn filter_field(query: &mut Self::ProxiedQuery) -> &mut Option<IndexFilter> {
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
}

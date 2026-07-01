use std::collections::BTreeMap;

use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::network::{Network, RemoteAvailability};

use super::ProxyQuery;

pub fn partition_shards<Q: ProxyQuery>(
    _query: Q,
    _remote_for_shard: impl Iterator<Item = (impl AsRef<str>, String)>,
) -> Result<impl Iterator<Item = Q::ProxiedQuery>, ResponseError> {
    Err::<std::iter::Empty<Q::ProxiedQuery>, _>(ResponseError::from_msg(
        "Meilisearch Enterprise Edition is required to use `useNetwork` when `network.leader` is set".into(),
        Code::RequiresEnterpriseEdition,
    ))
}

pub(super) fn remote_for_shard(
    _network: Network,
    _remotes_statuses: &RemoteAvailability,
) -> BTreeMap<String, String> {
    Default::default()
}

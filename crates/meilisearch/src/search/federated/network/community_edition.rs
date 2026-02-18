use std::collections::BTreeMap;

use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::network::Network;

use crate::search::SearchQueryWithIndex;

pub fn partition_shards(
    _query: SearchQueryWithIndex,
    _remote_for_shard: impl Iterator<Item = (impl AsRef<str>, String)>,
) -> Result<impl Iterator<Item = SearchQueryWithIndex>, ResponseError> {
    Err::<std::iter::Empty<SearchQueryWithIndex>, _>(ResponseError::from_msg(
        "Meilisearch Enterprise Edition is required to use `useNetwork` when `network.leader` is set".into(),
        Code::RequiresEnterpriseEdition,
    ))
}

pub(super) fn remote_for_shard(_network: Network) -> BTreeMap<String, String> {
    Default::default()
}

use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::network::Network;

use crate::search::SearchQueryWithIndex;

pub fn partition_shards(
    _network: Network,
    _query: SearchQueryWithIndex,
) -> Result<impl Iterator<Item = SearchQueryWithIndex>, ResponseError> {
    Err::<std::iter::Empty<SearchQueryWithIndex>, _>(ResponseError::from_msg(
        "Meilisearch Enterprise Edition is required to use `useNetwork` when `network.leader` is set".into(),
        Code::RequiresEnterpriseEdition,
    ))
}

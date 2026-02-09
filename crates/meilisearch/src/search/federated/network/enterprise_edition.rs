// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;

use meilisearch_types::error::ResponseError;
use meilisearch_types::milli::SHARD_FIELD;
use meilisearch_types::network::Network;
use rand::seq::IteratorRandom as _;

use crate::search::{fuse_filters, SearchQueryWithIndex};

/// Partition over all shards such that each shard appears exactly once.
///
/// The remote responsible for each shard is picked at random among the remotes that own the shard.
pub fn partition_shards(
    network: Network,
    query: SearchQueryWithIndex,
) -> Result<impl Iterator<Item = SearchQueryWithIndex>, ResponseError> {
    let mut rng = rand::thread_rng();

    let mut remote_for_shards: BTreeMap<_, Vec<String>> = BTreeMap::new();
    for (shard_name, shard) in network.shards {
        let Some(remote_for_shard) = shard.remotes.into_iter().choose(&mut rng) else {
            tracing::warn!("No remote for shard {shard_name}");
            continue;
        };
        remote_for_shards.entry(remote_for_shard).or_default().push(shard_name);
    }

    Ok(remote_for_shards.into_iter().map(move |(remote, shards)| {
        let mut query = query.clone();
        query.federation_options.get_or_insert_default().remote = Some(remote);

        /// FIXME: handle "strange" shard names
        let shard_filter = Some(serde_json::Value::String(format!(
            "{shard_field} IN [{shards}]",
            shard_field = SHARD_FIELD,
            shards = shards.join(", ")
        )));

        query.filter = fuse_filters(query.filter.take(), shard_filter);
        query
    }))
}

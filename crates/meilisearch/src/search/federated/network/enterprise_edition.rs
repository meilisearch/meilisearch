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
    query: SearchQueryWithIndex,
    remote_for_shard: impl Iterator<Item = (impl AsRef<str>, String)>,
) -> Result<impl Iterator<Item = SearchQueryWithIndex>, ResponseError> {
    Ok(remote_for_shard.map(move |(shard, remote)| {
        let mut query = query.clone();
        query.federation_options.get_or_insert_default().remote = Some(remote);

        let shard_filter =
            Some(serde_json::Value::String(format!("{SHARD_FIELD} = \"{}\"", shard.as_ref())));

        query.filter = fuse_filters(query.filter.take(), shard_filter);
        query
    }))
}

pub(super) fn remote_for_shard(network: Network) -> BTreeMap<String, String> {
    let mut rng = rand::thread_rng();

    let remote_for_shard = {
        network
            .shards
            .into_iter()
            .filter_map(move |(shard_name, shard)| {
                let Some(remote_for_shard) = shard.remotes.into_iter().choose(&mut rng) else {
                    tracing::warn!("No remote for shard {shard_name}");
                    return None;
                };

                Some((shard_name.escape_default().collect(), remote_for_shard))
            })
            .collect()
    };
    remote_for_shard
}

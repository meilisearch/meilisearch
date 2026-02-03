// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeSet;
use std::hash::{BuildHasher as _, BuildHasherDefault};

use super::{Shard, Shards};

impl Shards {
    pub fn from_shards_remotes_local<'a>(
        shards: impl Iterator<Item = (&'a str, &'a BTreeSet<String>)>,
        local: Option<&str>,
    ) -> Self {
        let shards = shards
            .map(|(name, remotes)| {
                let is_own = if let Some(local) = local { remotes.contains(local) } else { false };
                Shard { is_own, name: name.to_owned() }
            })
            .collect();
        Shards(shards)
    }

    pub fn processing_shard<'a>(&'a self, docid: &str) -> Option<&'a Shard> {
        let hasher = BuildHasherDefault::<twox_hash::XxHash3_64>::new();
        let to_hash = |shard: &'a Shard| (shard, hasher.hash_one((&shard.name, docid)));

        let shard =
            self.0.iter().map(to_hash).max_by_key(|(_, hash)| *hash).map(|(shard, _)| shard);
        shard
    }

    /// Computes the name of the shard that `name` belongs to.
    ///
    /// Returns `None` if `candidates` produces no element.
    pub fn shard<'a>(candidates: impl Iterator<Item = &'a str>, name: &str) -> Option<&'a str> {
        shard(candidates, name).map(|(shard, _)| shard)
    }

    /// Computes the name of the shard that `name` belongs to, if it is one of `new_candidates`.
    pub fn reshard<'a>(
        existing_candidates: impl Iterator<Item = &'a str>,
        new_candidates: impl Iterator<Item = &'a str>,
        name: &str,
    ) -> Resharding<'a> {
        match (shard(existing_candidates, name), shard(new_candidates, name)) {
            (None, None) => Resharding::Unsharded,
            (None, Some((shard, _))) | (Some((shard, _)), None) => Resharding::Sharded { shard },
            (Some((previous, losing)), Some((new, winning))) if winning > losing => {
                Resharding::Resharded { previous, new }
            }
            (Some((shard, _)), _) => Resharding::Sharded { shard },
        }
    }
}

pub enum Resharding<'a> {
    Unsharded,
    Sharded { shard: &'a str },
    Resharded { previous: &'a str, new: &'a str },
}

fn shard<'a>(candidates: impl Iterator<Item = &'a str>, name: &str) -> Option<(&'a str, u64)> {
    let hasher = BuildHasherDefault::<twox_hash::XxHash3_64>::new();
    let to_hash = |shard: &'a str| (shard, hasher.hash_one((shard, name)));

    candidates.map(to_hash).max_by_key(|(_, hash)| *hash)
}

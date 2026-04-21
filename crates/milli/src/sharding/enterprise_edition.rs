// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeSet;
use std::hash::{BuildHasher as _, BuildHasherDefault};

use itertools::Itertools;

use super::{Shard, Shards};

impl Shards {
    pub fn from_shards_remotes_local<'a>(
        shard_remotes: impl Iterator<Item = (&'a str, &'a BTreeSet<String>)>,
        local: Option<&str>,
    ) -> Self {
        let mut is_sorted = true;
        let mut shards = if let Ok(len) = shard_remotes.try_len() {
            Vec::with_capacity(len)
        } else {
            Vec::new()
        };

        let mut shard_remotes = shard_remotes.peekable();

        while let Some((shard, remotes)) = shard_remotes.next() {
            if let Some((next, _)) = shard_remotes.peek() {
                if shard > *next {
                    is_sorted = false;
                }
            }
            let is_own = if let Some(local) = local { remotes.contains(local) } else { false };
            shards.push(Shard { is_own, name: shard.to_owned() })
        }

        if !is_sorted {
            shards.sort_by(|left, right| left.name.cmp(&right.name));
        }
        Shards(shards)
    }

    pub fn processing_shard<'a>(&'a self, docid: &str) -> Option<&'a Shard> {
        let hasher = BuildHasherDefault::<twox_hash::XxHash3_64>::new();
        let to_hash = |shard: &'a Shard| (shard, hasher.hash_one((&shard.name, docid)));

        let shard =
            self.0.iter().map(to_hash).max_by_key(|(_, hash)| *hash).map(|(shard, _)| shard);
        shard
    }

    /// Given a set of `candidates` and an `object_id`, compute the hash value of all couples
    /// `(candidate, object_id)` and return the `candidate` corresponding to the max hash
    /// value over all candidates.
    ///
    /// If `candidates` is an empty iterator, then returns `None`.
    pub fn hash_rendezvous<'a>(
        candidates: impl Iterator<Item = &'a str>,
        object_id: &str,
    ) -> Option<&'a str> {
        hash_rendezvous_with_value(candidates, object_id).map(|(shard, _)| shard)
    }

    /// Given a set of `existing_candidates`, a set of `new_candidates` and an `object_id`,
    /// compute the hash value of all couples `(candidate, object_id)` and
    /// return a `Resharding` value expressing whether the candidate changed between the two sets:
    ///
    /// - `Resharding::Sharded`: if the candidate did not change
    /// - `Resharding::Resharded`: if the candidate changed
    /// - `Resharding::Unsharded`: if both sets of candidates are empty
    pub fn reshard<'a>(
        existing_candidates: impl Iterator<Item = &'a str>,
        new_candidates: impl Iterator<Item = &'a str>,
        object_id: &str,
    ) -> Resharding<'a> {
        match (
            hash_rendezvous_with_value(existing_candidates, object_id),
            hash_rendezvous_with_value(new_candidates, object_id),
        ) {
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

/// Given a set of `candidates` and an `object_id`, compute the hash value of all couples `(candidate, object_id)` and
/// return the couple `(candidate, hash_value)` corresponding to the max hash value over all candidates.
///
/// If `candidates` is an empty iterator, then returns `None`.
fn hash_rendezvous_with_value<'a>(
    candidates: impl Iterator<Item = &'a str>,
    object_id: &str,
) -> Option<(&'a str, u64)> {
    let hasher = BuildHasherDefault::<twox_hash::XxHash3_64>::new();
    let to_hash = |shard: &'a str| (shard, hasher.hash_one((shard, object_id)));

    candidates.map(to_hash).max_by_key(|(_, hash)| *hash)
}

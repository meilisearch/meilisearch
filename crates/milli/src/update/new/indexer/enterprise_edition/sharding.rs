// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::hash::{BuildHasher as _, BuildHasherDefault};

#[derive(Debug, Clone)]
pub struct Shards(pub Vec<Shard>);

#[derive(Debug, Clone)]
pub struct Shard {
    pub is_own: bool,
    pub name: String,
}

impl Shards {
    pub fn from_remotes_local<'a>(
        remotes: impl IntoIterator<Item = &'a str>,
        local: Option<&str>,
    ) -> Self {
        Shards(
            remotes
                .into_iter()
                .map(|name| Shard { is_own: Some(name) == local, name: name.to_owned() })
                .collect(),
        )
    }

    pub fn must_process(&self, docid: &str) -> bool {
        self.processing_shard(docid).map(|shard| shard.is_own).unwrap_or_default()
    }

    pub fn processing_shard<'a>(&'a self, docid: &str) -> Option<&'a Shard> {
        let hasher = BuildHasherDefault::<twox_hash::XxHash3_64>::new();
        let to_hash = |shard: &'a Shard| (shard, hasher.hash_one((&shard.name, docid)));

        let shard =
            self.0.iter().map(to_hash).max_by_key(|(_, hash)| *hash).map(|(shard, _)| shard);
        shard
    }
}

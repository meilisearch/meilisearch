// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::hash::{BuildHasher as _, BuildHasherDefault};

pub struct Shards {
    pub own: Vec<String>,
    pub others: Vec<String>,
}

impl Shards {
    pub fn must_process(&self, docid: &str) -> bool {
        let hasher = BuildHasherDefault::<twox_hash::XxHash3_64>::new();
        let to_hash = |shard: &String| hasher.hash_one((shard, docid));

        let max_hash = self.others.iter().map(to_hash).max().unwrap_or_default();

        self.own.iter().map(to_hash).any(|hash| hash > max_hash)
    }
}

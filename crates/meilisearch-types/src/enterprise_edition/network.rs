// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use milli::update::new::indexer::enterprise_edition::sharding::Shards;

use crate::network::Network;

impl Network {
    pub fn shards(&self) -> Option<Shards> {
        if self.sharding() {
            Some(Shards::from_remotes_local(
                self.remotes.keys().map(String::as_str),
                self.local.as_deref(),
            ))
        } else {
            None
        }
    }

    pub fn sharding(&self) -> bool {
        self.leader.is_some()
    }
}

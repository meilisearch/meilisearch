// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;

use itertools::{EitherOrBoth, Itertools};
use meilisearch_types::enterprise_edition::network::{DbNetwork, DbRemote, Network, Remote};
use meilisearch_types::milli;
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::tasks::{KindWithContent, Status, Task};

use crate::{Error, IndexScheduler};

impl IndexScheduler {
    pub(crate) fn process_network_changes(
        &self,
        progress: Progress,
        mut tasks: Vec<Task>,
    ) -> crate::Result<Vec<Task>> {
        let mut current_network = Some(self.network());
        for task in &tasks {
            let KindWithContent::NetworkTopologyChange { network } = &task.kind else {
                continue;
            };
            current_network = match (current_network, network) {
                (None, None) => None,
                (None, Some(network)) => Some(accumulate(DbNetwork::default(), network.clone())?),
                (Some(current_network), None) => Some(current_network),
                (Some(current_network), Some(new_network)) => {
                    Some(accumulate(current_network, new_network.clone())?)
                }
            };
        }

        if let Some(new_network) = current_network {
            self.put_network(new_network)?;
        } else {
            self.put_network(DbNetwork::default())?;
        }

        for task in &mut tasks {
            task.status = Status::Succeeded;
        }
        Ok(tasks)
    }
}

fn accumulate(old_network: DbNetwork, new_network: Network) -> crate::Result<DbNetwork> {
    let err = |err| Err(Error::from_milli(milli::Error::UserError(err), None));

    let merged_local = match new_network.local {
        Setting::Set(new_self) => Some(new_self),
        Setting::Reset => None,
        Setting::NotSet => old_network.local,
    };

    let merged_sharding = match new_network.sharding {
        Setting::Set(new_sharding) => new_sharding,
        Setting::Reset => false,
        Setting::NotSet => old_network.sharding,
    };

    if merged_sharding && merged_local.is_none() {
        return err(milli::UserError::NetworkShardingWithoutSelf);
    }

    let merged_remotes = match new_network.remotes {
        Setting::Set(new_remotes) => {
            let mut merged_remotes = BTreeMap::new();
            for either_or_both in old_network
                .remotes
                .into_iter()
                .merge_join_by(new_remotes.into_iter(), |left, right| left.0.cmp(&right.0))
            {
                match either_or_both {
                    EitherOrBoth::Both((name, old), (_, Some(new))) => {
                        let DbRemote {
                            url: old_url,
                            search_api_key: old_search_api_key,
                            write_api_key: old_write_api_key,
                        } = old;

                        let Remote {
                            url: new_url,
                            search_api_key: new_search_api_key,
                            write_api_key: new_write_api_key,
                        } = new;

                        let merged = DbRemote {
                            url: match new_url {
                                Setting::Set(new_url) => new_url,
                                Setting::Reset => {
                                    return err(milli::UserError::NetworkMissingUrl(name))
                                }
                                Setting::NotSet => old_url,
                            },
                            search_api_key: match new_search_api_key {
                                Setting::Set(new_search_api_key) => Some(new_search_api_key),
                                Setting::Reset => None,
                                Setting::NotSet => old_search_api_key,
                            },
                            write_api_key: match new_write_api_key {
                                Setting::Set(new_write_api_key) => Some(new_write_api_key),
                                Setting::Reset => None,
                                Setting::NotSet => old_write_api_key,
                            },
                        };
                        merged_remotes.insert(name, merged);
                    }
                    EitherOrBoth::Both((_, _), (_, None)) | EitherOrBoth::Right((_, None)) => {}
                    EitherOrBoth::Left((name, node)) => {
                        merged_remotes.insert(name, node);
                    }
                    EitherOrBoth::Right((name, Some(node))) => {
                        let Some(url) = node.url.set() else {
                            return err(milli::UserError::NetworkMissingUrl(name));
                        };
                        let node = DbRemote {
                            url,
                            search_api_key: node.search_api_key.set(),
                            write_api_key: node.write_api_key.set(),
                        };
                        merged_remotes.insert(name, node);
                    }
                }
            }
            merged_remotes
        }
        Setting::Reset => BTreeMap::new(),
        Setting::NotSet => old_network.remotes,
    };

    Ok(DbNetwork { local: merged_local, remotes: merged_remotes, sharding: merged_sharding })
}

// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;
use std::time::Duration;

use bumpalo::Bump;
use itertools::{EitherOrBoth, Itertools};
use meilisearch_types::enterprise_edition::network::{DbNetwork, DbRemote, Network, Remote};
use meilisearch_types::milli::documents::PrimaryKey;
use meilisearch_types::milli::progress::{EmbedderStats, Progress};
use meilisearch_types::milli::update::new::indexer;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::milli::{self};
use meilisearch_types::tasks::{KindWithContent, Status, Task};
use roaring::RoaringBitmap;

use crate::scheduler::process_export::{ExportContext, ExportOptions, TargetInstance};
use crate::{Error, IndexScheduler};

impl IndexScheduler {
    pub(crate) fn process_network_changes(
        &self,
        progress: Progress,
        mut tasks: Vec<Task>,
    ) -> crate::Result<Vec<Task>> {
        let old_network = self.network();
        let mut current_network = Some(old_network.clone());
        for task in &tasks {
            let KindWithContent::NetworkTopologyChange { network, origin } = &task.kind else {
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

        'network: {
            let mut new_network = current_network.unwrap_or_default();
            if old_network == new_network {
                // no change, exit
                break 'network;
            }

            /// TODO: only do this if the task originates with an end-user
            let must_replicate = old_network.sharding || new_network.sharding;

            if !must_replicate {
                self.put_network(new_network)?;
                break 'network;
            }

            let must_stop_processing = &self.scheduler.must_stop_processing;

            /// FIXME: make it mandatory for `self` to be part of the network
            let old_this = old_network.local.as_deref();
            /// FIXME: error here
            let new_this = new_network.local.unwrap();

            // in network replication, we need to tell old nodes that they are no longer part of the network.
            // This is made difficult by "node aliasing": Meilisearch has no way of knowing if two nodes with different names
            // or even different URLs actually refer to the same machine in two different versions of the network.
            //
            // This implementation ignores aliasing: a node is the same when it has the same name.
            //
            // To defeat aliasing, we iterate a first time to collect all deletions and additions, then we make sure to process the deletions
            // first, rather than processing the tasks in the alphalexical order of remotes.
            let mut node_deletions = Vec::new();
            let mut node_additions = Vec::new();
            for eob in old_network
                .remotes
                .iter()
                .merge_join_by(new_network.remotes.iter(), |(left, _), (right, _)| left.cmp(right))
            {
                match eob {
                    EitherOrBoth::Both((to_update_name, _), (_, new_node)) => {
                        if to_update_name.as_str() == new_this {
                            continue; // skip `self`
                        }
                        node_additions.push((to_update_name, new_node));
                    }
                    EitherOrBoth::Left((to_delete_name, to_delete_node)) => {
                        if Some(to_delete_name.as_str()) == old_this {
                            continue; // skip `self`
                        }
                        node_deletions.push((to_delete_name, to_delete_node));
                    }
                    EitherOrBoth::Right((to_add_name, to_add_node)) => {
                        if to_add_name.as_str() == new_this {
                            continue; // skip `self`
                        }
                        node_additions.push((to_add_name, to_add_node));
                    }
                }
            }

            let runtime = self.runtime.clone().unwrap();
            let mut in_flight = Vec::new();
            // process deletions
            for (to_delete_name, to_delete) in node_deletions {
                // set `self` to None so that this node is forgotten about
                new_network.local = None;
                in_flight.push(proxy_network(&runtime, to_delete.url.as_str(), &new_network)?);
            }

            runtime.block_on(async {
                for task in in_flight.drain(..) {
                    // TODO: log and ignore errors during deletion
                    let res = task.await;
                }
            });

            // process additions
            for (to_add_name, to_add) in node_additions {
                new_network.local = Some(to_add_name.clone());
                in_flight.push(proxy_network(&runtime, to_add.url.as_str(), &new_network)?);
            }

            runtime.block_on(async {
                for task in in_flight.drain(..) {
                    // TODO: handle errors during addition
                    let res = task.await;
                }
            });

            // balance documents
            new_network.local = Some(new_this);

            self.balance_documents(&new_network, &progress, &must_stop_processing)?;

            self.put_network(new_network)?;
        }

        for task in &mut tasks {
            task.status = Status::Succeeded;
        }
        Ok(tasks)
    }

    fn balance_documents(
        &self,
        new_network: &DbNetwork,
        progress: &Progress,
        must_stop_processing: &crate::scheduler::MustStopProcessing,
    ) -> crate::Result<()> {
        /// FIXME unwrap
        let new_shards = new_network.shards().unwrap();

        // TECHDEBT: this spawns a `ureq` agent additionally to `reqwest`. We probably want to harmonize all of this.
        let agent = ureq::AgentBuilder::new().timeout(Duration::from_secs(5)).build();

        let mut indexer_alloc = Bump::new();

        // process by batches of 20MiB. Allow for compression? Don't forget about embeddings
        let _: Vec<()> = self.try_for_each_index(|index_uid, index| -> crate::Result<()> {
            indexer_alloc.reset();
            let err = |err| Error::from_milli(err, Some(index_uid.to_string()));
            let index_rtxn = index.read_txn()?;
            let all_docids = index.external_documents_ids();
            let mut documents_to_move_to: hashbrown::HashMap<String, RoaringBitmap> =
                hashbrown::HashMap::new();
            let mut documents_to_delete = RoaringBitmap::new();

            for res in all_docids.iter(&index_rtxn)? {
                let (external_docid, docid) = res?;
                match new_shards.processing_shard(external_docid) {
                    Some(shard) if shard.is_own => continue,
                    Some(shard) => {
                        documents_to_move_to
                            .entry_ref(shard.name.as_str())
                            .or_default()
                            .insert(docid);
                    }
                    None => {
                        documents_to_delete.insert(docid);
                    }
                }
            }

            let fields_ids_map = index.fields_ids_map(&index_rtxn)?;

            for (remote, documents_to_move) in documents_to_move_to {
                /// TODO: justify the unwrap
                let remote = new_network.remotes.get(&remote).unwrap();

                let target = TargetInstance {
                    base_url: &remote.url,
                    api_key: remote.write_api_key.as_deref(),
                };
                let options = ExportOptions {
                    index_uid,
                    payload_size: None,
                    override_settings: false,
                    extra_headers: &Default::default(),
                };
                let ctx = ExportContext {
                    index,
                    index_rtxn: &index_rtxn,
                    universe: &documents_to_move,
                    progress,
                    agent: &agent,
                    must_stop_processing,
                };

                self.export_one_index(target, options, ctx)?;

                documents_to_delete |= documents_to_move;
            }

            if documents_to_delete.is_empty() {
                return Ok(());
            }

            let mut new_fields_ids_map = fields_ids_map.clone();

            // candidates not empty => index not empty => a primary key is set
            let primary_key = index.primary_key(&index_rtxn)?.unwrap();

            let primary_key = PrimaryKey::new_or_insert(primary_key, &mut new_fields_ids_map)
                .map_err(milli::Error::from)
                .map_err(err)?;

            let mut index_wtxn = index.write_txn()?;

            let mut indexer = indexer::DocumentDeletion::new();
            indexer.delete_documents_by_docids(documents_to_delete);
            let document_changes = indexer.into_changes(&indexer_alloc, primary_key);
            let embedders = index
                .embedding_configs()
                .embedding_configs(&index_wtxn)
                .map_err(milli::Error::from)
                .map_err(err)?;
            let embedders = self.embedders(index_uid, embedders)?;
            let indexer_config = self.index_mapper.indexer_config();
            let pool = &indexer_config.thread_pool;

            indexer::index(
                &mut index_wtxn,
                index,
                pool,
                indexer_config.grenad_parameters(),
                &fields_ids_map,
                new_fields_ids_map,
                None, // document deletion never changes primary key
                &document_changes,
                embedders,
                &|| must_stop_processing.get(),
                &progress,
                &EmbedderStats::default(),
            )
            .map_err(err)?;

            index_wtxn.commit()?;

            Ok(())
        })?;
        Ok(())
    }
}

fn proxy_network(
    runtime: &tokio::runtime::Handle,
    url: &str,
    network: &DbNetwork,
) -> crate::Result<tokio::task::JoinHandle<()>> {
    todo!()
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

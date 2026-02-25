// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeSet;
use std::time::Duration;

use bumpalo::Bump;
use hashbrown::hash_map::EntryRef;
use http_client::reqwest::header::AUTHORIZATION;
use meilisearch_types::heed::RoTxn;
use meilisearch_types::milli::documents::PrimaryKey;
use meilisearch_types::milli::heed::RwTxn;
use meilisearch_types::milli::progress::{EmbedderStats, Progress, VariableNameStep};
use meilisearch_types::milli::sharding::enterprise_edition::Resharding;
use meilisearch_types::milli::sharding::{DbShardDocids, ShardBalancingOutcome, Shards};
use meilisearch_types::milli::update::new::indexer;
use meilisearch_types::milli::vector::RuntimeEmbedders;
use meilisearch_types::milli::{self, MustStopProcessing};
use meilisearch_types::network::{route, Remote};
use meilisearch_types::tasks::network::{ExportMode, ExportShard, NetworkTopologyState, Origin};
use meilisearch_types::tasks::{KindWithContent, Status, Task};
use roaring::RoaringBitmap;

use crate::scheduler::create_batch::Batch;
use crate::scheduler::process_batch::ProcessBatchInfo;
use crate::scheduler::process_export::{ExportContext, ExportOptions, TargetInstance};
use crate::utils::ProcessingBatch;
use crate::{processing, Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(in crate::scheduler) fn process_network_index_batch(
        &self,
        mut network_task: Task,
        inner_batch: Box<Batch>,
        current_batch: &mut ProcessingBatch,
        progress: Progress,
    ) -> Result<(Vec<Task>, ProcessBatchInfo)> {
        let KindWithContent::NetworkTopologyChange(network_topology_change) =
            &mut network_task.kind
        else {
            tracing::error!("unexpected network kind for network task while processing batch");
            return Err(Error::CorruptedTaskQueue);
        };

        progress.update_progress(processing::network::NetworkTopologyState::from(
            network_topology_change.state(),
        ));

        let network = network_topology_change.network_for_state();

        let (mut tasks, info) =
            self.process_batch(*inner_batch, current_batch, progress, network)?;

        for task in &tasks {
            let Some(network) = task.network.as_ref() else {
                continue;
            };
            let Some(import) = network.import_data() else {
                continue;
            };
            if let Some(index_name) = import.index_name.as_deref() {
                network_topology_change.process_remote_tasks(
                    &import.remote_name,
                    index_name,
                    import.document_count,
                );
            }
        }
        network_task.details = Some(network_topology_change.to_details());

        tasks.push(network_task);
        Ok((tasks, info))
    }

    pub(in crate::scheduler) fn process_network_ready(
        &self,
        mut task: Task,
        progress: Progress,
    ) -> Result<(Vec<Task>, ProcessBatchInfo)> {
        let KindWithContent::NetworkTopologyChange(network_topology_change) = &mut task.kind else {
            tracing::error!("network topology change task has the wrong kind with content");
            return Err(Error::CorruptedTaskQueue);
        };

        let Some(task_network) = &task.network else {
            tracing::error!("network topology change task has no network");
            return Err(Error::CorruptedTaskQueue);
        };

        let origin;
        let origin = match task_network.origin() {
            Some(origin) => origin,
            None => {
                let myself =
                    network_topology_change.name_for_import().expect("origin is not the leader");
                origin = Origin {
                    remote_name: myself.to_string(),
                    task_uid: task.uid,
                    network_version: task_network.network_version(),
                };
                &origin
            }
        };

        progress.update_progress(processing::network::NetworkTopologyState::from(
            network_topology_change.state(),
        ));

        if let (Some((remotes, out_name)), Some(new_shards)) =
            (network_topology_change.export_to_process(), network_topology_change.new_shards())
        {
            self.balance_documents(
                remotes,
                out_name,
                new_shards,
                origin,
                &progress,
                &self.scheduler.must_stop_processing,
            )?
        }

        if let Some((remotes, in_name)) = network_topology_change.finished_import_to_notify() {
            self.notify_import_finished(remotes, in_name.to_owned(), origin)?;
        }

        let remotes_import_state = network_topology_change.remotes_import_state();
        if remotes_import_state.all_finished_successfully() {
            let moved_documents = self.delete_removed_shards(
                network_topology_change.removed_shard_names(),
                &progress,
                &self.scheduler.must_stop_processing,
            )?;
            network_topology_change.set_moved(moved_documents);
        } else if network_topology_change.state() == NetworkTopologyState::WaitingForOlderTasks {
            progress.update_progress(VariableNameStep::<processing::network::ImportRemotes>::new(
                "Waiting for other remotes to finish importing".to_string(),
                remotes_import_state.finished() as u32,
                remotes_import_state.total() as u32,
            ));
        }

        network_topology_change.update_state();

        progress.update_progress(processing::network::NetworkTopologyState::from(
            network_topology_change.state(),
        ));

        if network_topology_change.state() == NetworkTopologyState::Finished {
            task.status = Status::Succeeded;
        }

        task.details = Some(network_topology_change.to_details());
        Ok((vec![task], Default::default()))
    }

    fn balance_documents<
        'a,
        I: Iterator<Item = (&'a str, &'a Remote, J)> + Clone,
        J: Iterator<Item = ExportShard<'a>> + Clone,
    >(
        &self,
        remotes: I,
        out_name: &str,
        new_shards: Shards,
        network_change_origin: &Origin,
        progress: &Progress,
        must_stop_processing: &MustStopProcessing,
    ) -> crate::Result<()> {
        // TECHDEBT: this spawns a `ureq` agent additionally to `reqwest`. We probably want to harmonize all of this.
        let config = http_client::ureq::config::Config::builder()
            .prepare(|config| {
                config.timeout_global(Some(Duration::from_secs(5))).http_status_as_error(false)
            })
            .build();

        let agent =
            http_client::ureq::Agent::new_with_config(config, self.scheduler.ip_policy.clone());

        let scheduler_rtxn = self.env.read_txn()?;

        let index_count = self.index_mapper.index_count(&scheduler_rtxn)?;

        // when the instance is empty, we still need to tell that to remotes, as they cannot know of that fact and will be waiting for
        // data
        if index_count == 0 {
            for (remote_name, remote, _) in remotes {
                let target = TargetInstance {
                    remote_name: Some(remote_name),
                    base_url: &remote.url,
                    api_key: remote.write_api_key.as_deref(),
                };

                let res = self.export_no_index(
                    target,
                    out_name,
                    network_change_origin,
                    &agent,
                    must_stop_processing,
                );

                if let Err(err) = res {
                    tracing::warn!("Could not signal not to wait documents to `{remote_name}` due to error: {err}");
                }
            }
            return Ok(());
        }

        let mut index_index = 0;

        // shard rebalancing
        //
        // when a shard is removed, its documents must be redistributed among the other shards.
        // when this happens, we only want to send the portions of the other shards that changed.
        // in other words, for each (remote, shard):
        // 1. if shard is new for remote, and local is responsible for sending shard, send full shard
        // 2. otherwise, send shard ^ resharded documents
        self.index_mapper.try_for_each_index::<(), ()>(
            &scheduler_rtxn,
            |index_uid, index| -> crate::Result<()> {
                let err = |err| Error::from_milli(err, Some(index_uid.to_string()));

                let mut index_wtxn = index.write_txn()?;

                progress.update_progress(
                    VariableNameStep::<processing::network::ExportIndex>::new(
                        format!("Exporting documents from index `{index_uid}`"),
                        index_index,
                        index_count as u32,
                    ),
                );
                index_index += 1;

                let shard_docids = index.shard_docids();
                let ShardBalancingOutcome { unsharded, new_shards, existing_shards } = shard_docids
                    .rebalance_shards(index.documents_ids(&index_wtxn)?, &mut index_wtxn, &new_shards)
                    .map_err(err)?;

                // if we have unsharded documents or new shards, we need to reshard
                let (mut new_shard_docids, resharded) = balance_shards(
                    index,
                    &mut index_wtxn,
                    &shard_docids,
                    unsharded,
                    new_shards,
                    existing_shards,
                )
                .map_err(err)?;

                for (remote_name, remote, export_shards) in remotes.clone() {
                    let mut documents_to_move = RoaringBitmap::new();
                    for export_shard in export_shards {
                        let docids = docids_for_shard(
                            &mut new_shard_docids,
                            export_shard.name,
                            &index_wtxn,
                            &shard_docids,
                        )
                        .map_err(err)?;

                        match export_shard.mode {
                            ExportMode::ReshardedOnly => documents_to_move |= &*docids & &resharded,
                            ExportMode::FullShard => documents_to_move |= &*docids,
                        }
                    }

                    let target = TargetInstance {
                        remote_name: Some(remote_name),
                        base_url: &remote.url,
                        api_key: remote.write_api_key.as_deref(),
                    };
                    let options = ExportOptions {
                        index_uid,
                        payload_size: None,
                        override_settings: false,
                        export_mode:
                            crate::scheduler::process_export::ExportMode::NetworkBalancing {
                                index_count,
                                export_old_remote_name: out_name,
                                network_change_origin,
                            },
                    };
                    let ctx = ExportContext {
                        index,
                        index_rtxn: &index_wtxn,
                        universe: &documents_to_move,
                        progress,
                        agent: &agent,
                        must_stop_processing,
                    };

                    let res = self.export_one_index(target, options, ctx);

                    match res {
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!("Could not export documents to `{remote_name}` due to error: {err}\n  - Note: Documents will be kept");
                        }
                    }
                }

                index_wtxn.commit()?;

                Ok(())
            },
        )?;

        progress.update_progress(VariableNameStep::<processing::network::ExportIndex>::new(
            "Done exporting documents".to_string(),
            index_count as u32,
            index_count as u32,
        ));

        Ok(())
    }

    fn delete_removed_shards<'a>(
        &self,
        removed_shards: impl Iterator<Item = &'a str> + Clone,
        progress: &Progress,
        must_stop_processing: &MustStopProcessing,
    ) -> crate::Result<u64> {
        let mut deleted_documents = 0;
        let mut indexer_alloc = Bump::new();

        let scheduler_rtxn = self.env.read_txn()?;

        let index_count = self.index_mapper.index_count(&scheduler_rtxn)?;
        let mut index_index = 0;

        self.index_mapper.try_for_each_index::<(), ()>(
            &scheduler_rtxn,
            |index_uid, index| -> crate::Result<()> {
                indexer_alloc.reset();
                let mut index_wtxn = index.write_txn()?;

                let err = |err| Error::from_milli(err, Some(index_uid.to_string()));

                let embedders = index
                    .embedding_configs()
                    .embedding_configs(&index_wtxn)
                    .map_err(milli::Error::from)
                    .map_err(err)?;
                let embedders = self.embedders(index_uid.to_string(), embedders)?;

                let shard_docids = index.shard_docids();

                progress.update_progress(VariableNameStep::<
                    processing::network::DeleteDocumentsFromIndex,
                >::new(
                    format!("Deleting removed shards for index `{index_uid}`"),
                    index_index,
                    index_count as u32,
                ));
                index_index += 1;

                let mut documents_to_delete = RoaringBitmap::new();
                for shard in removed_shards.clone() {
                    let Some(shard_docids) =
                        shard_docids.docids(&index_wtxn, shard).map_err(err)?
                    else {
                        continue;
                    };
                    documents_to_delete |= shard_docids;
                }

                deleted_documents += documents_to_delete.len();
                self.delete_documents_from_index(
                    index,
                    &mut index_wtxn,
                    documents_to_delete,
                    embedders,
                    &indexer_alloc,
                    progress,
                    must_stop_processing,
                )
                .map_err(err)?;

                // update stats
                let mut mapper_wtxn = self.env.write_txn()?;
                let stats =
                    crate::index_mapper::IndexStats::new(index, &index_wtxn).map_err(err)?;
                self.index_mapper.store_stats_of(&mut mapper_wtxn, index_uid, &stats)?;

                index_wtxn.commit()?;
                // update stats after committing changes to index
                mapper_wtxn.commit()?;

                Ok(())
            },
        )?;
        progress.update_progress(
            VariableNameStep::<processing::network::DeleteDocumentsFromIndex>::new(
                "Done deleting removed shards from indexes".to_string(),
                index_count as u32,
                index_count as u32,
            ),
        );

        Ok(deleted_documents)
    }

    pub(in crate::scheduler) fn notify_import_finished<
        'a,
        I: Iterator<Item = (&'a str, &'a Remote)>,
    >(
        &self,
        remotes: I,
        in_name: String,
        origin: &Origin,
    ) -> crate::Result<()> {
        let Some(runtime) = &self.runtime else { return Ok(()) };

        runtime.block_on(self.notify_import_finished_async(remotes, in_name, origin))?;

        Ok(())
    }

    async fn notify_import_finished_async<'a, I: Iterator<Item = (&'a str, &'a Remote)>>(
        &self,
        remotes: I,
        in_name: String,
        origin: &Origin,
    ) -> crate::Result<()> {
        let client = http_client::reqwest::ClientBuilder::new()
            .build_with_policies(self.ip_policy().clone(), Default::default())
            .unwrap();

        let body = route::NetworkChange {
            origin: origin.clone(),
            message: route::Message::ImportFinishedForRemote { remote: in_name, successful: true },
        };

        for (remote_name, remote) in remotes {
            let bearer = remote.write_api_key.as_deref().map(|api_key| format!("Bearer {api_key}"));

            let url =
                match route::url_from_base_and_route(&remote.url, route::network_control_path()) {
                    Ok(url) => url,
                    Err(err) => {
                        tracing::warn!("could not build url to {remote_name}: {err}");
                        continue;
                    }
                };

            let request = client
                .post(url.to_string())
                .prepare(|mut request| {
                    request = request.header("Content-Type", "application/json");
                    if let Some(bearer) = &bearer {
                        request = request.header(AUTHORIZATION, bearer);
                    }
                    request.json(&body)
                })
                .send();

            // we don't really care when this task finishes so we can detach it and let it live.
            tokio::spawn(request);
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn delete_documents_from_index(
        &self,
        index: &milli::Index,
        index_wtxn: &mut RwTxn<'_>,
        documents_to_delete: RoaringBitmap,
        embedders: RuntimeEmbedders,
        indexer_alloc: &Bump,
        progress: &Progress,
        must_stop_processing: &milli::MustStopProcessing,
    ) -> crate::Result<(), milli::Error> {
        let index_rtxn = index.read_txn()?;
        let fields_ids_map = index.fields_ids_map(&index_rtxn)?;
        let mut new_fields_ids_map = fields_ids_map.clone();

        // candidates not empty => index not empty => a primary key is set
        let primary_key = index.primary_key(&index_rtxn)?.unwrap();

        let primary_key = PrimaryKey::new_or_insert(primary_key, &mut new_fields_ids_map)
            .map_err(milli::Error::from)?;

        let mut indexer = indexer::DocumentDeletion::new();
        indexer.delete_documents_by_docids(documents_to_delete);
        let document_changes = indexer.into_changes(indexer_alloc, primary_key);
        let indexer_config = self.index_mapper.indexer_config();
        let pool = &indexer_config.thread_pool;

        indexer::index(
            index_wtxn,
            index,
            pool,
            indexer_config.grenad_parameters(),
            &fields_ids_map,
            new_fields_ids_map,
            None, // document deletion never changes primary key
            &document_changes,
            embedders,
            &|| must_stop_processing.get(),
            progress,
            self.ip_policy(),
            &EmbedderStats::default(),
        )?;

        Ok(())
    }
}

fn docids_for_shard<'a>(
    new_shard_docids: &'a mut hashbrown::HashMap<String, RoaringBitmap>,
    shard: &str,
    rtxn: &RoTxn<'_>,
    shard_docids: &DbShardDocids,
) -> Result<&'a mut RoaringBitmap, milli::Error> {
    Ok(match new_shard_docids.entry_ref(shard) {
        EntryRef::Occupied(occupied_entry) => occupied_entry.into_mut(),
        EntryRef::Vacant(vacant_entry_ref) => {
            vacant_entry_ref.insert(shard_docids.docids(rtxn, shard)?.unwrap_or_default())
        }
    })
}

fn balance_shards(
    index: &milli::Index,
    index_wtxn: &mut RwTxn<'_>,
    shard_docids: &DbShardDocids,
    mut unsharded: RoaringBitmap,
    new_shards: BTreeSet<&str>,
    existing_shards: BTreeSet<String>,
) -> Result<(hashbrown::HashMap<String, RoaringBitmap>, RoaringBitmap), milli::Error> {
    let mut new_shard_docids = hashbrown::HashMap::<String, RoaringBitmap>::new();

    if unsharded.is_empty() && new_shards.is_empty() {
        return Ok((new_shard_docids, unsharded));
    }

    // set the correct shard for each document
    // we must iterate over all documents rather than just unsharded because:
    // 1. there can be new shards, causing resharding
    // 2. we don't have a good way to filter on external docids from a roaring anyway
    let all_docids = index.external_documents_ids();

    for res in all_docids.iter(&*index_wtxn)? {
        let (external_docid, docid) = res?;

        let new = if unsharded.contains(docid) {
            let Some(shard) = Shards::hash_rendezvous(
                existing_shards.iter().map(|x| x.as_str()).chain(new_shards.iter().copied()),
                external_docid,
            ) else {
                continue;
            };

            shard
        } else if !new_shards.is_empty() {
            match Shards::reshard(
                existing_shards.iter().map(|x| x.as_str()),
                new_shards.iter().copied(),
                external_docid,
            ) {
                Resharding::Unsharded | Resharding::Sharded { shard: _ } => continue,
                Resharding::Resharded { previous, new } => {
                    let docids = docids_for_shard(
                        &mut new_shard_docids,
                        previous,
                        index_wtxn,
                        shard_docids,
                    )?;

                    docids.remove(docid);

                    // we reuse `unsharded` as a future `resharded` here.
                    // modifying during the iteration is not an issue because each docid will be visited only once.
                    unsharded.insert(docid);

                    new
                }
            }
        } else {
            continue;
        };

        let docids = docids_for_shard(&mut new_shard_docids, new, index_wtxn, shard_docids)?;
        docids.insert(docid);
    }

    for (shard, docids) in &new_shard_docids {
        shard_docids.put_docids(index_wtxn, shard, docids)?;
    }

    let resharded = unsharded;

    Ok((new_shard_docids, resharded))
}

// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;
use std::time::Duration;

use bumpalo::Bump;
use meilisearch_types::milli::documents::PrimaryKey;
use meilisearch_types::milli::progress::{EmbedderStats, Progress};
use meilisearch_types::milli::update::new::indexer;
use meilisearch_types::milli::update::new::indexer::current_edition::sharding::Shards;
use meilisearch_types::milli::{self};
use meilisearch_types::network::Remote;
use meilisearch_types::tasks::network::{NetworkTopologyState, Origin};
use meilisearch_types::tasks::{KindWithContent, Status, Task};
use roaring::RoaringBitmap;

use super::create_batch::Batch;
use crate::scheduler::process_batch::ProcessBatchInfo;
use crate::scheduler::process_export::{ExportContext, ExportOptions, TargetInstance};
use crate::utils::ProcessingBatch;
use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_network_index_batch(
        &self,
        mut network_task: Task,
        inner_batch: Box<Batch>,
        current_batch: &mut ProcessingBatch,
        progress: Progress,
    ) -> Result<(Vec<Task>, ProcessBatchInfo)> {
        let (mut tasks, info) = self.process_batch(*inner_batch, current_batch, progress)?;
        let KindWithContent::NetworkTopologyChange(network_topology_change) =
            &mut network_task.kind
        else {
            tracing::error!("unexpected network kind for network task while processing batch");
            return Err(Error::CorruptedTaskQueue);
        };
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

    pub(super) fn process_network_ready(
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
                let myself = network_topology_change.in_name().expect("origin is not the leader");
                origin = Origin {
                    remote_name: myself.to_string(),
                    task_uid: task.uid,
                    network_version: task_network.network_version(),
                };
                &origin
            }
        };

        if let Some((remotes, out_name)) = network_topology_change.export_to_process() {
            let moved_documents = self.balance_documents(
                remotes,
                out_name,
                network_topology_change.in_name(),
                origin,
                &progress,
                &self.scheduler.must_stop_processing,
            )?;
            network_topology_change.set_moved(moved_documents);
        }
        network_topology_change.update_state();
        if network_topology_change.state() == NetworkTopologyState::Finished {
            task.status = Status::Succeeded;
        }

        task.details = Some(network_topology_change.to_details());
        Ok((vec![task], Default::default()))
    }

    fn balance_documents(
        &self,
        remotes: &BTreeMap<String, Remote>,
        out_name: &str,
        in_name: Option<&str>,
        network_change_origin: &Origin,
        progress: &Progress,
        must_stop_processing: &crate::scheduler::MustStopProcessing,
    ) -> crate::Result<u64> {
        let new_shards =
            Shards::from_remotes_local(remotes.keys().map(String::as_str).chain(in_name), in_name);

        // TECHDEBT: this spawns a `ureq` agent additionally to `reqwest`. We probably want to harmonize all of this.
        let agent = ureq::AgentBuilder::new().timeout(Duration::from_secs(5)).build();

        let mut indexer_alloc = Bump::new();

        let scheduler_rtxn = self.env.read_txn()?;

        let index_count = self.index_mapper.index_count(&scheduler_rtxn)?;

        // when the instance is empty, we still need to tell that to remotes, as they cannot know of that fact and will be waiting for
        // data
        if index_count == 0 {
            for (remote_name, remote) in remotes {
                let target = TargetInstance {
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
            return Ok(0);
        }

        let mut total_moved_documents = 0;

        self.index_mapper.try_for_each_index::<(), ()>(
            &scheduler_rtxn,
            |index_uid, index| -> crate::Result<()> {
                indexer_alloc.reset();
                let err = |err| Error::from_milli(err, Some(index_uid.to_string()));
                let index_rtxn = index.read_txn()?;
                let all_docids = index.external_documents_ids();
                let mut documents_to_move_to =
                    hashbrown::HashMap::<String, RoaringBitmap>::new();
                let mut documents_to_delete = RoaringBitmap::new();

                for res in all_docids.iter(&index_rtxn)? {
                    let (external_docid, docid) = res?;
                    match new_shards.processing_shard(external_docid) {
                        Some(shard) if shard.is_own => continue,
                        Some(shard) => {
                            documents_to_move_to.entry_ref(&shard.name).or_default().insert(docid);
                        }
                        None => {
                            documents_to_delete.insert(docid);
                        }
                    }
                }

                let fields_ids_map = index.fields_ids_map(&index_rtxn)?;

                for (remote_name, remote) in remotes {
                    let documents_to_move =
                        documents_to_move_to.remove(remote_name).unwrap_or_default();

                    let target = TargetInstance {
                        base_url: &remote.url,
                        api_key: remote.write_api_key.as_deref(),
                    };
                    let options = ExportOptions {
                        index_uid,
                        payload_size: None,
                        override_settings: false,
                        export_mode: super::process_export::ExportMode::NetworkBalancing {
                            index_count,
                            export_old_remote_name: out_name,
                            network_change_origin,
                        },
                    };
                    let ctx = ExportContext {
                        index,
                        index_rtxn: &index_rtxn,
                        universe: &documents_to_move,
                        progress,
                        agent: &agent,
                        must_stop_processing,
                    };

                    let res = self.export_one_index(target, options, ctx);

                    match res {
                        Ok(_) =>{ documents_to_delete |= documents_to_move;}
                        Err(err) => {
                            tracing::warn!("Could not export documents to `{remote_name}` due to error: {err}\n  - Note: Documents will be kept");
                        }
                    }


                }

                if documents_to_delete.is_empty() {
                    return Ok(());
                }

                total_moved_documents += documents_to_delete.len();

                self.delete_documents_from_index(progress, must_stop_processing, &indexer_alloc, index_uid, index, &err, index_rtxn, documents_to_delete, fields_ids_map)
            },
        )?;

        Ok(total_moved_documents)
    }

    #[allow(clippy::too_many_arguments)]
    fn delete_documents_from_index(
        &self,
        progress: &Progress,
        must_stop_processing: &super::MustStopProcessing,
        indexer_alloc: &Bump,
        index_uid: &str,
        index: &milli::Index,
        err: &impl Fn(milli::Error) -> Error,
        index_rtxn: milli::heed::RoTxn<'_, milli::heed::WithoutTls>,
        documents_to_delete: RoaringBitmap,
        fields_ids_map: milli::FieldsIdsMap,
    ) -> std::result::Result<(), Error> {
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
        let embedders = self.embedders(index_uid.to_string(), embedders)?;
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
            progress,
            &EmbedderStats::default(),
        )
        .map_err(err)?;

        // update stats
        let mut mapper_wtxn = self.env.write_txn()?;
        let stats = crate::index_mapper::IndexStats::new(index, &index_wtxn).map_err(err)?;
        self.index_mapper.store_stats_of(&mut mapper_wtxn, index_uid, &stats)?;

        index_wtxn.commit()?;
        // update stats after committing changes to index
        mapper_wtxn.commit()?;

        Ok(())
    }
}

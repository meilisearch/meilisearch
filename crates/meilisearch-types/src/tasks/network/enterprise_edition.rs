// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;

use itertools::EitherOrBoth;
use milli::sharding::Shards;
use milli::DocumentId;
use roaring::RoaringBitmap;

use super::TaskKeys;
use crate::network::Remote;
use crate::tasks::network::{
    ExportState, ImportIndexState, ImportState, InRemote, NetworkTopologyChange,
    NetworkTopologyState, OutRemote, ReceiveImportFinishedError, ReceiveTaskError,
    RemotesImportState,
};

pub struct ExportShard<'a> {
    /// name of the shard
    pub name: &'a str,
    /// whether this instance is responsible for sending this shard to the remote
    pub mode: ExportMode,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ExportMode {
    /// Only send resharded documents
    ReshardedOnly,
    /// Send whole shard
    FullShard,
}

impl NetworkTopologyChange {
    pub fn export_to_process(
        &self,
    ) -> Option<(
        impl Iterator<Item = (&str, &Remote, impl Iterator<Item = ExportShard<'_>> + Clone)> + Clone,
        &str,
    )> {
        if self.state != NetworkTopologyState::ExportingDocuments {
            return None;
        }

        let out_name = self.name_for_export()?;
        Some((
            self.new_network.remotes.iter().filter_map(move |(remote_name, remote)| {
                // don't export to ourselves
                if Some(remote_name.as_str()) == self.name_for_import() {
                    return None;
                }

                let out_name_clone = out_name.to_owned();

                let it = self.new_network.shards.iter().filter_map(move |(shard_name, shard)| {
                    if !shard.remotes.contains(remote_name) {
                        return None;
                    }
                    let is_new = self
                        .old_network
                        .shards
                        .get(shard_name)
                        .is_some_and(|shard| !shard.remotes.contains(remote_name));

                    let mode = 'mode: {
                        if !is_new {
                            break 'mode ExportMode::ReshardedOnly;
                        }

                        let Some(shard) = self.old_network.shards.get(shard_name) else {
                            break 'mode ExportMode::ReshardedOnly;
                        };

                        let Some(candidate) =
                            Shards::shard(shard.remotes.iter().map(|s| s.as_str()), shard_name)
                        else {
                            break 'mode ExportMode::ReshardedOnly;
                        };

                        if candidate != out_name_clone {
                            break 'mode ExportMode::ReshardedOnly;
                        }
                        ExportMode::FullShard
                    };

                    Some(ExportShard { name: shard_name, mode })
                });

                Some((remote_name.as_str(), remote, it))
            }),
            out_name,
        ))
    }

    pub fn finished_import_to_notify(
        &self,
    ) -> Option<(impl Iterator<Item = (&str, &Remote)>, &str)> {
        let in_name = self.name_for_import()?;

        if !self.is_import_finished() {
            return None;
        }

        let it = itertools::merge_join_by(
            self.old_network.remotes.iter(),
            self.new_network.remotes.iter(),
            |(left, _), (right, _)| left.cmp(right),
        )
        .filter_map(|eob| {
            let (remote_name, remote) = match eob {
                EitherOrBoth::Both(_, remote)
                | EitherOrBoth::Left(remote)
                | EitherOrBoth::Right(remote) => remote,
            };

            // don't notify to ourselves
            if Some(remote_name.as_str()) == self.name_for_export() {
                return None;
            }

            Some((remote_name.as_str(), remote))
        });

        Some((it, in_name))
    }

    pub fn new_shards(&self) -> Option<Shards> {
        self.new_network.shards()
    }

    pub fn set_moved(&mut self, moved_documents: u64) {
        self.stats.moved_documents = moved_documents;
    }

    /// Compute the next state from the current state of the task.
    pub fn update_state(&mut self) {
        self.state = match self.state {
            NetworkTopologyState::WaitingForOlderTasks => {
                // no more older tasks, so finished waiting
                NetworkTopologyState::ExportingDocuments
            }
            NetworkTopologyState::ExportingDocuments => {
                // processed all exported documents
                if self.is_import_finished() {
                    NetworkTopologyState::WaitingForOthers
                } else {
                    NetworkTopologyState::ImportingDocuments
                }
            }
            NetworkTopologyState::ImportingDocuments => {
                if self.is_import_finished() {
                    NetworkTopologyState::WaitingForOthers
                } else {
                    NetworkTopologyState::ImportingDocuments
                }
            }
            NetworkTopologyState::WaitingForOthers => {
                if self.remotes_import_state().all_finished() {
                    NetworkTopologyState::DeletingDocuments
                } else {
                    NetworkTopologyState::WaitingForOthers
                }
            }
            NetworkTopologyState::DeletingDocuments | NetworkTopologyState::Finished => {
                NetworkTopologyState::Finished
            }
        };
    }

    pub fn receive_remote_task(
        &mut self,
        remote_name: &str,
        index_name: Option<&str>,
        task_key: Option<DocumentId>,
        document_count: u64,
        total_indexes: u64,
        total_index_documents: u64,
    ) -> Result<(), ReceiveTaskError> {
        let remote = self
            .in_remotes
            .get_mut(remote_name)
            .ok_or_else(|| ReceiveTaskError::UnknownRemote(remote_name.to_string()))?;
        remote.import_state = match std::mem::take(&mut remote.import_state) {
            ImportState::WaitingForInitialTask => {
                if total_indexes == 0 {
                    ImportState::Finished { total_indexes, total_documents: 0 }
                } else {
                    let mut task_keys = RoaringBitmap::new();
                    if let Some(index_name) = index_name {
                        if let Some(task_key) = task_key {
                            task_keys.insert(task_key);
                        }
                        let mut import_index_state = BTreeMap::new();
                        import_index_state.insert(
                            index_name.to_owned(),
                            ImportIndexState::Ongoing {
                                total_documents: total_index_documents,
                                received_documents: document_count,
                                task_keys: TaskKeys(task_keys),
                                processed_documents: 0,
                            },
                        );
                        ImportState::Ongoing { import_index_state, total_indexes }
                    } else {
                        ImportState::WaitingForInitialTask
                    }
                }
            }
            ImportState::Ongoing { mut import_index_state, total_indexes } => {
                if let Some(index_name) = index_name {
                    if let Some((index_name, mut index_state)) =
                        import_index_state.remove_entry(index_name)
                    {
                        index_state = match index_state {
                            ImportIndexState::Ongoing {
                                total_documents,
                                received_documents: previously_received,
                                processed_documents,
                                mut task_keys,
                            } => {
                                if let Some(task_key) = task_key {
                                    if !task_keys.0.insert(task_key) {
                                        return Err(ReceiveTaskError::DuplicateTask(task_key));
                                    }
                                }

                                ImportIndexState::Ongoing {
                                    total_documents,
                                    received_documents: previously_received + document_count,
                                    processed_documents,
                                    task_keys,
                                }
                            }
                            ImportIndexState::Finished { total_documents } => {
                                ImportIndexState::Finished { total_documents }
                            }
                        };
                        import_index_state.insert(index_name, index_state);
                    } else {
                        let mut task_keys = RoaringBitmap::new();
                        if let Some(task_key) = task_key {
                            task_keys.insert(task_key);
                        }
                        let state = ImportIndexState::Ongoing {
                            total_documents: total_index_documents,
                            received_documents: document_count,
                            processed_documents: 0,
                            task_keys: TaskKeys(task_keys),
                        };
                        import_index_state.insert(index_name.to_string(), state);
                    }
                    ImportState::Ongoing { import_index_state, total_indexes }
                } else {
                    ImportState::Ongoing { import_index_state, total_indexes }
                }
            }
            ImportState::Finished { total_indexes, total_documents } => {
                ImportState::Finished { total_indexes, total_documents }
            }
        };
        Ok(())
    }

    pub fn receive_import_finished(
        &mut self,
        remote_name: &str,
        successful: bool,
    ) -> Result<bool, ReceiveImportFinishedError> {
        let remote = self
            .out_remotes
            .get_mut(remote_name)
            .ok_or_else(|| ReceiveImportFinishedError::UnknownRemote(remote_name.to_string()))?;

        let changed = remote.export_state == ExportState::Ongoing;
        remote.export_state = ExportState::Finished { successful };

        Ok(changed)
    }

    pub fn process_remote_tasks(
        &mut self,
        remote_name: &str,
        index_name: &str,
        document_count: u64,
    ) {
        let remote = self
            .in_remotes
            .get_mut(remote_name)
            .expect("process_remote_tasks called on a remote that is not in `in_remotes`");
        remote.import_state = match std::mem::take(&mut remote.import_state) {
            ImportState::WaitingForInitialTask => panic!("no task received yet one processed"),
            ImportState::Ongoing { mut import_index_state, total_indexes } => {
                let (index_name, mut index_state) =
                    import_index_state.remove_entry(index_name).unwrap();
                index_state = match index_state {
                    ImportIndexState::Ongoing {
                        total_documents,
                        received_documents,
                        processed_documents: previously_processed,
                        task_keys,
                    } => {
                        let newly_processed_documents = previously_processed + document_count;
                        if newly_processed_documents >= total_documents {
                            ImportIndexState::Finished { total_documents }
                        } else {
                            ImportIndexState::Ongoing {
                                total_documents,
                                received_documents,
                                processed_documents: newly_processed_documents,
                                task_keys,
                            }
                        }
                    }
                    ImportIndexState::Finished { total_documents } => {
                        ImportIndexState::Finished { total_documents }
                    }
                };
                import_index_state.insert(index_name, index_state);
                if import_index_state.len() as u64 == total_indexes
                    && import_index_state.values().all(|index| index.is_finished())
                {
                    let total_documents =
                        import_index_state.values().map(|index| index.total_documents()).sum();
                    ImportState::Finished { total_indexes, total_documents }
                } else {
                    ImportState::Ongoing { import_index_state, total_indexes }
                }
            }
            ImportState::Finished { total_indexes, total_documents } => {
                ImportState::Finished { total_indexes, total_documents }
            }
        }
    }

    pub fn is_import_finished(&self) -> bool {
        self.in_remotes.values().all(|remote| remote.is_finished())
    }

    pub fn remotes_import_state(&self) -> RemotesImportState {
        let mut import_state =
            RemotesImportState { total: self.out_remotes.len(), finished: 0, has_error: false };
        for remote in self.out_remotes.values() {
            match remote.export_state {
                ExportState::Ongoing => {}
                ExportState::Finished { successful } => {
                    import_state.finished += 1;
                    import_state.has_error |= !successful;
                }
            }
        }
        import_state
    }

    /// Iterates over the names of shards that still exist but are no longer owned by this remote
    pub fn removed_shard_names(&self) -> impl Iterator<Item = &str> + Clone + '_ {
        let this = self.name_for_export();
        itertools::merge_join_by(
            self.old_network.shards.iter(),
            self.new_network.shards.iter(),
            |(left, _), (right, _)| left.cmp(right),
        )
        .filter_map(move |eob| {
            let this = this?;
            match eob {
                EitherOrBoth::Both((shard_name, old), (_, new)) => {
                    let was_removed = old.remotes.contains(this) && !new.remotes.contains(this);
                    was_removed.then_some(shard_name.as_str())
                }
                EitherOrBoth::Left(_) => {
                    // removed shards have already been accounted for
                    None
                }
                EitherOrBoth::Right((shard_name, new)) => {
                    (!new.remotes.contains(this)).then_some(shard_name.as_str())
                }
            }
        })
    }
}

impl InRemote {
    pub fn is_finished(&self) -> bool {
        matches!(self.import_state, ImportState::Finished { .. })
    }
}

impl OutRemote {
    pub fn is_finished(&self) -> bool {
        matches!(self.export_state, ExportState::Finished { .. })
    }
}

impl Default for InRemote {
    fn default() -> Self {
        Self::new()
    }
}

impl ImportIndexState {
    pub fn is_finished(&self) -> bool {
        matches!(self, ImportIndexState::Finished { .. })
    }

    fn total_documents(&self) -> u64 {
        match *self {
            ImportIndexState::Ongoing { total_documents, .. }
            | ImportIndexState::Finished { total_documents } => total_documents,
        }
    }
}

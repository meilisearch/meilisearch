// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::{BTreeMap, BTreeSet};

use milli::DocumentId;

use crate::network::Remote;
use crate::tasks::network::{
    ImportIndexState, ImportState, InRemote, NetworkTopologyChange, NetworkTopologyState,
    ReceiveTaskError,
};

impl NetworkTopologyChange {
    pub fn export_to_process(&self) -> Option<(&BTreeMap<String, Remote>, &str)> {
        if self.state != NetworkTopologyState::ExportingDocuments {
            return None;
        }

        if self.out_remotes.is_empty() {
            return None;
        }

        let out_name = self.out_name()?;
        Some((&self.out_remotes, out_name))
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
                    NetworkTopologyState::Finished
                } else {
                    NetworkTopologyState::ImportingDocuments
                }
            }
            NetworkTopologyState::ImportingDocuments => {
                if self.is_import_finished() {
                    NetworkTopologyState::Finished
                } else {
                    NetworkTopologyState::ImportingDocuments
                }
            }
            NetworkTopologyState::Finished => NetworkTopologyState::Finished,
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
                    let mut task_keys = BTreeSet::new();
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
                                task_keys,
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
                                    if !task_keys.insert(task_key) {
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
                        let mut task_keys = BTreeSet::new();
                        if let Some(task_key) = task_key {
                            task_keys.insert(task_key);
                        }
                        let state = ImportIndexState::Ongoing {
                            total_documents: total_index_documents,
                            received_documents: document_count,
                            processed_documents: 0,
                            task_keys,
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
}

impl InRemote {
    pub fn is_finished(&self) -> bool {
        matches!(self.import_state, ImportState::Finished { .. })
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

use std::collections::BTreeMap;

use milli::DocumentId;

use crate::network::Remote;
use crate::tasks::network::{ImportState, InRemote, NetworkTopologyChange, ReceiveTaskError};

impl NetworkTopologyChange {
    pub fn export_to_process(&self) -> Option<(&BTreeMap<String, Remote>, &str)> {
        None
    }

    pub fn set_moved(&mut self, _moved_documents: u64) {}

    pub fn update_state(&mut self) {}

    pub fn receive_remote_task(
        &mut self,
        _remote_name: &str,
        _index_name: Option<&str>,
        _task_key: Option<DocumentId>,
        _document_count: u64,
        _total_indexes: u64,
        _total_index_documents: u64,
    ) -> Result<(), ReceiveTaskError> {
        Ok(())
    }

    pub fn process_remote_tasks(
        &mut self,
        _remote_name: &str,
        _index_name: &str,
        _document_count: u64,
    ) {
    }

    pub fn is_import_finished(&self) -> bool {
        true
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

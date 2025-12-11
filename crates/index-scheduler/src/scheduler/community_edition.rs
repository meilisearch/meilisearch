use meilisearch_types::milli::progress::Progress;
use meilisearch_types::tasks::Task;

use super::create_batch::Batch;
use crate::scheduler::process_batch::ProcessBatchInfo;
use crate::utils::ProcessingBatch;
use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_network_index_batch(
        &self,
        _network_task: Task,
        _inner_batch: Box<Batch>,
        _current_batch: &mut ProcessingBatch,
        _progress: Progress,
    ) -> Result<(Vec<Task>, ProcessBatchInfo)> {
        Err(Error::RequiresEnterpriseEdition { action: "processing a network task" })
    }

    pub(super) fn process_network_ready(
        &self,
        _task: Task,
        _progress: Progress,
    ) -> Result<(Vec<Task>, ProcessBatchInfo)> {
        Err(Error::RequiresEnterpriseEdition { action: "processing a network task" })
    }
}

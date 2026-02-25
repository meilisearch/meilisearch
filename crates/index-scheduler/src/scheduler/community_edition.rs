use meilisearch_types::milli::progress::Progress;
use meilisearch_types::network::Remote;
use meilisearch_types::tasks::network::Origin;
use meilisearch_types::tasks::Task;

use super::create_batch::Batch;
use crate::scheduler::process_batch::ProcessBatchInfo;
use crate::utils::ProcessingBatch;
use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(in crate::scheduler) fn notify_import_finished<
        'a,
        I: Iterator<Item = (&'a str, &'a Remote)>,
    >(
        &self,
        _remotes: I,
        _in_name: String,
        _origin: &Origin,
    ) -> crate::Result<()> {
        Ok(())
    }

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

    #[cfg(unix)]
    pub(super) async fn process_snapshot_to_s3(
        &self,
        _progress: Progress,
        _opts: meilisearch_types::milli::update::S3SnapshotOptions,
        _tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        Err(Error::RequiresEnterpriseEdition { action: "processing an S3-streaming snapshot task" })
    }
}

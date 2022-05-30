use crate::tasks::batch::{Batch, BatchContent};
use crate::tasks::BatchHandler;

pub struct SnapshotHandler;

#[async_trait::async_trait]
impl BatchHandler for SnapshotHandler {
    fn accept(&self, batch: &Batch) -> bool {
        matches!(batch.content, BatchContent::Snapshot(_))
    }

    async fn process_batch(&self, batch: Batch) -> Batch {
        match batch.content {
            BatchContent::Snapshot(job) => {
                if let Err(e) = job.run().await {
                    log::error!("snapshot error: {e}");
                }
            }
            _ => unreachable!(),
        }

        Batch::empty()
    }

    async fn finish(&self, _: &Batch) {}
}

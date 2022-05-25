use crate::tasks::batch::{Batch, BatchContent};
use crate::tasks::BatchHandler;

/// A sink handler for empty tasks.
pub struct EmptyBatchHandler;

#[async_trait::async_trait]
impl BatchHandler for EmptyBatchHandler {
    fn accept(&self, batch: &Batch) -> bool {
        matches!(batch.content, BatchContent::Empty)
    }

    async fn process_batch(&self, batch: Batch) -> Batch {
        batch
    }

    async fn finish(&self, _: &Batch) {
        ()
    }
}

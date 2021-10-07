use std::sync::Arc;

use crate::batch::Batch;
use crate::store::TaskStore;
use crate::{TaskPerformer, Result};

pub struct Scheduler<P> {
    store: TaskStore,
    performer: Arc<P>,
}

impl<P: TaskPerformer> Scheduler<P> {
    async fn run(self) {
        loop {
            let batch = self.prepare_batch().unwrap();
            let batch_result = self.performer.process(batch).unwrap();
            self.handle_batch_result(batch_result).unwrap();
        }
    }

    fn prepare_batch(&self) -> Result<Batch> {
        todo!()
    }

    fn handle_batch_result(&self, batch: Batch) -> Result<Batch> {
        todo!()
    }
}

use std::{path::Path, result::Result as StdResult, sync::Arc, time::Duration};
use batch::Batch;

#[cfg(not(test))]
use task_store::TaskStore;
#[cfg(test)]
use task_store::test::MockTaskStore as TaskStore;

use crate::scheduler::Scheduler;

pub mod batch;
pub mod task;
pub mod task_store;
pub mod scheduler;

type Result<T> = StdResult<T, Box<dyn std::error::Error + Sync + Send>>;

#[async_trait::async_trait(?Send)]
pub trait TaskPerformer {
    type Error: std::error::Error;
    /// Processes the `Task` batch returning the batch with the `Task` updated.
    async fn process(&self, batch: Batch) -> StdResult<Batch, Self::Error>;
}

pub fn create_task_store<P>(
    path: impl AsRef<Path>,
    size: usize,
    performer: Arc<P>,
    ) -> Result<TaskStore>
where P: TaskPerformer + Sync + Send + 'static,
{
    let task_store = TaskStore::new(path, size)?;
    let scheduler = Scheduler::new(task_store.clone(), performer, Duration::from_millis(1));
    tokio::task::spawn_local(scheduler.run());
    Ok(task_store)
}

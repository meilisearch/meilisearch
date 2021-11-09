use std::time::Duration;
use std::sync::Arc;
use std::result::Result as StdResult;
use std::path::Path;

use async_trait::async_trait;

#[cfg(not(test))]
pub use task_store::TaskStore;
#[cfg(test)]
pub use task_store::test::MockTaskStore as TaskStore;

use scheduler::Scheduler;
use batch::Batch;

pub mod batch;
pub mod task;
pub mod task_store;
pub mod scheduler;

type Result<T> = StdResult<T, Box<dyn std::error::Error + Sync + Send>>;

#[cfg_attr(test, mockall::automock(type Error=test::DebugError;))]
#[async_trait]
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

#[cfg(test)]
mod test {
    use std::fmt::Display;

    #[derive(Debug)]
    pub struct DebugError;

    impl Display for DebugError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an error")
        }
    }

    impl std::error::Error for DebugError {}

}

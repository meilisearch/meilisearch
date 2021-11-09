use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use serde::{Deserialize, Serialize};
#[cfg(test)]
pub use task_store::test::MockTaskStore as TaskStore;
#[cfg(not(test))]
pub use task_store::TaskStore;

use batch::Batch;
use scheduler::Scheduler;
use error::Result;

pub mod batch;
pub mod scheduler;
pub mod task;
pub mod task_store;
pub mod error;

#[cfg_attr(test, mockall::automock(type Error=test::DebugError;))]
#[async_trait]
pub trait TaskPerformer: Sync + Send + 'static {
    type Error: Serialize + for<'de> Deserialize<'de> + std::error::Error + Sync + Send + 'static;
    /// Processes the `Task` batch returning the batch with the `Task` updated.
    async fn process(&self, batch: Batch) -> Batch;
}

pub fn create_task_store<P>(
    path: impl AsRef<Path>,
    size: usize,
    performer: Arc<P>,
) -> Result<TaskStore>
where
    P: TaskPerformer,
{
    let task_store = TaskStore::new(path, size)?;
    let scheduler = Scheduler::new(task_store.clone(), performer, Duration::from_millis(1));
    tokio::task::spawn_local(scheduler.run());
    Ok(task_store)
}

#[cfg(test)]
mod test {
    use std::fmt::Display;
    use serde::{Serialize, Deserialize};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct DebugError;

    impl Display for DebugError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an error")
        }
    }

    impl std::error::Error for DebugError {}
}

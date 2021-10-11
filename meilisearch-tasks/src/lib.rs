use batch::Batch;

pub mod batch;
pub mod task;
pub mod store;
pub mod scheduler;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Sync + Send>>;

#[cfg_attr(test, mockall::automock)]
pub trait TaskPerformer {
    /// Processes the `Task` batch returning the batch with the `Task` updated.
    fn process(&self, task: Batch) -> Result<Batch>;
}

use batch::Batch;

pub mod batch;
pub mod task;
pub mod store;
pub mod scheduler;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub trait TaskPerformer {
    /// Processes the `Task` batch returning the batch with the `Task` updated.
    fn process(&self, task: Batch) -> Result<Batch>;
}

mod autobatcher;
mod batch;
pub mod error;
mod index_mapper;
mod index_scheduler;
pub mod task;
mod utils;

pub type Result<T> = std::result::Result<T, Error>;
pub type TaskId = u32;

pub use crate::index_scheduler::IndexScheduler;
pub use error::Error;
/// from the exterior you don't need to know there is multiple type of `Kind`
pub use task::KindWithContent as TaskKind;
/// from the exterior you don't need to know there is multiple type of `Task`
pub use task::TaskView as Task;

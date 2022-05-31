pub mod dump_handler;
pub mod empty_handler;
mod index_resolver_handler;
pub mod snapshot_handler;

#[cfg(test)]
mod test {
    use time::OffsetDateTime;

    use crate::tasks::{
        batch::{Batch, BatchContent},
        task::{Task, TaskContent},
    };

    pub fn task_to_batch(task: Task) -> Batch {
        let content = match task.content {
            TaskContent::DocumentAddition { .. } => {
                BatchContent::DocumentsAdditionBatch(vec![task])
            }
            TaskContent::DocumentDeletion { .. }
            | TaskContent::SettingsUpdate { .. }
            | TaskContent::IndexDeletion { .. }
            | TaskContent::IndexCreation { .. }
            | TaskContent::IndexUpdate { .. } => BatchContent::IndexUpdate(task),
            TaskContent::Dump { .. } => BatchContent::Dump(task),
        };

        Batch {
            id: Some(1),
            created_at: OffsetDateTime::now_utc(),
            content,
        }
    }
}

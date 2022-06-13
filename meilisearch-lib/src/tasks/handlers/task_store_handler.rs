use crate::tasks::batch::{Batch, BatchContent};
use crate::tasks::task::{Task, TaskContent, TaskEvent, TaskId, TaskResult};
use crate::tasks::{error::TaskError, BatchHandler, Result, TaskStore};

impl TaskStore {
    async fn abort_updates(&self, ids: &[TaskId]) -> Result<()> {
        let mut tasks = Vec::with_capacity(ids.len());

        for id in ids {
            let mut task = self.get_task(*id, None).await?;
            // Since updates are processed sequentially, no updates can be in an undecided state
            // here, therefore it's ok to only check for completion.
            if !task.is_finished() {
                task.events.push(TaskEvent::abort());
                tasks.push(task);
            } else {
                return Err(TaskError::AbortProcessedTask);
            }
        }

        self.update_tasks(tasks).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl BatchHandler for TaskStore {
    fn accept(&self, batch: &Batch) -> bool {
        matches!(batch.content, BatchContent::TaskAbortion(_))
    }

    async fn process_batch(&self, mut batch: Batch) -> Batch {
        match batch.content {
            BatchContent::TaskAbortion(Task {
                content: TaskContent::TaskAbortion { ref tasks },
                ref mut events,
                ..
            }) => {
                match self.abort_updates(tasks).await {
                    Ok(_) => events.push(TaskEvent::succeeded(TaskResult::Other)),
                    Err(e) => events.push(TaskEvent::failed(e)),
                }
                events.push(TaskEvent::succeeded(TaskResult::Other));
            }
            _ => unreachable!(),
        }

        batch
    }

    async fn finish(&self, _: &Batch) {}
}

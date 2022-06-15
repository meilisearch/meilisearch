use crate::tasks::batch::{Batch, BatchContent};
use crate::tasks::task::{Task, TaskContent, TaskEvent, TaskId, TaskResult};
use crate::tasks::{BatchHandler, Result, TaskFilter, TaskStore};

impl TaskStore {
    /// returns the number of aborted tasks
    async fn abort_tasks(&self, ids: &[TaskId]) -> Result<usize> {
        let mut tasks = Vec::with_capacity(ids.len());

        for id in ids {
            let mut task = self.get_task(*id, None).await?;
            // Since updates are processed sequentially, no updates can be in an undecided state
            // here, therefore it's ok to only check for completion.
            if task.is_pending() {
                task.events.push(TaskEvent::aborted());
                tasks.push(task);
            }
        }

        let tasks = self.update_tasks(tasks).await?;

        Ok(tasks.len())
    }

    async fn abort_pending_tasks(&self) -> Result<usize> {
        let mut filter = TaskFilter::default();
        filter.filter_fn(|t| t.is_pending());
        let mut pending_tasks = self.list_tasks(None, Some(filter), None).await?;

        pending_tasks.iter_mut().for_each(|t| {
            t.events.push(TaskEvent::aborted());
        });

        let tasks = self.update_tasks(pending_tasks).await?;

        Ok(tasks.len())
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
                content: TaskContent::TasksAbortion { ref tasks },
                ref mut events,
                ..
            }) => {
                if !events.iter().any(TaskEvent::is_aborted) {
                    match self.abort_tasks(tasks).await {
                        Ok(aborted) => {
                            events.push(TaskEvent::succeeded(TaskResult::TaskAbortion {
                                aborted_tasks: aborted as u64,
                            }))
                        }
                        Err(e) => events.push(TaskEvent::failed(e)),
                    }
                }
            }
            BatchContent::TaskAbortion(Task {
                content: TaskContent::TasksClear,
                ref mut events,
                ..
            }) => {
                if !events.iter().any(TaskEvent::is_aborted) {
                    match self.abort_pending_tasks().await {
                        Ok(aborted) => {
                            events.push(TaskEvent::succeeded(TaskResult::TaskAbortion {
                                aborted_tasks: aborted as u64,
                            }))
                        }
                        Err(e) => events.push(TaskEvent::failed(e)),
                    }
                }
            }
            _ => unreachable!(),
        }

        batch
    }

    async fn finish(&self, _: &Batch) {}
}

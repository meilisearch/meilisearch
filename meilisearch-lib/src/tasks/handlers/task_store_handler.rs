use crate::tasks::batch::{Batch, BatchContent};
use crate::tasks::task::{Task, TaskContent, TaskEvent, TaskResult};
use crate::tasks::{BatchHandler, TaskStore};

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
                let mut updated_tasks = Vec::with_capacity(tasks.len());
                for id in tasks {
                    let mut task = self.get_task(*id, None).await.unwrap();
                    if !task.is_finished() {
                        task.events.push(TaskEvent::abort());
                        updated_tasks.push(task);
                    } else {
                        panic!("can't abort already processed task");
                    }
                }

                self.update_tasks(updated_tasks).await.unwrap();

                events.push(TaskEvent::succeeded(TaskResult::Other));
            }
            _ => unreachable!(),
        }

        batch
    }

    async fn finish(&self, _: &Batch) {}
}

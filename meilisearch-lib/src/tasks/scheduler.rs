use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

use crate::tasks::task::TaskEvent;

use super::error::Result;
use super::TaskPerformer;

use super::batch::Batch;
#[cfg(test)]
use super::task_store::test::MockTaskStore as TaskStore;
use super::task_store::PendingTask;
#[cfg(not(test))]
use super::task_store::TaskStore;

/// The scheduler roles is to perform batches of tasks one at a time. It will monitor the TaskStore
/// for new tasks, put them in a batch, and process the batch as soon as possible.
///
/// When a batch is currently processing, the scheduler is just waiting.
pub struct Scheduler<P: TaskPerformer> {
    store: TaskStore,
    performer: Arc<P>,

    /// The interval at which the the `TaskStore` should be checked for new updates
    task_store_check_interval: Duration,
}

impl<P> Scheduler<P>
where
    P: TaskPerformer + Send + Sync + 'static,
    P::Error: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    pub fn new(store: TaskStore, performer: Arc<P>, task_store_check_interval: Duration) -> Self {
        Self {
            store,
            performer,
            task_store_check_interval,
        }
    }

    pub async fn run(self) {
        loop {
            if let Err(e) = self.process_next_batch().await {
                log::error!("an error occured while processing an update batch: {}", e);
            }
        }
    }

    async fn process_next_batch(&self) -> Result<()> {
        match self.prepare_batch().await? {
            Some(mut batch) => {
                for task in &mut batch.tasks {
                    match task {
                        PendingTask::Real(task) => {
                            task.events.push(TaskEvent::Processing(Utc::now()))
                        }
                        PendingTask::Ghost(_) => (),
                    }
                }
                self.store.update_tasks(batch.tasks.clone()).await?;

                let performer = self.performer.clone();
                let batch_result = performer.process(batch).await;
                self.handle_batch_result(batch_result).await?;
            }
            None => {
                // No updates found to create a batch we wait a bit before we retry.
                tokio::time::sleep(self.task_store_check_interval).await;
            }
        }

        Ok(())
    }

    /// Checks for pending tasks and groups them in a batch. If there are no pending update,
    /// return Ok(None)
    ///
    /// Until batching is properly implemented, the batches contain only one task.
    async fn prepare_batch(&self) -> Result<Option<Batch>> {
        match self.store.peek_pending_task().await {
            Some(PendingTask::Real(next_task_id)) => {
                let mut task = self.store.get_task(next_task_id, None).await?;

                task.events.push(TaskEvent::Batched {
                    timestamp: Utc::now(),
                    batch_id: 0,
                });

                let batch = Batch {
                    id: 0,
                    // index_uid: task.index_uid.clone(),
                    created_at: Utc::now(),
                    tasks: vec![PendingTask::Real(task)],
                };
                Ok(Some(batch))
            }
            Some(PendingTask::Ghost(task)) => Ok(Some(Batch {
                id: 0,
                created_at: Utc::now(),
                tasks: vec![PendingTask::Ghost(task)],
            })),
            None => Ok(None),
        }
    }

    /// Handles the result from a batch processing.
    ///
    /// When a task is processed, the result of the processing is pushed to its event list. The
    /// handle batch result make sure that the new state is save into its store.
    async fn handle_batch_result(&self, batch: Batch) -> Result<()> {
        let to_remove = batch
            .tasks
            .iter()
            .filter_map(|task| task.get_task_id())
            .collect();
        self.store.update_tasks(batch.tasks).await?;
        self.store.delete_tasks(to_remove).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use nelson::Mocker;

    use crate::index_resolver::IndexUid;
    use crate::tasks::task::Task;
    use crate::tasks::task_store::TaskFilter;

    use super::super::task::{TaskContent, TaskEvent, TaskId, TaskResult};
    use super::super::MockTaskPerformer;
    use super::*;

    #[tokio::test]
    async fn test_prepare_batch_full() {
        let mocker = Mocker::default();

        mocker
            .when::<(TaskId, Option<TaskFilter>), Result<Option<Task>>>("get_task")
            .once()
            .then(|(id, _filter)| {
                let task = Task {
                    id,
                    index_uid: IndexUid::new("Test".to_string()).unwrap(),
                    content: TaskContent::IndexDeletion,
                    events: vec![TaskEvent::Created(Utc::now())],
                };
                Ok(Some(task))
            });

        mocker
            .when::<(), Option<TaskId>>("peek_pending")
            .then(|()| Some(1));

        let store = TaskStore::mock(mocker);
        let performer = Arc::new(MockTaskPerformer::new());

        let scheduler = Scheduler {
            store,
            performer,
            task_store_check_interval: Duration::from_millis(1),
        };

        let batch = scheduler.prepare_batch().await.unwrap().unwrap();

        assert_eq!(batch.tasks.len(), 1);
        assert_eq!(batch.tasks[0].id, 1);
    }

    #[tokio::test]
    async fn test_prepare_batch_empty() {
        let mocker = Mocker::default();
        mocker
            .when::<(), Option<TaskId>>("peek_pending")
            .then(|()| None);

        let store = TaskStore::mock(mocker);
        let performer = Arc::new(MockTaskPerformer::new());

        let scheduler = Scheduler {
            store,
            performer,
            task_store_check_interval: Duration::from_millis(1),
        };

        assert!(scheduler.prepare_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_loop_run_normal() {
        let mocker = Mocker::default();
        let mut id = Some(1);
        mocker
            .when::<(), Option<TaskId>>("peek_pending")
            .then(move |()| id.take());
        mocker
            .when::<(TaskId, Option<TaskFilter>), Result<Option<Task>>>("get_task")
            .once()
            .then(|(id, _)| {
                let task = Task {
                    id,
                    index_uid: IndexUid::new("Test".to_string()).unwrap(),
                    content: TaskContent::IndexDeletion,
                    events: vec![TaskEvent::Created(Utc::now())],
                };
                Ok(Some(task))
            });
        mocker
            .when::<Vec<Task>, Result<()>>("update_tasks")
            .once()
            .then(|tasks| {
                assert_eq!(tasks.len(), 1);
                assert!(tasks[0]
                    .events
                    .iter()
                    .find(|e| matches!(e, &TaskEvent::Succeded { .. }))
                    .is_some());
                Ok(())
            });

        let store = TaskStore::mock(mocker);

        let mut performer = MockTaskPerformer::new();
        performer.expect_process().once().returning(|mut batch| {
            batch.tasks.iter_mut().for_each(|t| {
                t.events.push(TaskEvent::Succeded {
                    result: TaskResult::Other,
                    timestamp: Utc::now(),
                })
            });

            batch
        });

        let performer = Arc::new(performer);

        let scheduler = Scheduler {
            store,
            performer,
            task_store_check_interval: Duration::from_millis(1),
        };

        let handle = tokio::spawn(scheduler.run());

        match tokio::time::timeout(Duration::from_millis(100), handle).await {
            Ok(r) => r.unwrap(),
            _ => (),
        }
    }
}

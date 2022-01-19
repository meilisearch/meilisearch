use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{watch, RwLock};
use tokio::time::interval_at;

use super::batch::Batch;
use super::error::Result;
use super::scheduler::Pending;
use super::{Scheduler, TaskPerformer};
use crate::tasks::task::TaskEvent;

/// The update loop sequentially performs batches of updates by asking the scheduler for a batch,
/// and handing it to the `TaskPerformer`.
pub struct UpdateLoop<P: TaskPerformer> {
    scheduler: Arc<RwLock<Scheduler>>,
    performer: Arc<P>,

    notifier: Option<watch::Receiver<()>>,
    debounce_duration: Option<Duration>,
}

impl<P> UpdateLoop<P>
where
    P: TaskPerformer + Send + Sync + 'static,
{
    pub fn new(
        scheduler: Arc<RwLock<Scheduler>>,
        performer: Arc<P>,
        debuf_duration: Option<Duration>,
        notifier: watch::Receiver<()>,
    ) -> Self {
        Self {
            scheduler,
            performer,
            debounce_duration: debuf_duration,
            notifier: Some(notifier),
        }
    }

    pub async fn run(mut self) {
        let mut notifier = self.notifier.take().unwrap();

        loop {
            if notifier.changed().await.is_err() {
                break;
            }

            if let Some(t) = self.debounce_duration {
                let mut interval = interval_at(tokio::time::Instant::now() + t, t);
                interval.tick().await;
            };

            if let Err(e) = self.process_next_batch().await {
                log::error!("an error occured while processing an update batch: {}", e);
            }
        }
    }

    async fn process_next_batch(&self) -> Result<()> {
        let pending = { self.scheduler.write().await.prepare().await? };
        match pending {
            Pending::Batch(mut batch) => {
                for task in &mut batch.tasks {
                    task.events.push(TaskEvent::Processing(Utc::now()));
                }

                batch.tasks = {
                    self.scheduler
                        .read()
                        .await
                        .update_tasks(batch.tasks)
                        .await?
                };

                let performer = self.performer.clone();

                let batch = performer.process_batch(batch).await;

                self.handle_batch_result(batch).await?;
            }
            Pending::Job(job) => {
                let performer = self.performer.clone();
                performer.process_job(job).await;
            }
            Pending::Nothing => (),
        }

        Ok(())
    }

    /// Handles the result from a processed batch.
    ///
    /// When a task is processed, the result of the process is pushed to its event list. The
    /// `handle_batch_result` make sure that the new state is saved to the store.
    /// The tasks are then removed from the processing queue.
    async fn handle_batch_result(&self, mut batch: Batch) -> Result<()> {
        let mut scheduler = self.scheduler.write().await;
        let tasks = scheduler.update_tasks(batch.tasks).await?;
        scheduler.finish();
        drop(scheduler);
        batch.tasks = tasks;
        self.performer.finish(&batch).await;
        Ok(())
    }
}

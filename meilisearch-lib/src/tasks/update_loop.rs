use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;
use tokio::sync::{watch, RwLock};
use tokio::time::interval_at;

use super::batch::Batch;
use super::error::Result;
use super::{BatchHandler, Scheduler};
use crate::tasks::task::TaskEvent;

/// The update loop sequentially performs batches of updates by asking the scheduler for a batch,
/// and handing it to the `TaskPerformer`.
pub struct UpdateLoop {
    scheduler: Arc<RwLock<Scheduler>>,
    performers: Vec<Arc<dyn BatchHandler + Send + Sync + 'static>>,

    notifier: Option<watch::Receiver<()>>,
    debounce_duration: Option<Duration>,
}

impl UpdateLoop {
    pub fn new(
        scheduler: Arc<RwLock<Scheduler>>,
        performers: Vec<Arc<dyn BatchHandler + Send + Sync + 'static>>,
        debuf_duration: Option<Duration>,
        notifier: watch::Receiver<()>,
    ) -> Self {
        Self {
            scheduler,
            performers,
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
        let mut batch = { self.scheduler.write().await.prepare().await? };
        let performer = self
            .performers
            .iter()
            .find(|p| p.accept(&batch))
            .expect("No performer found for batch")
            .clone();

        batch
            .content
            .push_event(TaskEvent::Processing(OffsetDateTime::now_utc()));

        batch.content = {
            self.scheduler
                .read()
                .await
                .update_tasks(batch.content)
                .await?
        };

        let batch = performer.process_batch(batch).await;

        self.handle_batch_result(batch, performer).await?;

        Ok(())
    }

    /// Handles the result from a processed batch.
    ///
    /// When a task is processed, the result of the process is pushed to its event list. The
    /// `handle_batch_result` make sure that the new state is saved to the store.
    /// The tasks are then removed from the processing queue.
    async fn handle_batch_result(
        &self,
        mut batch: Batch,
        performer: Arc<dyn BatchHandler + Sync + Send + 'static>,
    ) -> Result<()> {
        let mut scheduler = self.scheduler.write().await;
        let content = scheduler.update_tasks(batch.content).await?;
        scheduler.finish();
        drop(scheduler);
        batch.content = content;
        performer.finish(&batch).await;
        Ok(())
    }
}

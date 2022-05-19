use time::OffsetDateTime;

use crate::index_resolver::IndexResolver;
use crate::index_resolver::{index_store::IndexStore, meta_store::IndexMetaStore};
use crate::tasks::batch::{Batch, BatchContent};
use crate::tasks::task::TaskEvent;
use crate::tasks::BatchHandler;

#[async_trait::async_trait]
impl<U, I> BatchHandler for IndexResolver<U, I>
where
    U: IndexMetaStore + Send + Sync + 'static,
    I: IndexStore + Send + Sync + 'static,
{
    fn accept(&self, batch: &Batch) -> bool {
        match batch.content {
            BatchContent::DocumentAddtitionBatch(_) | BatchContent::IndexUpdate(_) => true,
            _ => false,
        }
    }

    async fn process_batch(&self, mut batch: Batch) -> Batch {
        match batch.content {
            BatchContent::DocumentAddtitionBatch(ref mut tasks) => {
                *tasks = self
                    .process_document_addition_batch(std::mem::take(tasks))
                    .await;
            }
            BatchContent::IndexUpdate(ref mut task) => match self.process_task(&task).await {
                Ok(success) => {
                    task.events.push(TaskEvent::Succeded {
                        result: success,
                        timestamp: OffsetDateTime::now_utc(),
                    });
                }
                Err(err) => task.events.push(TaskEvent::Failed {
                    error: err.into(),
                    timestamp: OffsetDateTime::now_utc(),
                }),
            },
            _ => unreachable!(),
        }

        batch
    }

    async fn finish(&self, batch: &Batch) {
        if let BatchContent::DocumentAddtitionBatch(ref tasks) = batch.content {
            for task in tasks {
                if let Some(content_uuid) = task.get_content_uuid() {
                    if let Err(e) = self.file_store.delete(content_uuid).await {
                        log::error!("error deleting update file: {}", e);
                    }
                }
            }
        }
    }
}

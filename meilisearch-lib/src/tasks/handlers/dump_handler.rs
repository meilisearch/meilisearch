use crate::dump::DumpHandler;
use crate::index_resolver::index_store::IndexStore;
use crate::index_resolver::meta_store::IndexMetaStore;
use crate::tasks::batch::{Batch, BatchContent};
use crate::tasks::task::{Task, TaskContent, TaskEvent, TaskResult};
use crate::tasks::BatchHandler;

#[async_trait::async_trait]
impl<U, I> BatchHandler for DumpHandler<U, I>
where
    U: IndexMetaStore + Sync + Send + 'static,
    I: IndexStore + Sync + Send + 'static,
{
    fn accept(&self, batch: &Batch) -> bool {
        matches!(batch.content, BatchContent::Dump { .. })
    }

    async fn process_batch(&self, mut batch: Batch) -> Batch {
        match &batch.content {
            BatchContent::Dump(Task {
                content: TaskContent::Dump { uid },
                ..
            }) => {
                match self.run(uid.clone()).await {
                    Ok(_) => {
                        batch
                            .content
                            .push_event(TaskEvent::succeeded(TaskResult::Other));
                    }
                    Err(e) => batch.content.push_event(TaskEvent::failed(e.into())),
                }
                batch
            }
            _ => unreachable!("invalid batch content for dump"),
        }
    }

    async fn finish(&self, _: &Batch) {
        ()
    }
}

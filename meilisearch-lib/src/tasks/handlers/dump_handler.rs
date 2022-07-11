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
                    Err(e) => batch.content.push_event(TaskEvent::failed(e)),
                }
                batch
            }
            _ => unreachable!("invalid batch content for dump"),
        }
    }

    async fn finish(&self, _: &Batch) {}
}

#[cfg(test)]
mod test {
    use crate::dump::error::{DumpError, Result as DumpResult};
    use crate::index_resolver::{index_store::MockIndexStore, meta_store::MockIndexMetaStore};
    use crate::tasks::handlers::test::task_to_batch;

    use super::*;

    use nelson::Mocker;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn finish_does_nothing(
            task in any::<Task>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let handle = rt.spawn(async {
                let batch = task_to_batch(task);

                let mocker = Mocker::default();
                let dump_handler = DumpHandler::<MockIndexMetaStore, MockIndexStore>::mock(mocker);

                dump_handler.finish(&batch).await;
            });

            rt.block_on(handle).unwrap();
        }

        #[test]
        fn test_handle_dump_success(
            task in any::<Task>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let handle = rt.spawn(async {
                let batch = task_to_batch(task);
                let should_accept = matches!(batch.content, BatchContent::Dump { .. });

                let mocker = Mocker::default();
                if should_accept {
                    mocker.when::<String, DumpResult<()>>("run")
                    .once()
                    .then(|_| Ok(()));
                }

                let dump_handler = DumpHandler::<MockIndexMetaStore, MockIndexStore>::mock(mocker);

                let accept = dump_handler.accept(&batch);
                assert_eq!(accept, should_accept);

                if accept {
                    let batch = dump_handler.process_batch(batch).await;
                    let last_event = batch.content.first().unwrap().events.last().unwrap();
                    assert!(matches!(last_event, TaskEvent::Succeeded { .. }));
                }
            });

            rt.block_on(handle).unwrap();
        }

        #[test]
        fn test_handle_dump_error(
            task in any::<Task>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let handle = rt.spawn(async {
                let batch = task_to_batch(task);
                let should_accept = matches!(batch.content, BatchContent::Dump { .. });

                let mocker = Mocker::default();
                if should_accept {
                    mocker.when::<String, DumpResult<()>>("run")
                    .once()
                    .then(|_| Err(DumpError::Internal("error".into())));
                }

                let dump_handler = DumpHandler::<MockIndexMetaStore, MockIndexStore>::mock(mocker);

                let accept = dump_handler.accept(&batch);
                assert_eq!(accept, should_accept);

                if accept {
                    let batch = dump_handler.process_batch(batch).await;
                    let last_event = batch.content.first().unwrap().events.last().unwrap();
                    assert!(matches!(last_event, TaskEvent::Failed { .. }));
                }
            });

            rt.block_on(handle).unwrap();
        }
    }
}

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
        matches!(
            batch.content,
            BatchContent::DocumentsAdditionBatch(_) | BatchContent::IndexUpdate(_)
        )
    }

    async fn process_batch(&self, mut batch: Batch) -> Batch {
        match batch.content {
            BatchContent::DocumentsAdditionBatch(ref mut tasks) => {
                *tasks = self
                    .process_document_addition_batch(std::mem::take(tasks))
                    .await;
            }
            BatchContent::IndexUpdate(ref mut task) => match self.process_task(task).await {
                Ok(success) => task.events.push(TaskEvent::succeeded(success)),
                Err(err) => task.events.push(TaskEvent::failed(err.into())),
            },
            _ => unreachable!(),
        }

        batch
    }

    async fn finish(&self, batch: &Batch) {
        if let BatchContent::DocumentsAdditionBatch(ref tasks) = batch.content {
            for task in tasks {
                if let Some(content_uuid) = task.get_content_uuid() {
                    if let Err(e) = self.delete_content_file(content_uuid).await {
                        log::error!("error deleting update file: {}", e);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::index_resolver::index_store::MapIndexStore;
    use crate::index_resolver::meta_store::HeedMetaStore;
    use crate::index_resolver::{
        error::Result as IndexResult, index_store::MockIndexStore, meta_store::MockIndexMetaStore,
    };
    use crate::tasks::task::TaskResult;
    use crate::tasks::{
        handlers::test::task_to_batch,
        task::{Task, TaskContent},
    };
    use crate::update_file_store::{Result as FileStoreResult, UpdateFileStore};
    use crate::IndexUid;

    use super::*;
    use milli::update::IndexDocumentsMethod;
    use nelson::Mocker;
    use proptest::prelude::*;
    use uuid::Uuid;

    proptest! {
        #[test]
        fn test_accept_task(
            task in any::<Task>(),
        ) {
            let batch = task_to_batch(task);

            let index_store = MockIndexStore::new();
            let meta_store = MockIndexMetaStore::new();
            let mocker = Mocker::default();
            let update_file_store = UpdateFileStore::mock(mocker);
            let index_resolver = IndexResolver::new(meta_store, index_store, update_file_store);

            match batch.content {
                BatchContent::DocumentsAdditionBatch(_)
                    | BatchContent::IndexUpdate(_) => assert!(index_resolver.accept(&batch)),
                BatchContent::Dump(_)
                    | BatchContent::Snapshot(_)
                    | BatchContent::Empty => assert!(!index_resolver.accept(&batch)),
            }
        }
    }

    #[actix_rt::test]
    async fn finisher_called_on_document_update() {
        let index_store = MockIndexStore::new();
        let meta_store = MockIndexMetaStore::new();
        let mocker = Mocker::default();
        let content_uuid = Uuid::new_v4();
        mocker
            .when::<Uuid, FileStoreResult<()>>("delete")
            .once()
            .then(move |uuid| {
                assert_eq!(uuid, content_uuid);
                Ok(())
            });
        let update_file_store = UpdateFileStore::mock(mocker);
        let index_resolver = IndexResolver::new(meta_store, index_store, update_file_store);

        let task = Task {
            id: 1,
            content: TaskContent::DocumentAddition {
                content_uuid,
                merge_strategy: IndexDocumentsMethod::ReplaceDocuments,
                primary_key: None,
                documents_count: 100,
                allow_index_creation: true,
                index_uid: IndexUid::new_unchecked("test"),
            },
            events: Vec::new(),
        };

        let batch = task_to_batch(task);

        index_resolver.finish(&batch).await;
    }

    #[actix_rt::test]
    #[should_panic]
    async fn panic_when_passed_unsupported_batch() {
        let index_store = MockIndexStore::new();
        let meta_store = MockIndexMetaStore::new();
        let mocker = Mocker::default();
        let update_file_store = UpdateFileStore::mock(mocker);
        let index_resolver = IndexResolver::new(meta_store, index_store, update_file_store);

        let task = Task {
            id: 1,
            content: TaskContent::Dump {
                uid: String::from("hello"),
            },
            events: Vec::new(),
        };

        let batch = task_to_batch(task);

        index_resolver.process_batch(batch).await;
    }

    proptest! {
        #[test]
        fn index_document_task_deletes_update_file(
            task in any::<Task>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let handle = rt.spawn(async {
                let mocker = Mocker::default();

                if let TaskContent::DocumentAddition{ .. } = task.content {
                    mocker.when::<Uuid, IndexResult<()>>("delete_content_file").then(|_| Ok(()));
                }

                let index_resolver: IndexResolver<HeedMetaStore, MapIndexStore> = IndexResolver::mock(mocker);

                let batch = task_to_batch(task);

                index_resolver.finish(&batch).await;
            });

            rt.block_on(handle).unwrap();
        }

        #[test]
        fn test_handle_batch(task in any::<Task>()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let handle = rt.spawn(async {
                let mocker = Mocker::default();
                match task.content {
                    TaskContent::DocumentAddition { .. } => {
                        mocker.when::<Vec<Task>, Vec<Task>>("process_document_addition_batch").then(|tasks| tasks);
                    }
                    TaskContent::Dump { .. } => (),
                    _ => {
                        mocker.when::<&Task, IndexResult<TaskResult>>("process_task").then(|_| Ok(TaskResult::Other));
                    }
                }
                let index_resolver: IndexResolver<HeedMetaStore, MapIndexStore> = IndexResolver::mock(mocker);


                let batch = task_to_batch(task);

                if index_resolver.accept(&batch) {
                    index_resolver.process_batch(batch).await;
                }
            });

            if let Err(e) = rt.block_on(handle) {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                }
            }
        }
    }
}

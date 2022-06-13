use crate::index_resolver::IndexResolver;
use crate::index_resolver::{index_store::IndexStore, meta_store::IndexMetaStore};
use crate::tasks::batch::{Batch, BatchContent};
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
                self.process_document_addition_batch(tasks).await;
            }
            BatchContent::IndexUpdate(ref mut task) => {
                if !task.is_aborted() {
                    self.process_task(task).await;
                }
            }
            _ => unreachable!(),
        }

        batch
    }

    async fn finish(&self, batch: &Batch) {
        if let BatchContent::DocumentsAdditionBatch(ref tasks) = batch.content {
            // we do not ignore the aborted tasks here, since we want to remove their content.
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
    use crate::tasks::task::TaskEvent;
    use crate::tasks::{
        handlers::test::task_to_batch,
        task::{Task, TaskContent},
    };
    use crate::update_file_store::{Result as FileStoreResult, UpdateFileStore};

    use super::*;
    use meilisearch_types::index_uid::IndexUid;
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
                    | BatchContent::Empty
                    | BatchContent::TaskAbortion(_) => assert!(!index_resolver.accept(&batch)),
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
                        mocker.when::<&mut [Task], ()>("process_document_addition_batch").then(|_| ());
                    }
                    TaskContent::Dump { .. } => (),
                    _ => {
                        mocker.when::<&mut Task, ()>("process_task").then(|_| ());
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

    #[actix_rt::test]
    async fn test_abort_task() {
        let task = Task {
            id: 1,
            content: TaskContent::IndexUpdate {
                index_uid: IndexUid::new_unchecked("hello"),
                primary_key: None,
            },
            events: vec![TaskEvent::abort()],
        };

        let batch = task_to_batch(task.clone());

        let mocker = Mocker::default();
        let index_resolver: IndexResolver<HeedMetaStore, MapIndexStore> =
            IndexResolver::mock(mocker);

        let batch = index_resolver.process_batch(batch).await;
        assert!(index_resolver.accept(&batch));
        assert_eq!(batch.content.first().unwrap(), &task);

        index_resolver.finish(&batch).await;
    }

    #[actix_rt::test]
    async fn test_cleanup_after_abort() {
        let content_uuid = Uuid::new_v4();
        let task = Task {
            id: 1,
            content: TaskContent::DocumentAddition {
                index_uid: IndexUid::new_unchecked("hello"),
                content_uuid,
                merge_strategy: IndexDocumentsMethod::ReplaceDocuments,
                primary_key: None,
                documents_count: 10,
                allow_index_creation: true,
            },
            events: vec![TaskEvent::abort()],
        };

        let batch = task_to_batch(task.clone());

        let mocker = Mocker::default();
        mocker
            .when::<Uuid, IndexResult<()>>("delete_content_file")
            .then(|_| Ok(()));

        let index_resolver: IndexResolver<HeedMetaStore, MapIndexStore> =
            IndexResolver::mock(mocker);

        index_resolver.finish(&batch).await;
    }
}

pub mod error;
pub mod index_store;
pub mod meta_store;

use std::convert::TryFrom;
use std::path::Path;
use std::sync::Arc;

use error::{IndexResolverError, Result};
use index_store::{IndexStore, MapIndexStore};
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meta_store::{HeedMetaStore, IndexMetaStore};
use milli::heed::Env;
use milli::update::{DocumentDeletionResult, IndexerConfig};
use time::OffsetDateTime;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use crate::index::{error::Result as IndexResult, Index};
use crate::options::IndexerOpts;
use crate::tasks::task::{DocumentDeletion, Task, TaskContent, TaskEvent, TaskId, TaskResult};
use crate::update_file_store::UpdateFileStore;

use self::meta_store::IndexMeta;

pub type HardStateIndexResolver = IndexResolver<HeedMetaStore, MapIndexStore>;

#[cfg(not(test))]
pub use real::IndexResolver;

#[cfg(test)]
pub use test::MockIndexResolver as IndexResolver;

pub fn create_index_resolver(
    path: impl AsRef<Path>,
    index_size: usize,
    indexer_opts: &IndexerOpts,
    meta_env: Arc<milli::heed::Env>,
    file_store: UpdateFileStore,
) -> anyhow::Result<HardStateIndexResolver> {
    let uuid_store = HeedMetaStore::new(meta_env)?;
    let index_store = MapIndexStore::new(&path, index_size, indexer_opts)?;
    Ok(IndexResolver::new(uuid_store, index_store, file_store))
}

mod real {
    use super::*;

    pub struct IndexResolver<U, I> {
        pub(super) index_uuid_store: U,
        pub(super) index_store: I,
        pub(super) file_store: UpdateFileStore,
    }

    impl IndexResolver<HeedMetaStore, MapIndexStore> {
        pub fn load_dump(
            src: impl AsRef<Path>,
            dst: impl AsRef<Path>,
            index_db_size: usize,
            env: Arc<Env>,
            indexer_opts: &IndexerOpts,
        ) -> anyhow::Result<()> {
            HeedMetaStore::load_dump(&src, env)?;
            let indexes_path = src.as_ref().join("indexes");
            let indexes = indexes_path.read_dir()?;
            let indexer_config = IndexerConfig::try_from(indexer_opts)?;
            for index in indexes {
                Index::load_dump(&index?.path(), &dst, index_db_size, &indexer_config)?;
            }

            Ok(())
        }
    }

    impl<U, I> IndexResolver<U, I>
    where
        U: IndexMetaStore,
        I: IndexStore,
    {
        pub fn new(index_uuid_store: U, index_store: I, file_store: UpdateFileStore) -> Self {
            Self {
                index_uuid_store,
                index_store,
                file_store,
            }
        }

        pub async fn process_document_addition_batch(&self, tasks: &mut [Task]) {
            fn get_content_uuid(task: &Task) -> Uuid {
                match task {
                    Task {
                        content: TaskContent::DocumentAddition { content_uuid, .. },
                        ..
                    } => *content_uuid,
                    _ => panic!("unexpected task in the document addition batch"),
                }
            }

            let content_uuids = tasks.iter().map(get_content_uuid).collect::<Vec<_>>();

            match tasks.first() {
                Some(Task {
                    id,
                    content:
                        TaskContent::DocumentAddition {
                            merge_strategy,
                            primary_key,
                            allow_index_creation,
                            index_uid,
                            ..
                        },
                    ..
                }) => {
                    let primary_key = primary_key.clone();
                    let method = *merge_strategy;

                    let index = if *allow_index_creation {
                        self.get_or_create_index(index_uid.clone(), *id).await
                    } else {
                        self.get_index(index_uid.as_str().to_string()).await
                    };

                    // If the index doesn't exist and we are not allowed to create it with the first
                    // task, we must fails the whole batch.
                    let now = OffsetDateTime::now_utc();
                    let index = match index {
                        Ok(index) => index,
                        Err(e) => {
                            let error = ResponseError::from(e);
                            for task in tasks.iter_mut() {
                                task.events.push(TaskEvent::Failed {
                                    error: error.clone(),
                                    timestamp: now,
                                });
                            }

                            return;
                        }
                    };

                    let file_store = self.file_store.clone();
                    let result = spawn_blocking(move || {
                        index.update_documents(
                            method,
                            primary_key,
                            file_store,
                            content_uuids.into_iter(),
                        )
                    })
                    .await;

                    match result {
                        Ok(Ok(results)) => {
                            for (task, result) in tasks.iter_mut().zip(results) {
                                let event = match result {
                                    Ok(addition) => {
                                        TaskEvent::succeeded(TaskResult::DocumentAddition {
                                            indexed_documents: addition.indexed_documents,
                                        })
                                    }
                                    Err(error) => {
                                        TaskEvent::failed(IndexResolverError::from(error))
                                    }
                                };
                                task.events.push(event);
                            }
                        }
                        Ok(Err(e)) => {
                            let event = TaskEvent::failed(e);
                            for task in tasks.iter_mut() {
                                task.events.push(event.clone());
                            }
                        }
                        Err(e) => {
                            let event = TaskEvent::failed(IndexResolverError::from(e));
                            for task in tasks.iter_mut() {
                                task.events.push(event.clone());
                            }
                        }
                    }
                }
                _ => panic!("invalid batch!"),
            }
        }

        pub async fn delete_content_file(&self, content_uuid: Uuid) -> Result<()> {
            self.file_store.delete(content_uuid).await?;
            Ok(())
        }

        async fn process_task_inner(&self, task: &Task) -> Result<TaskResult> {
            match &task.content {
                TaskContent::DocumentAddition { .. } => {
                    panic!("updates should be handled by batch")
                }
                TaskContent::DocumentDeletion {
                    deletion: DocumentDeletion::Ids(ids),
                    index_uid,
                } => {
                    let ids = ids.clone();
                    let index = self.get_index(index_uid.clone().into_inner()).await?;

                    let DocumentDeletionResult {
                        deleted_documents, ..
                    } = spawn_blocking(move || index.delete_documents(&ids)).await??;

                    Ok(TaskResult::DocumentDeletion { deleted_documents })
                }
                TaskContent::DocumentDeletion {
                    deletion: DocumentDeletion::Clear,
                    index_uid,
                } => {
                    let index = self.get_index(index_uid.clone().into_inner()).await?;
                    let deleted_documents = spawn_blocking(move || -> IndexResult<u64> {
                        let number_documents = index.stats()?.number_of_documents;
                        index.clear_documents()?;
                        Ok(number_documents)
                    })
                    .await??;

                    Ok(TaskResult::ClearAll { deleted_documents })
                }
                TaskContent::SettingsUpdate {
                    settings,
                    is_deletion,
                    allow_index_creation,
                    index_uid,
                } => {
                    let index = if *is_deletion || !*allow_index_creation {
                        self.get_index(index_uid.clone().into_inner()).await?
                    } else {
                        self.get_or_create_index(index_uid.clone(), task.id).await?
                    };

                    let settings = settings.clone();
                    spawn_blocking(move || index.update_settings(&settings.check())).await??;

                    Ok(TaskResult::Other)
                }
                TaskContent::IndexDeletion { index_uid } => {
                    let index = self.delete_index(index_uid.clone().into_inner()).await?;

                    let deleted_documents = spawn_blocking(move || -> IndexResult<u64> {
                        Ok(index.stats()?.number_of_documents)
                    })
                    .await??;

                    Ok(TaskResult::ClearAll { deleted_documents })
                }
                TaskContent::IndexCreation {
                    primary_key,
                    index_uid,
                } => {
                    let index = self.create_index(index_uid.clone(), task.id).await?;

                    if let Some(primary_key) = primary_key {
                        let primary_key = primary_key.clone();
                        spawn_blocking(move || index.update_primary_key(primary_key)).await??;
                    }

                    Ok(TaskResult::Other)
                }
                TaskContent::IndexUpdate {
                    primary_key,
                    index_uid,
                } => {
                    let index = self.get_index(index_uid.clone().into_inner()).await?;

                    if let Some(primary_key) = primary_key {
                        let primary_key = primary_key.clone();
                        spawn_blocking(move || index.update_primary_key(primary_key)).await??;
                    }

                    Ok(TaskResult::Other)
                }
                _ => unreachable!("Invalid task for index resolver"),
            }
        }

        pub async fn process_task(&self, task: &mut Task) {
            match self.process_task_inner(task).await {
                Ok(res) => task.events.push(TaskEvent::succeeded(res)),
                Err(e) => task.events.push(TaskEvent::failed(e)),
            }
        }

        pub async fn dump(&self, path: impl AsRef<Path>) -> Result<()> {
            for (_, index) in self.list().await? {
                index.dump(&path)?;
            }
            self.index_uuid_store.dump(path.as_ref().to_owned()).await?;
            Ok(())
        }

        async fn create_index(&self, uid: IndexUid, creation_task_id: TaskId) -> Result<Index> {
            match self.index_uuid_store.get(uid.into_inner()).await? {
                (uid, Some(_)) => Err(IndexResolverError::IndexAlreadyExists(uid)),
                (uid, None) => {
                    let uuid = Uuid::new_v4();
                    let index = self.index_store.create(uuid).await?;
                    match self
                        .index_uuid_store
                        .insert(
                            uid,
                            IndexMeta {
                                uuid,
                                creation_task_id,
                            },
                        )
                        .await
                    {
                        Err(e) => {
                            match self.index_store.delete(uuid).await {
                                Ok(Some(index)) => {
                                    index.close();
                                }
                                Ok(None) => (),
                                Err(e) => log::error!("Error while deleting index: {:?}", e),
                            }
                            Err(e)
                        }
                        Ok(()) => Ok(index),
                    }
                }
            }
        }

        /// Get or create an index with name `uid`.
        pub async fn get_or_create_index(&self, uid: IndexUid, task_id: TaskId) -> Result<Index> {
            match self.create_index(uid, task_id).await {
                Ok(index) => Ok(index),
                Err(IndexResolverError::IndexAlreadyExists(uid)) => self.get_index(uid).await,
                Err(e) => Err(e),
            }
        }

        pub async fn list(&self) -> Result<Vec<(String, Index)>> {
            let uuids = self.index_uuid_store.list().await?;
            let mut indexes = Vec::new();
            for (name, IndexMeta { uuid, .. }) in uuids {
                match self.index_store.get(uuid).await? {
                    Some(index) => indexes.push((name, index)),
                    None => {
                        // we found an unexisting index, we remove it from the uuid store
                        let _ = self.index_uuid_store.delete(name).await;
                    }
                }
            }

            Ok(indexes)
        }

        pub async fn delete_index(&self, uid: String) -> Result<Index> {
            match self.index_uuid_store.delete(uid.clone()).await? {
                Some(IndexMeta { uuid, .. }) => match self.index_store.delete(uuid).await? {
                    Some(index) => {
                        index.clone().close();
                        Ok(index)
                    }
                    None => Err(IndexResolverError::UnexistingIndex(uid)),
                },
                None => Err(IndexResolverError::UnexistingIndex(uid)),
            }
        }

        pub async fn get_index(&self, uid: String) -> Result<Index> {
            match self.index_uuid_store.get(uid).await? {
                (name, Some(IndexMeta { uuid, .. })) => {
                    match self.index_store.get(uuid).await? {
                        Some(index) => Ok(index),
                        None => {
                            // For some reason we got a uuid to an unexisting index, we return an error,
                            // and remove the uuid from the uuid store.
                            let _ = self.index_uuid_store.delete(name.clone()).await;
                            Err(IndexResolverError::UnexistingIndex(name))
                        }
                    }
                }
                (name, _) => Err(IndexResolverError::UnexistingIndex(name)),
            }
        }

        pub async fn get_index_creation_task_id(&self, index_uid: String) -> Result<TaskId> {
            let (uid, meta) = self.index_uuid_store.get(index_uid).await?;
            meta.map(
                |IndexMeta {
                     creation_task_id, ..
                 }| creation_task_id,
            )
            .ok_or(IndexResolverError::UnexistingIndex(uid))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::index::IndexStats;

    use super::index_store::MockIndexStore;
    use super::meta_store::MockIndexMetaStore;
    use super::*;

    use futures::future::ok;
    use milli::FieldDistribution;
    use nelson::Mocker;

    pub enum MockIndexResolver<U, I> {
        Real(super::real::IndexResolver<U, I>),
        Mock(Mocker),
    }

    impl MockIndexResolver<HeedMetaStore, MapIndexStore> {
        pub fn load_dump(
            src: impl AsRef<Path>,
            dst: impl AsRef<Path>,
            index_db_size: usize,
            env: Arc<Env>,
            indexer_opts: &IndexerOpts,
        ) -> anyhow::Result<()> {
            super::real::IndexResolver::load_dump(src, dst, index_db_size, env, indexer_opts)
        }
    }

    impl<U, I> MockIndexResolver<U, I>
    where
        U: IndexMetaStore,
        I: IndexStore,
    {
        pub fn new(index_uuid_store: U, index_store: I, file_store: UpdateFileStore) -> Self {
            Self::Real(super::real::IndexResolver {
                index_uuid_store,
                index_store,
                file_store,
            })
        }

        pub fn mock(mocker: Mocker) -> Self {
            Self::Mock(mocker)
        }

        pub async fn process_document_addition_batch(&self, tasks: &mut [Task]) {
            match self {
                IndexResolver::Real(r) => r.process_document_addition_batch(tasks).await,
                IndexResolver::Mock(m) => unsafe {
                    m.get("process_document_addition_batch").call(tasks)
                },
            }
        }

        pub async fn process_task(&self, task: &mut Task) {
            match self {
                IndexResolver::Real(r) => r.process_task(task).await,
                IndexResolver::Mock(m) => unsafe { m.get("process_task").call(task) },
            }
        }

        pub async fn dump(&self, path: impl AsRef<Path>) -> Result<()> {
            match self {
                IndexResolver::Real(r) => r.dump(path).await,
                IndexResolver::Mock(_) => todo!(),
            }
        }

        /// Get or create an index with name `uid`.
        pub async fn get_or_create_index(&self, uid: IndexUid, task_id: TaskId) -> Result<Index> {
            match self {
                IndexResolver::Real(r) => r.get_or_create_index(uid, task_id).await,
                IndexResolver::Mock(_) => todo!(),
            }
        }

        pub async fn list(&self) -> Result<Vec<(String, Index)>> {
            match self {
                IndexResolver::Real(r) => r.list().await,
                IndexResolver::Mock(_) => todo!(),
            }
        }

        pub async fn delete_index(&self, uid: String) -> Result<Index> {
            match self {
                IndexResolver::Real(r) => r.delete_index(uid).await,
                IndexResolver::Mock(_) => todo!(),
            }
        }

        pub async fn get_index(&self, uid: String) -> Result<Index> {
            match self {
                IndexResolver::Real(r) => r.get_index(uid).await,
                IndexResolver::Mock(_) => todo!(),
            }
        }

        pub async fn get_index_creation_task_id(&self, index_uid: String) -> Result<TaskId> {
            match self {
                IndexResolver::Real(r) => r.get_index_creation_task_id(index_uid).await,
                IndexResolver::Mock(_) => todo!(),
            }
        }

        pub async fn delete_content_file(&self, content_uuid: Uuid) -> Result<()> {
            match self {
                IndexResolver::Real(r) => r.delete_content_file(content_uuid).await,
                IndexResolver::Mock(m) => unsafe {
                    m.get("delete_content_file").call(content_uuid)
                },
            }
        }
    }

    #[actix_rt::test]
    async fn test_remove_unknown_index() {
        let mut meta_store = MockIndexMetaStore::new();
        meta_store
            .expect_delete()
            .once()
            .returning(|_| Box::pin(ok(None)));

        let index_store = MockIndexStore::new();

        let mocker = Mocker::default();
        let file_store = UpdateFileStore::mock(mocker);

        let index_resolver = IndexResolver::new(meta_store, index_store, file_store);

        let mut task = Task {
            id: 1,
            content: TaskContent::IndexDeletion {
                index_uid: IndexUid::new_unchecked("test"),
            },
            events: Vec::new(),
        };

        index_resolver.process_task(&mut task).await;

        assert!(matches!(task.events[0], TaskEvent::Failed { .. }));
    }

    #[actix_rt::test]
    async fn test_remove_index() {
        let mut meta_store = MockIndexMetaStore::new();
        meta_store.expect_delete().once().returning(|_| {
            Box::pin(ok(Some(IndexMeta {
                uuid: Uuid::new_v4(),
                creation_task_id: 1,
            })))
        });

        let mut index_store = MockIndexStore::new();
        index_store.expect_delete().once().returning(|_| {
            let mocker = Mocker::default();
            mocker.when::<(), ()>("close").then(|_| ());
            mocker
                .when::<(), IndexResult<IndexStats>>("stats")
                .then(|_| {
                    Ok(IndexStats {
                        size: 10,
                        number_of_documents: 10,
                        is_indexing: None,
                        field_distribution: FieldDistribution::default(),
                    })
                });
            Box::pin(ok(Some(Index::mock(mocker))))
        });

        let mocker = Mocker::default();
        let file_store = UpdateFileStore::mock(mocker);

        let index_resolver = IndexResolver::new(meta_store, index_store, file_store);

        let mut task = Task {
            id: 1,
            content: TaskContent::IndexDeletion {
                index_uid: IndexUid::new_unchecked("test"),
            },
            events: Vec::new(),
        };

        index_resolver.process_task(&mut task).await;

        assert!(matches!(task.events[0], TaskEvent::Succeeded { .. }));
    }

    #[actix_rt::test]
    async fn test_delete_documents() {
        let mut meta_store = MockIndexMetaStore::new();
        meta_store.expect_get().once().returning(|_| {
            Box::pin(ok((
                "test".to_string(),
                Some(IndexMeta {
                    uuid: Uuid::new_v4(),
                    creation_task_id: 1,
                }),
            )))
        });

        let mut index_store = MockIndexStore::new();
        index_store.expect_get().once().returning(|_| {
            let mocker = Mocker::default();
            mocker
                .when::<(), IndexResult<()>>("clear_documents")
                .once()
                .then(|_| Ok(()));
            mocker
                .when::<(), IndexResult<IndexStats>>("stats")
                .once()
                .then(|_| {
                    Ok(IndexStats {
                        size: 10,
                        number_of_documents: 10,
                        is_indexing: None,
                        field_distribution: FieldDistribution::default(),
                    })
                });
            Box::pin(ok(Some(Index::mock(mocker))))
        });

        let mocker = Mocker::default();
        let file_store = UpdateFileStore::mock(mocker);

        let index_resolver = IndexResolver::new(meta_store, index_store, file_store);

        let mut task = Task {
            id: 1,
            content: TaskContent::DocumentDeletion {
                deletion: DocumentDeletion::Clear,
                index_uid: IndexUid::new_unchecked("test"),
            },
            events: Vec::new(),
        };

        index_resolver.process_task(&mut task).await;

        assert!(matches!(task.events[0], TaskEvent::Succeeded { .. }));
    }

    #[actix_rt::test]
    async fn test_index_update() {
        let mut meta_store = MockIndexMetaStore::new();
        meta_store.expect_get().once().returning(|_| {
            Box::pin(ok((
                "test".to_string(),
                Some(IndexMeta {
                    uuid: Uuid::new_v4(),
                    creation_task_id: 1,
                }),
            )))
        });

        let mut index_store = MockIndexStore::new();
        index_store.expect_get().once().returning(|_| {
            let mocker = Mocker::default();

            mocker
                .when::<String, IndexResult<crate::index::IndexMeta>>("update_primary_key")
                .once()
                .then(|_| {
                    Ok(crate::index::IndexMeta {
                        created_at: OffsetDateTime::now_utc(),
                        updated_at: OffsetDateTime::now_utc(),
                        primary_key: Some("key".to_string()),
                    })
                });
            Box::pin(ok(Some(Index::mock(mocker))))
        });

        let mocker = Mocker::default();
        let file_store = UpdateFileStore::mock(mocker);

        let index_resolver = IndexResolver::new(meta_store, index_store, file_store);

        let mut task = Task {
            id: 1,
            content: TaskContent::IndexUpdate {
                primary_key: Some("key".to_string()),
                index_uid: IndexUid::new_unchecked("test"),
            },
            events: Vec::new(),
        };

        index_resolver.process_task(&mut task).await;

        assert!(matches!(task.events[0], TaskEvent::Succeeded { .. }));
    }
}

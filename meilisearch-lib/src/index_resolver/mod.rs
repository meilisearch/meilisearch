pub mod error;
pub mod index_store;
pub mod meta_store;

use std::convert::TryInto;
use std::path::Path;

use chrono::Utc;
use error::{IndexResolverError, Result};
use index_store::{IndexStore, MapIndexStore};
use meilisearch_error::ResponseError;
use meta_store::{HeedMetaStore, IndexMetaStore};
use milli::update::DocumentDeletionResult;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use uuid::Uuid;

use crate::index::{error::Result as IndexResult, Index};
use crate::options::IndexerOpts;
use crate::tasks::batch::Batch;
use crate::tasks::task::{DocumentDeletion, Task, TaskContent, TaskEvent, TaskId, TaskResult};
use crate::tasks::TaskPerformer;

use self::meta_store::IndexMeta;

pub type HardStateIndexResolver = IndexResolver<HeedMetaStore, MapIndexStore>;

#[async_trait::async_trait]
impl<U, I> TaskPerformer for IndexResolver<U, I>
where
    U: IndexMetaStore + Send + Sync + 'static,
    I: IndexStore + Send + Sync + 'static,
{
    type Error = ResponseError;

    async fn process(&self, mut batch: Batch) -> Batch {
        // Until batching is implemented, all batch should contain only one update.
        debug_assert_eq!(batch.len(), 1);

        if let Some(task) = batch.tasks.first_mut() {
            task.events.push(TaskEvent::Processing(Utc::now()));

            match self.process_task(task).await {
                Ok(success) => {
                    task.events.push(TaskEvent::Succeded {
                        result: success,
                        timestamp: Utc::now(),
                    });
                }
                Err(err) => task.events.push(TaskEvent::Failed {
                    error: err.into(),
                    timestamp: Utc::now(),
                }),
            }
        }

        batch
    }
}

pub fn create_index_resolver(
    path: impl AsRef<Path>,
    index_size: usize,
    indexer_opts: &IndexerOpts,
) -> anyhow::Result<HardStateIndexResolver> {
    let uuid_store = HeedMetaStore::new(&path)?;
    let index_store = MapIndexStore::new(&path, index_size, indexer_opts)?;
    Ok(IndexResolver::new(uuid_store, index_store))
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct IndexUid(#[cfg_attr(test, proptest(regex("[a-zA-Z0-9_-]*")))] String);

impl IndexUid {
    pub fn new(uid: String) -> Result<Self> {
        if uid
            .chars()
            .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
        {
            Ok(Self(uid))
        } else {
            Err(IndexResolverError::BadlyFormatted(uid))
        }
    }

    #[cfg(test)]
    pub fn new_unchecked(s: impl AsRef<str>) -> Self {
        Self(s.as_ref().to_string())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    /// Return a reference over the inner str.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for IndexUid {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryInto<IndexUid> for String {
    type Error = IndexResolverError;

    fn try_into(self) -> Result<IndexUid> {
        IndexUid::new(self)
    }
}

pub struct IndexResolver<U, I> {
    index_uuid_store: U,
    index_store: I,
}

impl IndexResolver<HeedMetaStore, MapIndexStore> {
    // pub fn load_dump(
    //     src: impl AsRef<Path>,
    //     dst: impl AsRef<Path>,
    //     index_db_size: usize,
    //     indexer_opts: &IndexerOpts,
    // ) -> anyhow::Result<()> {
    //     HeedUuidStore::load_dump(&src, &dst)?; let indexes_path = src.as_ref().join("indexes"); let indexes = indexes_path.read_dir()?; let update_handler = UpdateHandler::new(indexer_opts)?;
    //     for index in indexes {
    //         let index = index?;
    //         Index::load_dump(&index.path(), &dst, index_db_size, &update_handler)?;
    //     }

    //     Ok(())
    // }
}

impl<U, I> IndexResolver<U, I>
where
    U: IndexMetaStore,
    I: IndexStore,
{
    pub fn new(index_uuid_store: U, index_store: I) -> Self {
        Self {
            index_uuid_store,
            index_store,
        }
    }

    async fn process_task(&self, task: &Task) -> Result<TaskResult> {
        let index_uid = task.index_uid.clone();
        match &task.content {
            TaskContent::DocumentAddition {
                content_uuid,
                merge_strategy,
                primary_key,
                ..
            } => {
                let primary_key = primary_key.clone();
                let content_uuid = *content_uuid;
                let method = *merge_strategy;

                let index = self.get_or_create_index(index_uid, task.id).await?;
                let result = spawn_blocking(move || {
                    index.update_documents(method, content_uuid, primary_key)
                })
                .await??;

                Ok(result.into())
            }
            TaskContent::DocumentDeletion(DocumentDeletion::Ids(ids)) => {
                let ids = ids.clone();
                let index = self.get_index(index_uid.into_inner()).await?;

                let DocumentDeletionResult {
                    deleted_documents, ..
                } = spawn_blocking(move || index.delete_documents(&ids)).await??;

                Ok(TaskResult::DocumentDeletion { deleted_documents })
            }
            TaskContent::DocumentDeletion(DocumentDeletion::Clear) => {
                let index = self.get_index(index_uid.into_inner()).await?;
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
            } => {
                let index = if *is_deletion {
                    self.get_index(index_uid.into_inner()).await?
                } else {
                    self.get_or_create_index(index_uid, task.id).await?
                };

                let settings = settings.clone();
                spawn_blocking(move || index.update_settings(&settings.check())).await??;

                Ok(TaskResult::Other)
            }
            TaskContent::IndexDeletion => {
                let index = self.delete_index(index_uid.into_inner()).await?;

                let deleted_documents = spawn_blocking(move || -> IndexResult<u64> {
                    Ok(index.stats()?.number_of_documents)
                })
                .await??;

                Ok(TaskResult::ClearAll { deleted_documents })
            }
            TaskContent::IndexCreation { primary_key } => {
                let index = self.create_index(index_uid, task.id).await?;

                if let Some(primary_key) = primary_key {
                    let primary_key = primary_key.clone();
                    spawn_blocking(move || index.update_primary_key(primary_key)).await??;
                }

                Ok(TaskResult::Other)
            }
            TaskContent::IndexUpdate { primary_key } => {
                let index = self.get_index(index_uid.into_inner()).await?;

                if let Some(primary_key) = primary_key {
                    let primary_key = primary_key.clone();
                    spawn_blocking(move || index.update_primary_key(primary_key)).await??;
                }

                Ok(TaskResult::Other)
            }
        }
    }

    // pub async fn dump(&self, path: impl AsRef<Path>) -> Result<Vec<Index>> {
    //     let uuids = self.index_uuid_store.dump(path.as_ref().to_owned()).await?;
    //     let mut indexes = Vec::new();
    //     for uuid in uuids {
    //         indexes.push(self.get_index_by_uuid(uuid).await?);
    //     }

    //     Ok(indexes)
    // }

    //  pub async fn get_uuids_size(&self) -> Result<u64> {
    //      Ok(self.index_uuid_store.get_size().await?)
    //  }

    //  pub async fn snapshot(&self, path: impl AsRef<Path>) -> Result<Vec<Index>> {
    //      let uuids = self
    //          .index_uuid_store
    //          .snapshot(path.as_ref().to_owned())
    //          .await?;
    //      let mut indexes = Vec::new();
    //      for uuid in uuids {
    //          indexes.push(self.get_index_by_uuid(uuid).await?);
    //      }

    //      Ok(indexes)
    //  }

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
                                index.inner().clone().prepare_for_closing();
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
                    index.inner().clone().prepare_for_closing();
                    Ok(index)
                }
                None => Err(IndexResolverError::UnexistingIndex(uid)),
            },
            None => Err(IndexResolverError::UnexistingIndex(uid)),
        }
    }

    // pub async fn get_index_by_uuid(&self, uuid: Uuid) -> Result<Index> {
    //     // TODO: Handle this error better.
    //     self.index_store
    //         .get(uuid)
    //         .await?
    //         .ok_or_else(|| IndexResolverError::UnexistingIndex(String::new()))
    // }

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

    // pub async fn get_uuid(&self, uid: String) -> Result<Uuid> {
    //     match self.index_uuid_store.get_uuid(uid).await? {
    //         (_, Some(uuid)) => Ok(uuid),
    //         (name, _) => Err(IndexResolverError::UnexistingIndex(name)),
    //     }
    // }
}

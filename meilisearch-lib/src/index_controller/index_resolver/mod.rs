pub mod error;
pub mod index_store;
pub mod uuid_store;

use std::path::Path;

use chrono::Utc;
use error::{IndexResolverError, Result};
use index_store::{IndexStore, MapIndexStore};
use meilisearch_error::ResponseError;
use tokio::task::spawn_blocking;
use uuid::Uuid;
use uuid_store::{HeedUuidStore, UuidStore};

use crate::index::updates::UpdateResult;
use crate::index::Index;
use crate::options::IndexerOpts;
use crate::tasks::batch::Batch;
use crate::tasks::task::{DocumentDeletion, Task, TaskContent, TaskEvent, TaskResult};
use crate::tasks::TaskPerformer;

pub type HardStateIndexResolver = IndexResolver<HeedUuidStore, MapIndexStore>;

#[async_trait::async_trait]
impl<U, I> TaskPerformer for IndexResolver<U, I>
where
    U: UuidStore + Send + Sync + 'static,
    I: IndexStore + Send + Sync + 'static,
{
    type Error = ResponseError;

    async fn process(&self, mut batch: Batch) -> Batch {
        // Until batching is implemented, all batch should contain only one update.
        debug_assert_eq!(batch.len(), 1);

        if let Some(task) = batch.tasks.first_mut() {
            task.events.push(TaskEvent::Processing(Utc::now()));

            match self.process_task(batch.index_uid.clone(), task).await {
                Ok(_success) => {
                    task.events.push(TaskEvent::Succeded {
                        result: TaskResult,
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
    let uuid_store = HeedUuidStore::new(&path)?;
    let index_store = MapIndexStore::new(&path, index_size, indexer_opts)?;
    Ok(IndexResolver::new(uuid_store, index_store))
}

pub struct IndexResolver<U, I> {
    index_uuid_store: U,
    index_store: I,
}

impl IndexResolver<HeedUuidStore, MapIndexStore> {
    // pub fn load_dump(
    //     src: impl AsRef<Path>,
    //     dst: impl AsRef<Path>,
    //     index_db_size: usize,
    //     indexer_opts: &IndexerOpts,
    // ) -> anyhow::Result<()> {
    //     HeedUuidStore::load_dump(&src, &dst)?;

    //     let indexes_path = src.as_ref().join("indexes");
    //     let indexes = indexes_path.read_dir()?;

    //     let update_handler = UpdateHandler::new(indexer_opts)?;
    //     for index in indexes {
    //         let index = index?;
    //         Index::load_dump(&index.path(), &dst, index_db_size, &update_handler)?;
    //     }

    //     Ok(())
    // }
}

impl<U, I> IndexResolver<U, I>
where
    U: UuidStore,
    I: IndexStore,
{
    pub fn new(index_uuid_store: U, index_store: I) -> Self {
        Self {
            index_uuid_store,
            index_store,
        }
    }

    async fn process_task(&self, index_uid: String, task: &Task) -> Result<UpdateResult> {
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

                    let index = self.get_or_create_index(index_uid).await?;
                    let result = spawn_blocking(move || {
                        index.update_documents(method, content_uuid, primary_key)
                    })
                    .await??;

                    Ok(result)
                }
            TaskContent::DocumentDeletion(DocumentDeletion::Ids(ids)) => {
                let ids = ids.clone();
                let index = self.get_index(index_uid).await?;
                Ok(spawn_blocking(move || index.delete_documents(&ids)).await??)
            }
            TaskContent::DocumentDeletion(DocumentDeletion::Clear) => {
                let index = self.get_index(index_uid).await?;
                Ok(spawn_blocking(move || index.clear_documents()).await??)
            }
            TaskContent::SettingsUpdate(settings) => {
                let index = self.get_or_create_index(index_uid).await?;
                let settings = settings.clone();
                let result = spawn_blocking(move || index.update_settings(&settings.check()))
                .await??;

                Ok(result)
            }
            TaskContent::IndexDeletion => {
                self.delete_index(index_uid).await?;
                // TODO: handle task deletion

                Ok(UpdateResult::Other)
            }
            TaskContent::CreateIndex { primary_key } => {
                let index = self.create_index(index_uid).await?;

                if let Some(primary_key) = primary_key {
                    let primary_key = primary_key.clone();
                    spawn_blocking(move || index.update_primary_key(primary_key))
                    .await??;
                }

                Ok(UpdateResult::Other)
            }
            TaskContent::UpdateIndex { primary_key } => {
                let index = self.get_index(index_uid).await?;

                if let Some(primary_key) = primary_key {
                    let primary_key = primary_key.clone();
                    spawn_blocking(move || index.update_primary_key(primary_key))
                    .await??;
                }

                Ok(UpdateResult::Other)
            },
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

    pub async fn create_index(&self, uid: String) -> Result<Index> {
        if !is_index_uid_valid(&uid) {
            return Err(IndexResolverError::BadlyFormatted(uid));
        }

        match self.index_uuid_store.get_uuid(uid).await? {
            (uid, Some(_)) => Err(IndexResolverError::IndexAlreadyExists(uid)),
            (uid, None) => {
                let uuid = Uuid::new_v4();
                let index = self.index_store.create(uuid).await?;
                match self.index_uuid_store.insert(uid, uuid).await {
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
    pub async fn get_or_create_index(&self, uid: String) -> Result<Index> {
        match self.create_index(uid).await {
            Ok(index) => Ok(index),
            Err(IndexResolverError::IndexAlreadyExists(uid)) => self.get_index(uid).await,
            Err(e) => Err(e),
        }
    }

    pub async fn list(&self) -> Result<Vec<(String, Index)>> {
        let uuids = self.index_uuid_store.list().await?;
        let mut indexes = Vec::new();
        for (name, uuid) in uuids {
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

    pub async fn delete_index(&self, uid: String) -> Result<Uuid> {
        match self.index_uuid_store.delete(uid.clone()).await? {
            Some(uuid) => {
                match self.index_store.delete(uuid).await {
                    Ok(Some(index)) => {
                        index.inner().clone().prepare_for_closing();
                    }
                    Ok(None) => (),
                    Err(e) => log::error!("Error while deleting index: {:?}", e),
                }
                Ok(uuid)
            }
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
        match self.index_uuid_store.get_uuid(uid).await? {
            (name, Some(uuid)) => {
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

    // pub async fn get_uuid(&self, uid: String) -> Result<Uuid> {
    //     match self.index_uuid_store.get_uuid(uid).await? {
    //         (_, Some(uuid)) => Ok(uuid),
    //         (name, _) => Err(IndexResolverError::UnexistingIndex(name)),
    //     }
    // }
}

fn is_index_uid_valid(uid: &str) -> bool {
    uid.chars()
        .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
}

pub mod error;
pub mod index_store;
pub mod uuid_store;

use std::result::Result as StdResult;
use std::path::Path;

use chrono::Utc;
use error::{IndexResolverError, Result};
use index_store::{IndexStore, MapIndexStore};
use uuid::Uuid;
use uuid_store::{HeedUuidStore, UuidStore};

use crate::tasks::task::{DocumentDeletion, TaskResult};
use crate::tasks::{TaskPerformer, batch::Batch, task::{DocumentAdditionMergeStrategy, TaskContent, TaskError, TaskEvent}};

use crate::{index::{Index, error::IndexError, update_handler::UpdateHandler}, index_controller::updates::status::UpdateResult, options::IndexerOpts};

pub type HardStateIndexResolver = IndexResolver<HeedUuidStore, MapIndexStore>;

#[async_trait::async_trait(?Send)]
impl<U, I> TaskPerformer for IndexResolver<U, I>
where U: UuidStore,
      I: IndexStore,
{
    type Error = IndexResolverError;

    async fn process(&self, mut batch: Batch) -> StdResult<Batch, Self::Error> {
        use milli::update::IndexDocumentsMethod;
        // Until batching is implemented, all batch should contain only one update.
        debug_assert_eq!(batch.len(), 1);


        if let Some(task) = batch.tasks.first_mut() {
            task.events.push(TaskEvent::Processing(Utc::now()));

            let index_uid = batch.index_uid.clone();
            let result = match &task.content {
                TaskContent::DocumentAddition { content_uuid, merge_strategy, primary_key, .. } =>  {
                    let method = match merge_strategy {
                        DocumentAdditionMergeStrategy::UpdateDocument => IndexDocumentsMethod::UpdateDocuments,
                        DocumentAdditionMergeStrategy::ReplaceDocument => IndexDocumentsMethod::ReplaceDocuments,
                    };

                    let primary_key = primary_key.clone();
                    let content_uuid = *content_uuid;

                    let index = self.get_or_create_index(index_uid).await?;
                    tokio::task::spawn_blocking(move || -> StdResult<_, IndexError> {
                        let mut txn = index.write_txn()?;

                        if let Some(primary_key) = primary_key {
                            index.update_primary_key(&mut txn, primary_key)?;
                        }

                        let res = index.update_documents(
                            &mut txn,
                            method,
                            content_uuid)?;
                        txn.commit()?;
                        Ok(res)
                    }).await?
                },
                TaskContent::DocumentDeletion(DocumentDeletion::Ids(ids)) => {
                    let ids = ids.clone();
                    let index = self.get_or_create_index(index_uid).await?;
                    tokio::task::spawn_blocking(move || -> StdResult<_, IndexError> {
                        let mut txn = index.write_txn()?;
                        let res = index.delete_documents(&mut txn, &ids)?;
                        txn.commit()?;
                        Ok(res)
                    }).await?
                },
                TaskContent::DocumentDeletion(DocumentDeletion::Clear) => {
                    let index = self.get_or_create_index(index_uid).await?;
                    tokio::task::spawn_blocking(move || -> StdResult<_, IndexError> {
                        let mut txn = index.write_txn()?;
                        let res = index.clear_documents(&mut txn)?;
                        txn.commit()?;
                        Ok(res)
                    }).await?
                },
                TaskContent::SettingsUpdate(settings) => {
                    let index = self.get_or_create_index(index_uid).await?;
                    let settings = settings.clone();
                    tokio::task::spawn_blocking(move || -> StdResult<_, IndexError> {
                        let mut txn = index.write_txn()?;
                        let res = index.update_settings(&mut txn, &settings.check())?;
                        txn.commit()?;
                        Ok(res)
                    }).await?
                },
                TaskContent::IndexDeletion => {
                    self.delete_index(index_uid).await?;

                    Ok(UpdateResult::Other)
                },
                TaskContent::CreateIndex { primary_key } => {
                    let index = self.create_index(index_uid).await?;

                    if let Some(primary_key) = primary_key {
                        let primary_key = primary_key.clone();
                        tokio::task::spawn_blocking(move || -> StdResult<_, IndexError> {
                            let mut txn = index.write_txn()?;
                            let res = index.update_primary_key(&mut txn, primary_key)?;
                            txn.commit()?;
                            Ok(res)
                        }).await??;
                    }

                    Ok(UpdateResult::Other)
                }
            };

            match result {
                Ok(_success) => {
                    task.events.push(TaskEvent::Succeded {
                        result: TaskResult,
                        timestamp: Utc::now(),
                    });
                },
                Err(_err) => {
                    task.events.push(TaskEvent::Failed {
                        error: TaskError,
                        timestamp: Utc::now(),
                    })
                },
            }
        }

        Ok(batch)
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
    pub fn load_dump(
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        index_db_size: usize,
        indexer_opts: &IndexerOpts,
    ) -> anyhow::Result<()> {
        HeedUuidStore::load_dump(&src, &dst)?;

        let indexes_path = src.as_ref().join("indexes");
        let indexes = indexes_path.read_dir()?;

        let update_handler = UpdateHandler::new(indexer_opts)?;
        for index in indexes {
            let index = index?;
            Index::load_dump(&index.path(), &dst, index_db_size, &update_handler)?;
        }

        Ok(())
    }
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

    pub async fn dump(&self, path: impl AsRef<Path>) -> Result<Vec<Index>> {
        let uuids = self.index_uuid_store.dump(path.as_ref().to_owned()).await?;
        let mut indexes = Vec::new();
        for uuid in uuids {
            indexes.push(self.get_index_by_uuid(uuid).await?);
        }

        Ok(indexes)
    }

    pub async fn get_uuids_size(&self) -> Result<u64> {
        Ok(self.index_uuid_store.get_size().await?)
    }

    pub async fn snapshot(&self, path: impl AsRef<Path>) -> Result<Vec<Index>> {
        let uuids = self
            .index_uuid_store
            .snapshot(path.as_ref().to_owned())
            .await?;
        let mut indexes = Vec::new();
        for uuid in uuids {
            indexes.push(self.get_index_by_uuid(uuid).await?);
        }

        Ok(indexes)
    }

    pub async fn create_index(
        &self,
        uid: String,
    ) -> Result<Index> {
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
    pub async fn get_or_create_index(
    &self,
    uid: String,
    ) -> Result<Index> {
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

    pub async fn get_index_by_uuid(&self, uuid: Uuid) -> Result<Index> {
        // TODO: Handle this error better.
        self.index_store
            .get(uuid)
            .await?
            .ok_or_else(|| IndexResolverError::UnexistingIndex(String::new()))
    }

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

    pub async fn get_uuid(&self, uid: String) -> Result<Uuid> {
        match self.index_uuid_store.get_uuid(uid).await? {
            (_, Some(uuid)) => Ok(uuid),
            (name, _) => Err(IndexResolverError::UnexistingIndex(name)),
        }
    }
}

fn is_index_uid_valid(uid: &str) -> bool {
    uid.chars()
        .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
}

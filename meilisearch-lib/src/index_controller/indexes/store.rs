use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use milli::update::UpdateBuilder;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use super::error::{IndexActorError, Result};
use crate::index::Index;
use crate::index_controller::update_file_store::UpdateFileStore;

type AsyncMap<K, V> = Arc<RwLock<HashMap<K, V>>>;

#[async_trait::async_trait]
pub trait IndexStore {
    async fn create(&self, uuid: Uuid, primary_key: Option<String>) -> Result<Index>;
    async fn get(&self, uuid: Uuid) -> Result<Option<Index>>;
    async fn delete(&self, uuid: Uuid) -> Result<Option<Index>>;
}

pub struct MapIndexStore {
    index_store: AsyncMap<Uuid, Index>,
    path: PathBuf,
    index_size: usize,
    update_file_store: Arc<UpdateFileStore>,
}

impl MapIndexStore {
    pub fn new(path: impl AsRef<Path>, index_size: usize) -> Self {
        let update_file_store = Arc::new(UpdateFileStore::new(path.as_ref()).unwrap());
        let path = path.as_ref().join("indexes/");
        let index_store = Arc::new(RwLock::new(HashMap::new()));
        Self {
            index_store,
            path,
            index_size,
            update_file_store,
        }
    }
}

#[async_trait::async_trait]
impl IndexStore for MapIndexStore {
    async fn create(&self, uuid: Uuid, primary_key: Option<String>) -> Result<Index> {
        // We need to keep the lock until we are sure the db file has been opened correclty, to
        // ensure that another db is not created at the same time.
        let mut lock = self.index_store.write().await;

        if let Some(index) = lock.get(&uuid) {
            return Ok(index.clone());
        }
        let path = self.path.join(format!("index-{}", uuid));
        if path.exists() {
            return Err(IndexActorError::IndexAlreadyExists);
        }

        let index_size = self.index_size;
        let file_store = self.update_file_store.clone();
        let index = spawn_blocking(move || -> Result<Index> {
            let index = Index::open(path, index_size, file_store)?;
            if let Some(primary_key) = primary_key {
                let mut txn = index.write_txn()?;

                let mut builder = UpdateBuilder::new(0).settings(&mut txn, &index);
                builder.set_primary_key(primary_key);
                builder.execute(|_, _| ())?;

                txn.commit()?;
            }
            Ok(index)
        })
        .await??;

        lock.insert(uuid, index.clone());

        Ok(index)
    }

    async fn get(&self, uuid: Uuid) -> Result<Option<Index>> {
        let guard = self.index_store.read().await;
        match guard.get(&uuid) {
            Some(index) => Ok(Some(index.clone())),
            None => {
                // drop the guard here so we can perform the write after without deadlocking;
                drop(guard);
                let path = self.path.join(format!("index-{}", uuid));
                if !path.exists() {
                    return Ok(None);
                }

                let index_size = self.index_size;
                let file_store = self.update_file_store.clone();
                let index = spawn_blocking(move || Index::open(path, index_size, file_store)).await??;
                self.index_store.write().await.insert(uuid, index.clone());
                Ok(Some(index))
            }
        }
    }

    async fn delete(&self, uuid: Uuid) -> Result<Option<Index>> {
        let db_path = self.path.join(format!("index-{}", uuid));
        fs::remove_dir_all(db_path).await?;
        let index = self.index_store.write().await.remove(&uuid);
        Ok(index)
    }
}

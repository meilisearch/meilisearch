use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use milli::update::UpdateBuilder;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use super::error::{IndexResolverError, Result};
use crate::index::update_handler::UpdateHandler;
use crate::index::Index;
use crate::index_controller::update_file_store::UpdateFileStore;
use crate::options::IndexerOpts;

type AsyncMap<K, V> = Arc<RwLock<HashMap<K, V>>>;

#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock)]
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
    update_handler: Arc<UpdateHandler>,
}

impl MapIndexStore {
    pub fn new(
        path: impl AsRef<Path>,
        index_size: usize,
        indexer_opts: &IndexerOpts,
    ) -> anyhow::Result<Self> {
        let update_handler = Arc::new(UpdateHandler::new(indexer_opts)?);
        let update_file_store = Arc::new(UpdateFileStore::new(path.as_ref()).unwrap());
        let path = path.as_ref().join("indexes/");
        let index_store = Arc::new(RwLock::new(HashMap::new()));
        Ok(Self {
            index_store,
            path,
            index_size,
            update_file_store,
            update_handler,
        })
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
        let path = self.path.join(format!("{}", uuid));
        if path.exists() {
            return Err(IndexResolverError::UuidAlreadyExists(uuid));
        }

        let index_size = self.index_size;
        let file_store = self.update_file_store.clone();
        let update_handler = self.update_handler.clone();
        let index = spawn_blocking(move || -> Result<Index> {
            let index = Index::open(path, index_size, file_store, uuid, update_handler)?;
            if let Some(primary_key) = primary_key {
                let inner = index.inner();
                let mut txn = inner.write_txn()?;

                let mut builder = UpdateBuilder::new(0).settings(&mut txn, index.inner());
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
                let path = self.path.join(format!("{}", uuid));
                if !path.exists() {
                    return Ok(None);
                }

                let index_size = self.index_size;
                let file_store = self.update_file_store.clone();
                let update_handler = self.update_handler.clone();
                let index = spawn_blocking(move || {
                    Index::open(path, index_size, file_store, uuid, update_handler)
                })
                .await??;
                self.index_store.write().await.insert(uuid, index.clone());
                Ok(Some(index))
            }
        }
    }

    async fn delete(&self, uuid: Uuid) -> Result<Option<Index>> {
        let db_path = self.path.join(format!("{}", uuid));
        fs::remove_dir_all(db_path).await?;
        let index = self.index_store.write().await.remove(&uuid);
        Ok(index)
    }
}

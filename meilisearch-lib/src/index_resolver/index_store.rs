use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use milli::update::IndexerConfig;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use super::error::{IndexResolverError, Result};
use crate::index::Index;
use crate::options::IndexerOpts;

type AsyncMap<K, V> = Arc<RwLock<HashMap<K, V>>>;

#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait IndexStore {
    async fn create(&self, uuid: Uuid) -> Result<Index>;
    async fn get(&self, uuid: Uuid) -> Result<Option<Index>>;
    async fn delete(&self, uuid: Uuid) -> Result<Option<Index>>;
}

pub struct MapIndexStore {
    index_store: AsyncMap<Uuid, Index>,
    path: PathBuf,
    index_size: usize,
    indexer_config: Arc<IndexerConfig>,
}

impl MapIndexStore {
    pub fn new(
        path: impl AsRef<Path>,
        index_size: usize,
        indexer_opts: &IndexerOpts,
    ) -> anyhow::Result<Self> {
        let indexer_config = Arc::new(IndexerConfig::try_from(indexer_opts)?);
        let path = path.as_ref().join("indexes/");
        let index_store = Arc::new(RwLock::new(HashMap::new()));
        Ok(Self {
            index_store,
            path,
            index_size,
            indexer_config,
        })
    }
}

#[async_trait::async_trait]
impl IndexStore for MapIndexStore {
    async fn create(&self, uuid: Uuid) -> Result<Index> {
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
        let update_handler = self.indexer_config.clone();
        let index = spawn_blocking(move || -> Result<Index> {
            let index = Index::open(path, index_size, uuid, update_handler)?;
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
                let update_handler = self.indexer_config.clone();
                let index =
                    spawn_blocking(move || Index::open(path, index_size, uuid, update_handler))
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

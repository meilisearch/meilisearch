use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::{Result, UpdateError, UpdateStore};
use crate::index_controller::IndexActorHandle;

#[async_trait::async_trait]
pub trait UpdateStoreStore {
    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<UpdateStore>>;
    async fn delete(&self, uuid: Uuid) -> Result<Option<Arc<UpdateStore>>>;
    async fn get(&self, uuid: Uuid) -> Result<Option<Arc<UpdateStore>>>;
}

pub struct MapUpdateStoreStore<I> {
    db: Arc<RwLock<HashMap<Uuid, Arc<UpdateStore>>>>,
    index_handle: I,
    path: PathBuf,
    update_store_size: usize,
}

impl<I: IndexActorHandle> MapUpdateStoreStore<I> {
    pub fn new(index_handle: I, path: impl AsRef<Path>, update_store_size: usize) -> Self {
        let db = Arc::new(RwLock::new(HashMap::new()));
        let path = path.as_ref().to_owned();
        Self {
            db,
            index_handle,
            path,
            update_store_size,
        }
    }
}

#[async_trait::async_trait]
impl<I> UpdateStoreStore for MapUpdateStoreStore<I>
where
    I: IndexActorHandle + Clone + Send + Sync + 'static,
{
    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<UpdateStore>> {
        match self.db.write().await.entry(uuid) {
            Entry::Vacant(e) => {
                let mut options = heed::EnvOpenOptions::new();
                let update_store_size = self.update_store_size;
                options.map_size(update_store_size);
                let path = self.path.clone().join(format!("updates-{}", e.key()));
                fs::create_dir_all(&path).await.unwrap();
                let index_handle = self.index_handle.clone();
                let store = UpdateStore::open(options, &path, move |meta, file| {
                    futures::executor::block_on(index_handle.update(meta, file))
                })
                .map_err(|e| UpdateError::Error(e.into()))?;
                let store = e.insert(store);
                Ok(store.clone())
            }
            Entry::Occupied(e) => Ok(e.get().clone()),
        }
    }

    async fn get(&self, uuid: Uuid) -> Result<Option<Arc<UpdateStore>>> {
        let guard = self.db.read().await;
        match guard.get(&uuid) {
            Some(uuid) => Ok(Some(uuid.clone())),
            None => {
                // The index is not found in the found in the loaded indexes, so we attempt to load
                // it from disk. We need to acquire a write lock **before** attempting to open the
                // index, because someone could be trying to open it at the same time as us.
                drop(guard);
                let path = self.path.clone().join(format!("updates-{}", uuid));
                if path.exists() {
                    let mut guard = self.db.write().await;
                    match guard.entry(uuid) {
                        Entry::Vacant(entry) => {
                            // We can safely load the index
                            let index_handle = self.index_handle.clone();
                            let mut options = heed::EnvOpenOptions::new();
                            let update_store_size = self.update_store_size;
                            options.map_size(update_store_size);
                            let store = UpdateStore::open(options, &path, move |meta, file| {
                                futures::executor::block_on(index_handle.update(meta, file))
                            })
                            .map_err(|e| UpdateError::Error(e.into()))?;
                            let store = entry.insert(store);
                            Ok(Some(store.clone()))
                        }
                        Entry::Occupied(entry) => {
                            // The index was loaded while we attempted to to iter
                            Ok(Some(entry.get().clone()))
                        }
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn delete(&self, uuid: Uuid) -> Result<Option<Arc<UpdateStore>>> {
        let store = self.db.write().await.remove(&uuid);
        let path = self.path.clone().join(format!("updates-{}", uuid));
        if store.is_some() || path.exists() {
            fs::remove_dir_all(path).await.unwrap();
        }
        Ok(store)
    }
}

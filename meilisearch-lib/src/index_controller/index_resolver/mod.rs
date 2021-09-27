pub mod uuid_store;
mod index_store;
//mod message;
pub mod error;

use std::path::Path;

use uuid::Uuid;
use uuid_store::{UuidStore, HeedUuidStore};
use index_store::{IndexStore, MapIndexStore};
use error::{Result, IndexResolverError};

use crate::{index::Index, options::IndexerOpts};

pub type HardStateIndexResolver = IndexResolver<HeedUuidStore, MapIndexStore>;

pub fn create_index_resolver(path: impl AsRef<Path>, index_size: usize, indexer_opts: &IndexerOpts) -> anyhow::Result<HardStateIndexResolver> {
    let uuid_store = HeedUuidStore::new(&path)?;
    let index_store = MapIndexStore::new(&path, index_size, indexer_opts)?;
    Ok(IndexResolver::new(uuid_store, index_store))
}

pub struct IndexResolver<U, I> {
    index_uuid_store: U,
    index_store: I,
}

impl<U, I> IndexResolver<U ,I>
where U: UuidStore,
      I: IndexStore,
{
    pub fn new(
        index_uuid_store: U,
        index_store: I,
        ) -> Self {
        Self {
            index_uuid_store,
            index_store,
        }
    }

    pub async fn dump(&self, _path: impl AsRef<Path>) -> Result<Vec<Uuid>> {
        todo!()
    }

    pub async fn get_size(&self) -> Result<u64> {
        todo!()
        //Ok(self.index_store.get_size()? + self.index_uuid_store.get_size().await?)
    }

    pub async fn snapshot(&self, path: impl AsRef<Path>) -> Result<Vec<Index>> {
        let uuids = self.index_uuid_store.snapshot(path.as_ref().to_owned()).await?;
        let mut indexes = Vec::new();

        for uuid in uuids {
            indexes.push(self.get_index_by_uuid(uuid).await?);
        }

        Ok(indexes)
    }

    pub async fn create_index(&self, uid: String, primary_key: Option<String>) -> Result<(Uuid, Index)> {
        let uuid = Uuid::new_v4();
        let index = self.index_store.create(uuid, primary_key).await?;
        self.index_uuid_store.insert(uid, uuid).await?;
        Ok((uuid, index))
    }

    pub async fn list(&self) -> Result<Vec<(String, Index)>> {
        let uuids = self.index_uuid_store.list().await?;
        let mut indexes = Vec::new();
        for (name, uuid) in uuids {
            match self.index_store.get(uuid).await? {
                Some(index) => {
                    indexes.push((name, index))
                },
                None => {
                    // we found an unexisting index, we remove it from the uuid store
                    let _ = self.index_uuid_store.delete(name).await;
                },
            }
        }

        Ok(indexes)
    }

    pub async fn delete_index(&self, uid: String) -> Result<()> {
        match self.index_uuid_store.delete(uid.clone()).await? {
            Some(uuid) => {
                let _ = self.index_store.delete(uuid).await;
                Ok(())
            }
            None => Err(IndexResolverError::UnexistingIndex(uid)),
        }
    }

    pub async fn get_index_by_uuid(&self, uuid: Uuid) -> Result<Index> {
        // TODO: Handle this error better.
        self.index_store.get(uuid).await?.ok_or(IndexResolverError::UnexistingIndex(String::new()))
    }

    pub async fn get_index(&self, uid: String) -> Result<Index> {
        match self.index_uuid_store.get_uuid(uid).await? {
            (name, Some(uuid)) => {
                match self.index_store.get(uuid).await? {
                    Some(index) => Ok(index),
                    None => {
                        // For some reason we got a uuid to an unexisting index, we return an error,
                        // and remove the uuid from th uuid store.
                        let _ = self.index_uuid_store.delete(name.clone()).await;
                        Err(IndexResolverError::UnexistingIndex(name))
                    },
                }
            }
            (name, _) => Err(IndexResolverError::UnexistingIndex(name))
        }
    }

    pub async fn get_uuid(&self, uid: String) -> Result<Uuid> {
        match self.index_uuid_store.get_uuid(uid).await? {
            (_, Some(uuid)) => Ok(uuid),
            (name, _) => Err(IndexResolverError::UnexistingIndex(name))
        }
    }
}

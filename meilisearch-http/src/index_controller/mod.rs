mod index_actor;
mod update_actor;
mod update_handler;
mod update_store;
mod updates;
mod uuid_resolver;
mod snapshot;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use actix_web::web::{Bytes, Payload};
use anyhow::bail;
use futures::stream::StreamExt;
use milli::update::{IndexDocumentsMethod, UpdateFormat};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::index::{Document, SearchQuery, SearchResult};
use crate::index::{Facets, Settings, UpdateResult};

pub use updates::{Failed, Processed, Processing};
use snapshot::SnapshotService;

pub type UpdateStatus = updates::UpdateStatus<UpdateMeta, UpdateResult, String>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMetadata {
    uid: String,
    name: String,
    #[serde(flatten)]
    meta: index_actor::IndexMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMeta {
    DocumentsAddition {
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        primary_key: Option<String>,
    },
    ClearDocuments,
    DeleteDocuments,
    Settings(Settings),
    Facets(Facets),
}

#[derive(Clone, Debug)]
pub struct IndexSettings {
    pub uid: Option<String>,
    pub primary_key: Option<String>,
}

pub struct IndexController {
    uuid_resolver: uuid_resolver::UuidResolverHandle,
    index_handle: index_actor::IndexActorHandle,
    update_handle: update_actor::UpdateActorHandle<Bytes>,
}

impl IndexController {
    pub fn new(
        path: impl AsRef<Path>,
        index_size: usize,
        update_store_size: usize,
    ) -> anyhow::Result<Self> {
        let uuid_resolver = uuid_resolver::UuidResolverHandle::new(&path)?;
        let index_handle = index_actor::IndexActorHandle::new(&path, index_size)?;
        let update_handle =
            update_actor::UpdateActorHandle::new(index_handle.clone(), &path, update_store_size)?;
        let snapshot_service = SnapshotService::new(
            index_handle.clone(),
            uuid_resolver.clone(),
            update_handle.clone(),
            Duration::from_millis(10000),
            "/dev/toto".into());
        tokio::task::spawn(snapshot_service.run());
        Ok(Self {
            uuid_resolver,
            index_handle,
            update_handle,
        })
    }

    pub async fn add_documents(
        &self,
        uid: String,
        method: milli::update::IndexDocumentsMethod,
        format: milli::update::UpdateFormat,
        mut payload: Payload,
        primary_key: Option<String>,
    ) -> anyhow::Result<UpdateStatus> {
        let uuid = self.uuid_resolver.get_or_create(uid).await?;
        let meta = UpdateMeta::DocumentsAddition {
            method,
            format,
            primary_key,
        };
        let (sender, receiver) = mpsc::channel(10);

        // It is necessary to spawn a local task to senf the payload to the update handle to
        // prevent dead_locking between the update_handle::update that waits for the update to be
        // registered and the update_actor that waits for the the payload to be sent to it.
        tokio::task::spawn_local(async move {
            while let Some(bytes) = payload.next().await {
                match bytes {
                    Ok(bytes) => {
                        let _ = sender.send(Ok(bytes)).await;
                    }
                    Err(e) => {
                        let error: Box<dyn std::error::Error + Sync + Send + 'static> = Box::new(e);
                        let _ = sender.send(Err(error)).await;
                    }
                }
            }
        });

        // This must be done *AFTER* spawning the task.
        let status = self.update_handle.update(meta, receiver, uuid).await?;
        Ok(status)
    }

    pub async fn clear_documents(&self, uid: String) -> anyhow::Result<UpdateStatus> {
        let uuid = self.uuid_resolver.resolve(uid).await?;
        let meta = UpdateMeta::ClearDocuments;
        let (_, receiver) = mpsc::channel(1);
        let status = self.update_handle.update(meta, receiver, uuid).await?;
        Ok(status)
    }

    pub async fn delete_documents(
        &self,
        uid: String,
        document_ids: Vec<String>,
    ) -> anyhow::Result<UpdateStatus> {
        let uuid = self.uuid_resolver.resolve(uid).await?;
        let meta = UpdateMeta::DeleteDocuments;
        let (sender, receiver) = mpsc::channel(10);

        tokio::task::spawn(async move {
            let json = serde_json::to_vec(&document_ids).unwrap();
            let bytes = Bytes::from(json);
            let _ = sender.send(Ok(bytes)).await;
        });

        let status = self.update_handle.update(meta, receiver, uuid).await?;
        Ok(status)
    }

    pub async fn update_settings(
        &self,
        uid: String,
        settings: Settings,
        create: bool,
    ) -> anyhow::Result<UpdateStatus> {
        let uuid = if create {
            let uuid = self.uuid_resolver.get_or_create(uid).await?;
            // We need to create the index upfront, since it would otherwise only be created when
            // the update is processed. This would make calls to GET index to fail until the update
            // is complete. Since this is get or create, we ignore the error when the index already
            // exists.
            match self.index_handle.create_index(uuid, None).await {
                Ok(_) | Err(index_actor::IndexError::IndexAlreadyExists) => (),
                Err(e) => return Err(e.into()),
            }
            uuid
        } else {
            self.uuid_resolver.resolve(uid).await?
        };
        let meta = UpdateMeta::Settings(settings);
        // Nothing so send, drop the sender right away, as not to block the update actor.
        let (_, receiver) = mpsc::channel(1);

        let status = self.update_handle.update(meta, receiver, uuid).await?;
        Ok(status)
    }

    pub async fn create_index(
        &self,
        index_settings: IndexSettings,
    ) -> anyhow::Result<IndexMetadata> {
        let IndexSettings { uid, primary_key } = index_settings;
        let uid = uid.ok_or_else(|| anyhow::anyhow!("Can't create an index without a uid."))?;
        let uuid = self.uuid_resolver.create(uid.clone()).await?;
        let meta = self.index_handle.create_index(uuid, primary_key).await?;
        let _ = self.update_handle.create(uuid).await?;
        let meta = IndexMetadata { name: uid.clone(), uid, meta };

        Ok(meta)
    }

    pub async fn delete_index(&self, uid: String) -> anyhow::Result<()> {
        let uuid = self.uuid_resolver.delete(uid).await?;
        self.update_handle.delete(uuid).await?;
        self.index_handle.delete(uuid).await?;
        Ok(())
    }

    pub async fn update_status(&self, uid: String, id: u64) -> anyhow::Result<UpdateStatus> {
        let uuid = self.uuid_resolver.resolve(uid).await?;
        let result = self.update_handle.update_status(uuid, id).await?;
        Ok(result)
    }

    pub async fn all_update_status(&self, uid: String) -> anyhow::Result<Vec<UpdateStatus>> {
        let uuid = self.uuid_resolver.resolve(uid).await?;
        let result = self.update_handle.get_all_updates_status(uuid).await?;
        Ok(result)
    }

    pub async fn list_indexes(&self) -> anyhow::Result<Vec<IndexMetadata>> {
        let uuids = self.uuid_resolver.list().await?;

        let mut ret = Vec::new();

        for (uid, uuid) in uuids {
            let meta = self.index_handle.get_index_meta(uuid).await?;
            let meta = IndexMetadata { name: uid.clone(), uid, meta };
            ret.push(meta);
        }

        Ok(ret)
    }

    pub async fn settings(&self, uid: String) -> anyhow::Result<Settings> {
        let uuid = self.uuid_resolver.resolve(uid.clone()).await?;
        let settings = self.index_handle.settings(uuid).await?;
        Ok(settings)
    }

    pub async fn documents(
        &self,
        uid: String,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> anyhow::Result<Vec<Document>> {
        let uuid = self.uuid_resolver.resolve(uid.clone()).await?;
        let documents = self
            .index_handle
            .documents(uuid, offset, limit, attributes_to_retrieve)
            .await?;
        Ok(documents)
    }

    pub async fn document(
        &self,
        uid: String,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> anyhow::Result<Document> {
        let uuid = self.uuid_resolver.resolve(uid.clone()).await?;
        let document = self
            .index_handle
            .document(uuid, doc_id, attributes_to_retrieve)
            .await?;
        Ok(document)
    }

    pub async fn update_index(
        &self,
        uid: String,
        index_settings: IndexSettings,
    ) -> anyhow::Result<IndexMetadata> {
        if index_settings.uid.is_some() {
            bail!("Can't change the index uid.")
        }

        let uuid = self.uuid_resolver.resolve(uid.clone()).await?;
        let meta = self.index_handle.update_index(uuid, index_settings).await?;
        let meta = IndexMetadata { name: uid.clone(), uid, meta };
        Ok(meta)
    }

    pub async fn search(&self, uid: String, query: SearchQuery) -> anyhow::Result<SearchResult> {
        let uuid = self.uuid_resolver.resolve(uid).await?;
        let result = self.index_handle.search(uuid, query).await?;
        Ok(result)
    }

    pub async fn get_index(&self, uid: String) -> anyhow::Result<IndexMetadata> {
        let uuid = self.uuid_resolver.resolve(uid.clone()).await?;
        let meta = self.index_handle.get_index_meta(uuid).await?;
        let meta = IndexMetadata { name: uid.clone(), uid, meta };
        Ok(meta)
    }
}

pub async fn get_arc_ownership_blocking<T>(mut item: Arc<T>) -> T {
    loop {
        match Arc::try_unwrap(item) {
            Ok(item) => return item,
            Err(item_arc) => {
                item = item_arc;
                sleep(Duration::from_millis(100)).await;
                continue;
            }
        }
    }
}

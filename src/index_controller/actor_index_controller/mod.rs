mod index_actor;
mod update_actor;
mod uuid_resolver;
mod update_store;
mod update_handler;

use std::path::Path;

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;
use super::IndexMetadata;
use futures::stream::StreamExt;
use actix_web::web::Payload;
use super::UpdateMeta;
use crate::index::{SearchResult, SearchQuery};
use actix_web::web::Bytes;

pub struct IndexController {
    uuid_resolver: uuid_resolver::UuidResolverHandle,
    index_handle: index_actor::IndexActorHandle,
    update_handle: update_actor::UpdateActorHandle<Bytes>,
}

enum IndexControllerMsg {
    CreateIndex {
        uuid: Uuid,
        primary_key: Option<String>,
        ret: oneshot::Sender<anyhow::Result<IndexMetadata>>,
    },
    Shutdown,
}

impl IndexController {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let uuid_resolver = uuid_resolver::UuidResolverHandle::new();
        let index_actor = index_actor::IndexActorHandle::new(&path);
        let update_handle = update_actor::UpdateActorHandle::new(index_actor.clone(), &path);
        Self { uuid_resolver, index_handle: index_actor, update_handle }
    }

    pub async fn add_documents(
        &self,
        index: String,
        method: milli::update::IndexDocumentsMethod,
        format: milli::update::UpdateFormat,
        mut payload: Payload,
        primary_key: Option<String>,
    ) -> anyhow::Result<super::UpdateStatus> {
        let uuid = self.uuid_resolver.get_or_create(index).await?;
        let meta = UpdateMeta::DocumentsAddition { method, format, primary_key };
        let (sender, receiver) = mpsc::channel(10);

        // It is necessary to spawn a local task to senf the payload to the update handle to
        // prevent dead_locking between the update_handle::update that waits for the update to be
        // registered and the update_actor that waits for the the payload to be sent to it.
        tokio::task::spawn_local(async move {
            while let Some(bytes) = payload.next().await {
                match bytes {
                    Ok(bytes) => { sender.send(Ok(bytes)).await; },
                    Err(e) => {
                        let error: Box<dyn std::error::Error + Sync + Send + 'static> = Box::new(e);
                        sender.send(Err(error)).await; },
                }
            }
        });

        // This must be done *AFTER* spawning the task.
        let status = self.update_handle.update(meta, receiver, uuid).await?;
        Ok(status)
    }

    fn clear_documents(&self, index: String) -> anyhow::Result<super::UpdateStatus> {
        todo!()
    }

    fn delete_documents(&self, index: String, document_ids: Vec<String>) -> anyhow::Result<super::UpdateStatus> {
        todo!()
    }

    fn update_settings(&self, index_uid: String, settings: super::Settings) -> anyhow::Result<super::UpdateStatus> {
        todo!()
    }

    pub async fn create_index(&self, index_settings: super::IndexSettings) -> anyhow::Result<super::IndexMetadata> {
        let super::IndexSettings { name, primary_key } = index_settings;
        let uuid = self.uuid_resolver.create(name.unwrap()).await?;
        let index_meta = self.index_handle.create_index(uuid, primary_key).await?;
        Ok(index_meta)
    }

    fn delete_index(&self, index_uid: String) -> anyhow::Result<()> {
        todo!()
    }

    fn swap_indices(&self, index1_uid: String, index2_uid: String) -> anyhow::Result<()> {
        todo!()
    }

    pub fn index(&self, name: String) -> anyhow::Result<Option<std::sync::Arc<milli::Index>>> {
        todo!()
    }

    fn update_status(&self, index: String, id: u64) -> anyhow::Result<Option<super::UpdateStatus>> {
        todo!()
    }

    fn all_update_status(&self, index: String) -> anyhow::Result<Vec<super::UpdateStatus>> {
        todo!()
    }

    pub fn list_indexes(&self) -> anyhow::Result<Vec<super::IndexMetadata>> {
        todo!()
    }

    fn update_index(&self, name: String, index_settings: super::IndexSettings) -> anyhow::Result<super::IndexMetadata> {
        todo!()
    }

    pub async fn search(&self, name: String, query: SearchQuery) -> anyhow::Result<SearchResult> {
        let uuid = self.uuid_resolver.resolve(name).await.unwrap().unwrap();
        let result = self.index_handle.search(uuid, query).await?;
        Ok(result)
    }
}

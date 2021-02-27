mod index_actor;
mod update_actor;
mod uuid_resolver;
mod update_store;
mod update_handler;

use tokio::sync::oneshot;
use super::IndexController;
use uuid::Uuid;
use super::IndexMetadata;
use tokio::fs::File;
use super::UpdateMeta;
use crate::data::{SearchResult, SearchQuery};

pub struct ActorIndexController {
    uuid_resolver: uuid_resolver::UuidResolverHandle,
    index_handle: index_actor::IndexActorHandle,
    update_handle: update_actor::UpdateActorHandle,
}

impl ActorIndexController {
    pub fn new() -> Self {
        let uuid_resolver = uuid_resolver::UuidResolverHandle::new();
        let index_actor = index_actor::IndexActorHandle::new();
        let update_handle = update_actor::UpdateActorHandle::new(index_actor.clone());
        Self { uuid_resolver, index_handle: index_actor, update_handle }
    }
}

enum IndexControllerMsg {
    CreateIndex {
        uuid: Uuid,
        primary_key: Option<String>,
        ret: oneshot::Sender<anyhow::Result<IndexMetadata>>,
    },
    Shutdown,
}

#[async_trait::async_trait(?Send)]
impl IndexController for ActorIndexController {
    async fn add_documents(
        &self,
        index: String,
        method: milli::update::IndexDocumentsMethod,
        format: milli::update::UpdateFormat,
        data: File,
        primary_key: Option<String>,
    ) -> anyhow::Result<super::UpdateStatus> {
        let uuid = self.uuid_resolver.get_or_create(index).await?;
        let meta = UpdateMeta::DocumentsAddition { method, format, primary_key };
        let status = self.update_handle.update(meta, Some(data), uuid).await?;
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

    async fn create_index(&self, index_settings: super::IndexSettings) -> anyhow::Result<super::IndexMetadata> {
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

    fn index(&self, name: String) -> anyhow::Result<Option<std::sync::Arc<milli::Index>>> {
        todo!()
    }

    fn update_status(&self, index: String, id: u64) -> anyhow::Result<Option<super::UpdateStatus>> {
        todo!()
    }

    fn all_update_status(&self, index: String) -> anyhow::Result<Vec<super::UpdateStatus>> {
        todo!()
    }

    fn list_indexes(&self) -> anyhow::Result<Vec<super::IndexMetadata>> {
        todo!()
    }

    fn update_index(&self, name: String, index_settings: super::IndexSettings) -> anyhow::Result<super::IndexMetadata> {
        todo!()
    }

    async fn search(&self, name: String, query: SearchQuery) -> anyhow::Result<SearchResult> {
        let uuid = self.uuid_resolver.resolve(name).await.unwrap().unwrap();
        let result = self.index_handle.search(uuid, query).await?;
        Ok(result)
    }
}

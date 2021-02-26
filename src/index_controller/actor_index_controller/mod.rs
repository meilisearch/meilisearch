mod index_actor;
mod update_actor;
mod uuid_resolver;

use tokio::fs::File;
use tokio::sync::oneshot;
use super::IndexController;
use uuid::Uuid;
use super::IndexMetadata;


pub struct ActorIndexController {
    uuid_resolver: uuid_resolver::UuidResolverHandle,
    index_actor: index_actor::IndexActorHandle,
}

impl ActorIndexController {
    pub fn new() -> Self {
        let uuid_resolver = uuid_resolver::UuidResolverHandle::new();
        let index_actor = index_actor::IndexActorHandle::new();
        Self { uuid_resolver, index_actor }
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

#[async_trait::async_trait]
impl IndexController for ActorIndexController {
    async fn add_documents(
        &self,
        index: String,
        method: milli::update::IndexDocumentsMethod,
        format: milli::update::UpdateFormat,
        data: File,
        primary_key: Option<String>,
    ) -> anyhow::Result<super::UpdateStatus> {
        todo!()
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
        let index_meta = self.index_actor.create_index(uuid, primary_key).await?;
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
}

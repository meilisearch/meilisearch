use milli::update::{IndexDocumentsMethod, UpdateFormat};
use actix_web::web::Payload;

use crate::index_controller::{IndexMetadata, IndexSettings, UpdateStatus};
use crate::index::Settings;
use super::Data;


impl Data {
    pub async fn add_documents(
        &self,
        index: impl AsRef<str> + Send + Sync + 'static,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        stream: Payload,
        primary_key: Option<String>,
    ) -> anyhow::Result<UpdateStatus>
    {
        let update_status = self.index_controller.add_documents(index.as_ref().to_string(), method, format, stream, primary_key).await?;
        Ok(update_status)
    }

    pub async fn update_settings(
        &self,
        index: String,
        settings: Settings,
        create: bool,
    ) -> anyhow::Result<UpdateStatus> {
        let update = self.index_controller.update_settings(index, settings, create).await?;
        Ok(update.into())
    }

    pub async fn clear_documents(
        &self,
        index: impl AsRef<str> + Sync + Send + 'static,
    ) -> anyhow::Result<UpdateStatus> {
        let update = self.index_controller.clear_documents(index.as_ref().to_string()).await?;
        Ok(update)
    }

    pub async fn delete_documents(
        &self,
        index: impl AsRef<str> + Sync + Send + 'static,
        document_ids: Vec<String>,
    ) -> anyhow::Result<UpdateStatus> {
        let update = self.index_controller.delete_documents(index.as_ref().to_string(), document_ids).await?;
        Ok(update.into())
    }

    pub async fn delete_index(
        &self,
        index: impl AsRef<str> + Send + Sync + 'static,
    ) -> anyhow::Result<()> {
        self.index_controller.delete_index(index.as_ref().to_owned()).await?;
        Ok(())
    }

    pub async fn get_update_status(&self, index: impl AsRef<str>, uid: u64) -> anyhow::Result<Option<UpdateStatus>> {
        self.index_controller.update_status(index.as_ref().to_string(), uid).await
    }

    pub async fn get_updates_status(&self, index: impl AsRef<str>) -> anyhow::Result<Vec<UpdateStatus>> {
        self.index_controller.all_update_status(index.as_ref().to_string()).await
    }

    pub async fn update_index(
        &self,
        uid: impl AsRef<str>,
        primary_key: Option<impl AsRef<str>>,
        new_uid: Option<impl AsRef<str>>
    ) -> anyhow::Result<IndexMetadata> {
        let settings = IndexSettings {
            uid: new_uid.map(|s| s.as_ref().to_string()),
            primary_key: primary_key.map(|s| s.as_ref().to_string()),
        };

        self.index_controller.update_index(uid.as_ref().to_string(), settings).await
    }
}

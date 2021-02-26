use std::ops::Deref;

//use async_compression::tokio_02::write::GzipEncoder;
//use futures_util::stream::StreamExt;
//use milli::update::{IndexDocumentsMethod, UpdateFormat};
//use tokio::io::AsyncWriteExt;
use actix_web::web::Payload;

use crate::index_controller::UpdateStatus;
use crate::index_controller::{Settings, IndexMetadata};
use super::Data;

impl Data {
    pub async fn add_documents<B, E>(
        &self,
        index: impl AsRef<str> + Send + Sync + 'static,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        stream: Payload,
        primary_key: Option<String>,
    ) -> anyhow::Result<UpdateStatus>
    where
        B: Deref<Target = [u8]>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let update_status = self.index_controller.add_documents(index.as_ref().to_string(), method, format, stream, primary_key).await?;
        Ok(update_status)
    }

    pub async fn update_settings(
        &self,
        _index: impl AsRef<str> + Send + Sync + 'static,
        _settings: Settings
    ) -> anyhow::Result<UpdateStatus> {
        todo!()
        //let index_controller = self.index_controller.clone();
        //let update = tokio::task::spawn_blocking(move || index_controller.update_settings(index, settings)).await??;
        //Ok(update.into())
    }

    pub async fn clear_documents(
        &self,
        _index: impl AsRef<str> + Sync + Send + 'static,
    ) -> anyhow::Result<UpdateStatus> {
        todo!()
        //let index_controller = self.index_controller.clone();
        //let update = tokio::task::spawn_blocking(move || index_controller.clear_documents(index)).await??;
        //Ok(update.into())
    }

    pub async fn delete_documents(
        &self,
        _index: impl AsRef<str> + Sync + Send + 'static,
        _document_ids: Vec<String>,
    ) -> anyhow::Result<UpdateStatus> {
        todo!()
        //let index_controller = self.index_controller.clone();
        //let update = tokio::task::spawn_blocking(move || index_controller.delete_documents(index, document_ids)).await??;
        //Ok(update.into())
    }

    pub async fn delete_index(
        &self,
        _index: impl AsRef<str> + Send + Sync + 'static,
    ) -> anyhow::Result<()> {
        todo!()
        //let index_controller = self.index_controller.clone();
        //tokio::task::spawn_blocking(move || { index_controller.delete_index(index) }).await??;
        //Ok(())
    }

    #[inline]
    pub fn get_update_status(&self, index: impl AsRef<str>, uid: u64) -> anyhow::Result<Option<UpdateStatus>> {
        todo!()
        //self.index_controller.update_status(index, uid)
    }

    pub fn get_updates_status(&self, index: impl AsRef<str>) -> anyhow::Result<Vec<UpdateStatus>> {
        todo!()
        //self.index_controller.all_update_status(index)
    }

    pub fn update_index(
        &self,
        name: impl AsRef<str>,
        primary_key: Option<impl AsRef<str>>,
        new_name: Option<impl AsRef<str>>
    ) -> anyhow::Result<IndexMetadata> {
        todo!()
        //let settings = IndexSettings {
            //name: new_name.map(|s| s.as_ref().to_string()),
            //primary_key: primary_key.map(|s| s.as_ref().to_string()),
        //};

        //self.index_controller.update_index(name, settings)
    }
}

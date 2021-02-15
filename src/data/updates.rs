use std::ops::Deref;

use milli::update::{IndexDocumentsMethod, UpdateFormat};
use async_compression::tokio_02::write::GzipEncoder;
use futures_util::stream::StreamExt;
use tokio::io::AsyncWriteExt;

use super::Data;
use crate::index_controller::{IndexController, Settings, IndexSettings, IndexMetadata};
use crate::index_controller::UpdateStatus;

impl Data {
    pub async fn add_documents<B, E>(
        &self,
        index: impl AsRef<str> + Send + Sync + 'static,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        mut stream: impl futures::Stream<Item=Result<B, E>> + Unpin,
        primary_key: Option<String>,
    ) -> anyhow::Result<UpdateStatus>
    where
        B: Deref<Target = [u8]>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let file = tokio::task::spawn_blocking(tempfile::tempfile).await?;
        let file = tokio::fs::File::from_std(file?);
        let mut encoder = GzipEncoder::new(file);

        let mut empty_update = true;
        while let Some(result) = stream.next().await {
            empty_update = false;
            let bytes = &*result?;
            encoder.write_all(&bytes[..]).await?;
        }

        encoder.shutdown().await?;
        let mut file = encoder.into_inner();
        file.sync_all().await?;
        let file = file.into_std().await;


        let index_controller = self.index_controller.clone();
        let update = tokio::task::spawn_blocking(move ||{
            let mmap;
            let bytes = if empty_update {
                &[][..]
            } else {
                mmap = unsafe { memmap::Mmap::map(&file)? };
                &mmap
            };
            index_controller.add_documents(index, method, format, &bytes, primary_key)
        }).await??;
        Ok(update.into())
    }

    pub async fn update_settings(
        &self,
        index: impl AsRef<str> + Send + Sync + 'static,
        settings: Settings
    ) -> anyhow::Result<UpdateStatus> {
        let index_controller = self.index_controller.clone();
        let update = tokio::task::spawn_blocking(move || index_controller.update_settings(index, settings)).await??;
        Ok(update.into())
    }

    pub async fn clear_documents(
        &self,
        index: impl AsRef<str> + Sync + Send + 'static,
    ) -> anyhow::Result<UpdateStatus> {
        let index_controller = self.index_controller.clone();
        let update = tokio::task::spawn_blocking(move || index_controller.clear_documents(index)).await??;
        Ok(update.into())
    }

    pub async fn delete_documents(
        &self,
        index: impl AsRef<str> + Sync + Send + 'static,
        document_ids: Vec<String>,
    ) -> anyhow::Result<UpdateStatus> {
        let index_controller = self.index_controller.clone();
        let update = tokio::task::spawn_blocking(move || index_controller.delete_documents(index, document_ids)).await??;
        Ok(update.into())
    }

    #[inline]
    pub fn get_update_status(&self, index: impl AsRef<str>, uid: u64) -> anyhow::Result<Option<UpdateStatus>> {
        self.index_controller.update_status(index, uid)
    }

    pub fn get_updates_status(&self, index: impl AsRef<str>) -> anyhow::Result<Vec<UpdateStatus>> {
        self.index_controller.all_update_status(index)
    }

    pub fn update_index(
        &self,
        name: impl AsRef<str>,
        primary_key: Option<impl AsRef<str>>,
        new_name: Option<impl AsRef<str>>
    ) -> anyhow::Result<IndexMetadata> {
        let settings = IndexSettings {
            name: new_name.map(|s| s.as_ref().to_string()),
            primary_key: primary_key.map(|s| s.as_ref().to_string()),
        };

        self.index_controller.update_index(name, settings)
    }
}

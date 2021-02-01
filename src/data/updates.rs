use std::ops::Deref;

use milli::update::{IndexDocumentsMethod, UpdateFormat};
use async_compression::tokio_02::write::GzipEncoder;
use futures_util::stream::StreamExt;
use tokio::io::AsyncWriteExt;

use super::Data;
use crate::index_controller::{IndexController, Settings};
use crate::index_controller::UpdateStatus;

impl Data {
    pub async fn add_documents<B, E>(
        &self,
        index: impl AsRef<str> + Send + Sync + 'static,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        mut stream: impl futures::Stream<Item=Result<B, E>> + Unpin,
    ) -> anyhow::Result<UpdateStatus>
    where
        B: Deref<Target = [u8]>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let file = tokio::task::spawn_blocking(tempfile::tempfile).await?;
        let file = tokio::fs::File::from_std(file?);
        let mut encoder = GzipEncoder::new(file);

        while let Some(result) = stream.next().await {
            let bytes = &*result?;
            encoder.write_all(&bytes[..]).await?;
        }

        encoder.shutdown().await?;
        let mut file = encoder.into_inner();
        file.sync_all().await?;
        let file = file.into_std().await;
        let mmap = unsafe { memmap::Mmap::map(&file)? };

        let index_controller = self.index_controller.clone();
        let update = tokio::task::spawn_blocking(move || index_controller.add_documents(index, method, format, &mmap[..])).await??;
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

    #[inline]
    pub fn get_update_status(&self, index: impl AsRef<str>, uid: u64) -> anyhow::Result<Option<UpdateStatus>> {
        self.index_controller.update_status(index, uid)
    }

    pub fn get_updates_status(&self, index: impl AsRef<str>) -> anyhow::Result<Vec<UpdateStatus>> {
        self.index_controller.all_update_status(index)
    }
}

use std::ops::Deref;

use milli::update::{IndexDocumentsMethod, UpdateFormat};
//use milli::update_store::UpdateStatus;
use async_compression::tokio_02::write::GzipEncoder;
use futures_util::stream::StreamExt;
use tokio::io::AsyncWriteExt;

use super::Data;
use crate::index_controller::IndexController;
use crate::index_controller::{UpdateStatusResponse, Settings};


impl Data {
        pub async fn add_documents<B, E, S>(
        &self,
        index: S,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        mut stream: impl futures::Stream<Item=Result<B, E>> + Unpin,
    ) -> anyhow::Result<UpdateStatusResponse>
    where
        B: Deref<Target = [u8]>,
        E: std::error::Error + Send + Sync + 'static,
        S: AsRef<str> + Send + Sync + 'static,
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

        let indexes = self.indexes.clone();
        let update = tokio::task::spawn_blocking(move ||indexes.add_documents(index, method, format, &mmap[..])).await??;
        Ok(update.into())
    }

    pub async fn update_settings<S: AsRef<str> + Send + Sync + 'static>(
        &self,
        index: S,
        settings: Settings
    ) -> anyhow::Result<UpdateStatusResponse> {
        let indexes = self.indexes.clone();
        let update = tokio::task::spawn_blocking(move || indexes.update_settings(index, settings)).await??;
        Ok(update.into())
    }

    //#[inline]
    //pub fn get_update_status<S: AsRef<str>>(&self, _index: S, uid: u64) -> anyhow::Result<Option<UpdateStatus<UpdateMeta, UpdateResult, String>>> {
        //self.indexes.get_update_status(uid)
    //}

    //pub fn get_updates_status(&self, _index: &str) -> anyhow::Result<Vec<UpdateStatus<UpdateMeta, UpdateResult, String>>> {
        //let result = self.update_queue.iter_metas(|processing, processed, pending, aborted, failed| {
            //let mut metas = processing
            //.map(UpdateStatus::from)
            //.into_iter()
            //.chain(processed.filter_map(|i| Some(i.ok()?.1)).map(UpdateStatus::from))
            //.chain(pending.filter_map(|i| Some(i.ok()?.1)).map(UpdateStatus::from))
            //.chain(aborted.filter_map(|i| Some(i.ok()?.1)).map(UpdateStatus::from))
            //.chain(failed.filter_map(|i| Some(i.ok()?.1)).map(UpdateStatus::from))
            //.collect::<Vec<_>>();
            //metas.sort_by(|a, b| a.id().cmp(&b.id()));
            //Ok(metas)
        //})?;
        //Ok(result)
    //}
}

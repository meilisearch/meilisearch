use std::ops::Deref;

use async_compression::tokio_02::write::GzipEncoder;
use futures_util::stream::StreamExt;
use tokio::io::AsyncWriteExt;
use milli::update::{IndexDocumentsMethod, UpdateFormat};
use milli::update_store::UpdateStatus;

use super::Data;
use crate::updates::{UpdateMeta, UpdateResult};

impl Data {
        pub async fn add_documents<B, E, S>(
        &self,
        _index: S,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        mut stream: impl futures::Stream<Item=Result<B, E>> + Unpin,
    ) -> anyhow::Result<UpdateStatus<UpdateMeta, String, String>>
    where
        B: Deref<Target = [u8]>,
        E: std::error::Error + Send + Sync + 'static,
        S: AsRef<str>,
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

        let meta = UpdateMeta::DocumentsAddition { method, format };

        let queue = self.update_queue.clone();
        let update = tokio::task::spawn_blocking(move || queue.register_update(meta, &mmap[..])).await??;

        Ok(update.into())
    }


    #[inline]
    pub fn get_update_status(&self, _index: &str, uid: u64) -> anyhow::Result<Option<UpdateStatus<UpdateMeta, UpdateResult, String>>> {
        self.update_queue.get_update_status(uid)
    }

    pub fn get_updates_status(&self, _index: &str) -> anyhow::Result<Vec<UpdateStatus<UpdateMeta, UpdateResult, String>>> {
        let result = self.update_queue.iter_metas(|processing, processed, pending, aborted, failed| {
            let mut metas = processing
            .map(UpdateStatus::from)
            .into_iter()
            .chain(processed.filter_map(|i| Some(i.ok()?.1)).map(UpdateStatus::from))
            .chain(pending.filter_map(|i| Some(i.ok()?.1)).map(UpdateStatus::from))
            .chain(aborted.filter_map(|i| Some(i.ok()?.1)).map(UpdateStatus::from))
            .chain(failed.filter_map(|i| Some(i.ok()?.1)).map(UpdateStatus::from))
            .collect::<Vec<_>>();
            metas.sort_by(|a, b| a.id().cmp(&b.id()));
            Ok(metas)
        })?;
        Ok(result)
    }
}

use std::ops::Deref;
use std::sync::Arc;
use std::fs::create_dir_all;

use async_compression::tokio_02::write::GzipEncoder;
use futures_util::stream::StreamExt;
use tokio::io::AsyncWriteExt;
use milli::Index;
use milli::update::{IndexDocumentsMethod, UpdateFormat};
use sha2::Digest;

use crate::option::Opt;
use crate::updates::{UpdateQueue, UpdateMeta, UpdateStatus, UpdateMetaProgress};

#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
}

impl Deref for Data {
    type Target = DataInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone)]
pub struct DataInner {
    pub indexes: Arc<Index>,
    pub update_queue: Arc<UpdateQueue>,
    api_keys: ApiKeys,
    options: Opt,
}

#[derive(Clone)]
pub struct ApiKeys {
    pub public: Option<String>,
    pub private: Option<String>,
    pub master: Option<String>,
}

impl ApiKeys {
    pub fn generate_missing_api_keys(&mut self) {
        if let Some(master_key) = &self.master {
            if self.private.is_none() {
                let key = format!("{}-private", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.private = Some(format!("{:x}", sha));
            }
            if self.public.is_none() {
                let key = format!("{}-public", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.public = Some(format!("{:x}", sha));
            }
        }
    }
}

impl Data {
    pub fn new(options: Opt) -> anyhow::Result<Data> {
        let db_size = options.max_mdb_size.get_bytes() as usize;
        let path = options.db_path.join("main");
        create_dir_all(&path)?;
        let indexes = Index::new(&path, Some(db_size))?;
        let indexes = Arc::new(indexes);

        let update_queue = Arc::new(UpdateQueue::new(&options, indexes.clone())?);

        let mut api_keys = ApiKeys {
            master: options.clone().master_key,
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let inner = DataInner { indexes, options, update_queue, api_keys };
        let inner = Arc::new(inner);

        Ok(Data { inner })
    }

    pub async fn add_documents<B, E>(
        &self,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        mut stream: impl futures::Stream<Item=Result<B, E>> + Unpin,
    ) -> anyhow::Result<UpdateStatus<UpdateMeta, UpdateMetaProgress, String>>
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

        let meta = UpdateMeta::DocumentsAddition { method, format };

        let queue = self.update_queue.clone();
        let meta_cloned = meta.clone();
        let update_id = tokio::task::spawn_blocking(move || queue.register_update(&meta_cloned, &mmap[..])).await??;

        Ok(UpdateStatus::Pending { update_id, meta })
    }

    #[inline]
    pub fn http_payload_size_limit(&self) -> usize {
        self.options.http_payload_size_limit.get_bytes() as usize
    }

    #[inline]
    pub fn api_keys(&self) -> &ApiKeys {
        &self.api_keys
    }
}

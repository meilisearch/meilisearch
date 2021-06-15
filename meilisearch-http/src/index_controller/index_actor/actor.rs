use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use async_stream::stream;
use futures::stream::StreamExt;
use heed::CompactionOption;
use log::debug;
use milli::update::UpdateBuilder;
use tokio::task::spawn_blocking;
use tokio::{fs, sync::mpsc};
use uuid::Uuid;

use crate::index::{
    update_handler::UpdateHandler, Checked, Document, SearchQuery, SearchResult, Settings,
};
use crate::index_controller::{
    get_arc_ownership_blocking, Failed, IndexStats, Processed, Processing,
};
use crate::option::IndexerOpts;

use super::error::{IndexActorError, Result};
use super::{IndexMeta, IndexMsg, IndexSettings, IndexStore};

pub const CONCURRENT_INDEX_MSG: usize = 10;

pub struct IndexActor<S> {
    receiver: Option<mpsc::Receiver<IndexMsg>>,
    update_handler: Arc<UpdateHandler>,
    store: S,
}

impl<S: IndexStore + Sync + Send> IndexActor<S> {
    pub fn new(receiver: mpsc::Receiver<IndexMsg>, store: S) -> anyhow::Result<Self> {
        let options = IndexerOpts::default();
        let update_handler = UpdateHandler::new(&options)?;
        let update_handler = Arc::new(update_handler);
        let receiver = Some(receiver);
        Ok(Self {
            receiver,
            update_handler,
            store,
        })
    }

    /// `run` poll the write_receiver and read_receiver concurrently, but while messages send
    /// through the read channel are processed concurrently, the messages sent through the write
    /// channel are processed one at a time.
    pub async fn run(mut self) {
        let mut receiver = self
            .receiver
            .take()
            .expect("Index Actor must have a inbox at this point.");

        let stream = stream! {
            loop {
                match receiver.recv().await {
                    Some(msg) => yield msg,
                    None => break,
                }
            }
        };

        stream
            .for_each_concurrent(Some(CONCURRENT_INDEX_MSG), |msg| self.handle_message(msg))
            .await;
    }

    async fn handle_message(&self, msg: IndexMsg) {
        use IndexMsg::*;
        match msg {
            CreateIndex {
                uuid,
                primary_key,
                ret,
            } => {
                let _ = ret.send(self.handle_create_index(uuid, primary_key).await);
            }
            Update {
                ret,
                meta,
                data,
                uuid,
            } => {
                let _ = ret.send(self.handle_update(uuid, meta, data).await);
            }
            Search { ret, query, uuid } => {
                let _ = ret.send(self.handle_search(uuid, query).await);
            }
            Settings { ret, uuid } => {
                let _ = ret.send(self.handle_settings(uuid).await);
            }
            Documents {
                ret,
                uuid,
                attributes_to_retrieve,
                offset,
                limit,
            } => {
                let _ = ret.send(
                    self.handle_fetch_documents(uuid, offset, limit, attributes_to_retrieve)
                        .await,
                );
            }
            Document {
                uuid,
                attributes_to_retrieve,
                doc_id,
                ret,
            } => {
                let _ = ret.send(
                    self.handle_fetch_document(uuid, doc_id, attributes_to_retrieve)
                        .await,
                );
            }
            Delete { uuid, ret } => {
                let _ = ret.send(self.handle_delete(uuid).await);
            }
            GetMeta { uuid, ret } => {
                let _ = ret.send(self.handle_get_meta(uuid).await);
            }
            UpdateIndex {
                uuid,
                index_settings,
                ret,
            } => {
                let _ = ret.send(self.handle_update_index(uuid, index_settings).await);
            }
            Snapshot { uuid, path, ret } => {
                let _ = ret.send(self.handle_snapshot(uuid, path).await);
            }
            Dump { uuid, path, ret } => {
                let _ = ret.send(self.handle_dump(uuid, path).await);
            }
            GetStats { uuid, ret } => {
                let _ = ret.send(self.handle_get_stats(uuid).await);
            }
        }
    }

    async fn handle_search(&self, uuid: Uuid, query: SearchQuery) -> Result<SearchResult> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexActorError::UnexistingIndex)?;
        let result = spawn_blocking(move || index.perform_search(query)).await??;
        Ok(result)
    }

    async fn handle_create_index(
        &self,
        uuid: Uuid,
        primary_key: Option<String>,
    ) -> Result<IndexMeta> {
        let index = self.store.create(uuid, primary_key).await?;
        let meta = spawn_blocking(move || IndexMeta::new(&index)).await??;
        Ok(meta)
    }

    async fn handle_update(
        &self,
        uuid: Uuid,
        meta: Processing,
        data: Option<File>,
    ) -> Result<std::result::Result<Processed, Failed>> {
        debug!("Processing update {}", meta.id());
        let update_handler = self.update_handler.clone();
        let index = match self.store.get(uuid).await? {
            Some(index) => index,
            None => self.store.create(uuid, None).await?,
        };

        Ok(spawn_blocking(move || update_handler.handle_update(meta, data, index)).await?)
    }

    async fn handle_settings(&self, uuid: Uuid) -> Result<Settings<Checked>> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexActorError::UnexistingIndex)?;
        let result = spawn_blocking(move || index.settings()).await??;
        Ok(result)
    }

    async fn handle_fetch_documents(
        &self,
        uuid: Uuid,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Vec<Document>> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexActorError::UnexistingIndex)?;
        let result =
            spawn_blocking(move || index.retrieve_documents(offset, limit, attributes_to_retrieve))
                .await??;

        Ok(result)
    }

    async fn handle_fetch_document(
        &self,
        uuid: Uuid,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Document> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexActorError::UnexistingIndex)?;

        let result =
            spawn_blocking(move || index.retrieve_document(doc_id, attributes_to_retrieve))
                .await??;

        Ok(result)
    }

    async fn handle_delete(&self, uuid: Uuid) -> Result<()> {
        let index = self.store.delete(uuid).await?;

        if let Some(index) = index {
            tokio::task::spawn(async move {
                let index = index.0;
                let store = get_arc_ownership_blocking(index).await;
                spawn_blocking(move || {
                    store.prepare_for_closing().wait();
                    debug!("Index closed");
                });
            });
        }

        Ok(())
    }

    async fn handle_get_meta(&self, uuid: Uuid) -> Result<IndexMeta> {
        match self.store.get(uuid).await? {
            Some(index) => {
                let meta = spawn_blocking(move || IndexMeta::new(&index)).await??;
                Ok(meta)
            }
            None => Err(IndexActorError::UnexistingIndex),
        }
    }

    async fn handle_update_index(
        &self,
        uuid: Uuid,
        index_settings: IndexSettings,
    ) -> Result<IndexMeta> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexActorError::UnexistingIndex)?;

        let result = spawn_blocking(move || match index_settings.primary_key {
            Some(primary_key) => {
                let mut txn = index.write_txn()?;
                if index.primary_key(&txn)?.is_some() {
                    return Err(IndexActorError::ExistingPrimaryKey);
                }
                let mut builder = UpdateBuilder::new(0).settings(&mut txn, &index);
                builder.set_primary_key(primary_key);
                builder
                    .execute(|_, _| ())
                    .map_err(|e| IndexActorError::Internal(Box::new(e)))?;
                let meta = IndexMeta::new_txn(&index, &txn)?;
                txn.commit()?;
                Ok(meta)
            }
            None => {
                let meta = IndexMeta::new(&index)?;
                Ok(meta)
            }
        })
        .await??;

        Ok(result)
    }

    async fn handle_snapshot(&self, uuid: Uuid, mut path: PathBuf) -> Result<()> {
        use tokio::fs::create_dir_all;

        path.push("indexes");
        create_dir_all(&path).await?;

        if let Some(index) = self.store.get(uuid).await? {
            let mut index_path = path.join(format!("index-{}", uuid));

            create_dir_all(&index_path).await?;

            index_path.push("data.mdb");
            spawn_blocking(move || -> Result<()> {
                // Get write txn to wait for ongoing write transaction before snapshot.
                let _txn = index.write_txn()?;
                index
                    .env
                    .copy_to_path(index_path, CompactionOption::Enabled)?;
                Ok(())
            })
            .await??;
        }

        Ok(())
    }

    /// Create a `documents.jsonl` and a `settings.json` in `path/uid/` with a dump of all the
    /// documents and all the settings.
    async fn handle_dump(&self, uuid: Uuid, path: PathBuf) -> Result<()> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexActorError::UnexistingIndex)?;

        let path = path.join(format!("indexes/index-{}/", uuid));
        fs::create_dir_all(&path).await?;

        tokio::task::spawn_blocking(move || index.dump(path)).await??;

        Ok(())
    }

    async fn handle_get_stats(&self, uuid: Uuid) -> Result<IndexStats> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexActorError::UnexistingIndex)?;

        spawn_blocking(move || {
            let rtxn = index.read_txn()?;

            Ok(IndexStats {
                size: index.size(),
                number_of_documents: index
                    .number_of_documents(&rtxn)
                    .map_err(|e| IndexActorError::Internal(Box::new(e)))?,
                is_indexing: None,
                fields_distribution: index
                    .fields_distribution(&rtxn)
                    .map_err(|e| IndexActorError::Internal(e.into()))?,
            })
        })
        .await?
    }
}

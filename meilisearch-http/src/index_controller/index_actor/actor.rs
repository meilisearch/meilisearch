use std::fs::File;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use async_stream::stream;
use futures::pin_mut;
use futures::stream::StreamExt;
use heed::CompactionOption;
use log::debug;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use crate::index::{Document, SearchQuery, SearchResult, Settings};
use crate::index_controller::update_handler::UpdateHandler;
use crate::index_controller::{
    get_arc_ownership_blocking, updates::Processing, IndexStats, UpdateMeta,
};
use crate::option::IndexerOpts;

use super::{IndexError, IndexMeta, IndexMsg, IndexSettings, IndexStore, Result, UpdateResult};

pub struct IndexActor<S> {
    read_receiver: Option<mpsc::Receiver<IndexMsg>>,
    write_receiver: Option<mpsc::Receiver<IndexMsg>>,
    update_handler: Arc<UpdateHandler>,
    store: S,
}

impl<S: IndexStore + Sync + Send> IndexActor<S> {
    pub fn new(
        read_receiver: mpsc::Receiver<IndexMsg>,
        write_receiver: mpsc::Receiver<IndexMsg>,
        store: S,
    ) -> Result<Self> {
        let options = IndexerOpts::default();
        let update_handler = UpdateHandler::new(&options).map_err(IndexError::Error)?;
        let update_handler = Arc::new(update_handler);
        let read_receiver = Some(read_receiver);
        let write_receiver = Some(write_receiver);
        Ok(Self {
            read_receiver,
            write_receiver,
            store,
            update_handler,
        })
    }

    /// `run` poll the write_receiver and read_receiver concurrently, but while messages send
    /// through the read channel are processed concurrently, the messages sent through the write
    /// channel are processed one at a time.
    pub async fn run(mut self) {
        let mut read_receiver = self
            .read_receiver
            .take()
            .expect("Index Actor must have a inbox at this point.");

        let read_stream = stream! {
            loop {
                match read_receiver.recv().await {
                    Some(msg) => yield msg,
                    None => break,
                }
            }
        };

        let mut write_receiver = self
            .write_receiver
            .take()
            .expect("Index Actor must have a inbox at this point.");

        let write_stream = stream! {
            loop {
                match write_receiver.recv().await {
                    Some(msg) => yield msg,
                    None => break,
                }
            }
        };

        pin_mut!(write_stream);
        pin_mut!(read_stream);

        let fut1 = read_stream.for_each_concurrent(Some(10), |msg| self.handle_message(msg));
        let fut2 = write_stream.for_each_concurrent(Some(1), |msg| self.handle_message(msg));

        let fut1: Box<dyn Future<Output = ()> + Unpin + Send> = Box::new(fut1);
        let fut2: Box<dyn Future<Output = ()> + Unpin + Send> = Box::new(fut2);

        tokio::join!(fut1, fut2);
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
            Update { ret, meta, data } => {
                let _ = ret.send(self.handle_update(meta, data).await);
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
            GetStats { uuid, ret } => {
                let _ = ret.send(self.handle_get_stats(uuid).await);
            }
        }
    }

    async fn handle_search(&self, uuid: Uuid, query: SearchQuery) -> anyhow::Result<SearchResult> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexError::UnexistingIndex)?;
        spawn_blocking(move || index.perform_search(query)).await?
    }

    async fn handle_create_index(
        &self,
        uuid: Uuid,
        primary_key: Option<String>,
    ) -> Result<IndexMeta> {
        let index = self.store.create(uuid, primary_key).await?;
        let meta = spawn_blocking(move || IndexMeta::new(&index))
            .await
            .map_err(|e| IndexError::Error(e.into()))??;
        Ok(meta)
    }

    async fn handle_update(
        &self,
        meta: Processing<UpdateMeta>,
        data: File,
    ) -> Result<UpdateResult> {
        debug!("Processing update {}", meta.id());
        let uuid = meta.index_uuid();
        let update_handler = self.update_handler.clone();
        let index = match self.store.get(*uuid).await? {
            Some(index) => index,
            None => self.store.create(*uuid, None).await?,
        };
        spawn_blocking(move || update_handler.handle_update(meta, data, index))
            .await
            .map_err(|e| IndexError::Error(e.into()))
    }

    async fn handle_settings(&self, uuid: Uuid) -> Result<Settings> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexError::UnexistingIndex)?;
        spawn_blocking(move || index.settings().map_err(IndexError::Error))
            .await
            .map_err(|e| IndexError::Error(e.into()))?
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
            .ok_or(IndexError::UnexistingIndex)?;
        spawn_blocking(move || {
            index
                .retrieve_documents(offset, limit, attributes_to_retrieve)
                .map_err(IndexError::Error)
        })
        .await
        .map_err(|e| IndexError::Error(e.into()))?
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
            .ok_or(IndexError::UnexistingIndex)?;
        spawn_blocking(move || {
            index
                .retrieve_document(doc_id, attributes_to_retrieve)
                .map_err(IndexError::Error)
        })
        .await
        .map_err(|e| IndexError::Error(e.into()))?
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
                let meta = spawn_blocking(move || IndexMeta::new(&index))
                    .await
                    .map_err(|e| IndexError::Error(e.into()))??;
                Ok(meta)
            }
            None => Err(IndexError::UnexistingIndex),
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
            .ok_or(IndexError::UnexistingIndex)?;

        spawn_blocking(move || match index_settings.primary_key {
            Some(ref primary_key) => {
                let mut txn = index.write_txn()?;
                if index.primary_key(&txn)?.is_some() {
                    return Err(IndexError::ExistingPrimaryKey);
                }
                index.put_primary_key(&mut txn, primary_key)?;
                let meta = IndexMeta::new_txn(&index, &txn)?;
                txn.commit()?;
                Ok(meta)
            }
            None => {
                let meta = IndexMeta::new(&index)?;
                Ok(meta)
            }
        })
        .await
        .map_err(|e| IndexError::Error(e.into()))?
    }

    async fn handle_snapshot(&self, uuid: Uuid, mut path: PathBuf) -> Result<()> {
        use tokio::fs::create_dir_all;

        path.push("indexes");
        create_dir_all(&path)
            .await
            .map_err(|e| IndexError::Error(e.into()))?;

        if let Some(index) = self.store.get(uuid).await? {
            let mut index_path = path.join(format!("index-{}", uuid));
            create_dir_all(&index_path)
                .await
                .map_err(|e| IndexError::Error(e.into()))?;
            index_path.push("data.mdb");
            spawn_blocking(move || -> anyhow::Result<()> {
                // Get write txn to wait for ongoing write transaction before snapshot.
                let _txn = index.write_txn()?;
                index
                    .env
                    .copy_to_path(index_path, CompactionOption::Enabled)?;
                Ok(())
            })
            .await
            .map_err(|e| IndexError::Error(e.into()))?
            .map_err(IndexError::Error)?;
        }

        Ok(())
    }

    async fn handle_get_stats(&self, uuid: Uuid) -> Result<IndexStats> {
        let index = self
            .store
            .get(uuid)
            .await?
            .ok_or(IndexError::UnexistingIndex)?;

        spawn_blocking(move || {
            let rtxn = index.read_txn()?;

            Ok(IndexStats {
                size: index.size()?,
                number_of_documents: index.number_of_documents(&rtxn)?,
                is_indexing: false, // We set this field in src/index_controller/mod.rs get_stats
                fields_distribution: index.fields_distribution(&rtxn)?,
            })
        })
        .await
        .map_err(|e| IndexError::Error(e.into()))?
    }
}

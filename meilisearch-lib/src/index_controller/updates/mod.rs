pub mod error;
mod message;
pub mod status;
pub mod store;

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use async_stream::stream;
use futures::StreamExt;
use log::trace;
use milli::update::IndexDocumentsMethod;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use self::error::{Result, UpdateLoopError};
pub use self::message::UpdateMsg;
use self::store::{UpdateStore, UpdateStoreInfo};
use crate::document_formats::{read_csv, read_json, read_ndjson};
use crate::index::{Index, Settings, Unchecked};
use crate::index_controller::update_file_store::UpdateFileStore;
use status::UpdateStatus;

use super::index_resolver::index_store::IndexStore;
use super::index_resolver::uuid_store::UuidStore;
use super::index_resolver::IndexResolver;
use super::{DocumentAdditionFormat, Update};

pub type UpdateSender = mpsc::Sender<UpdateMsg>;

pub fn create_update_handler<U, I>(
    index_resolver: Arc<IndexResolver<U, I>>,
    db_path: impl AsRef<Path>,
    update_store_size: usize,
) -> anyhow::Result<UpdateSender>
where
    U: UuidStore + Sync + Send + 'static,
    I: IndexStore + Sync + Send + 'static,
{
    let path = db_path.as_ref().to_owned();
    let (sender, receiver) = mpsc::channel(100);
    let actor = UpdateLoop::new(update_store_size, receiver, path, index_resolver)?;

    tokio::task::spawn(actor.run());

    Ok(sender)
}

pub struct UpdateLoop {
    store: Arc<UpdateStore>,
    inbox: Option<mpsc::Receiver<UpdateMsg>>,
    update_file_store: UpdateFileStore,
    must_exit: Arc<AtomicBool>,
}

impl UpdateLoop {
    pub fn new<U, I>(
        update_db_size: usize,
        inbox: mpsc::Receiver<UpdateMsg>,
        path: impl AsRef<Path>,
        index_resolver: Arc<IndexResolver<U, I>>,
    ) -> anyhow::Result<Self>
    where
        U: UuidStore + Sync + Send + 'static,
        I: IndexStore + Sync + Send + 'static,
    {
        let path = path.as_ref().to_owned();
        std::fs::create_dir_all(&path)?;

        let mut options = heed::EnvOpenOptions::new();
        options.map_size(update_db_size);

        let must_exit = Arc::new(AtomicBool::new(false));

        let update_file_store = UpdateFileStore::new(&path).unwrap();
        let store = UpdateStore::open(
            options,
            &path,
            index_resolver,
            must_exit.clone(),
            update_file_store.clone(),
        )?;

        let inbox = Some(inbox);

        Ok(Self {
            store,
            inbox,
            must_exit,
            update_file_store,
        })
    }

    pub async fn run(mut self) {
        use UpdateMsg::*;

        trace!("Started update actor.");

        let mut inbox = self
            .inbox
            .take()
            .expect("A receiver should be present by now.");

        let must_exit = self.must_exit.clone();
        let stream = stream! {
            loop {
                let msg = inbox.recv().await;

                if must_exit.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                match msg {
                    Some(msg) => yield msg,
                    None => break,
                }
            }
        };

        stream
            .for_each_concurrent(Some(10), |msg| async {
                match msg {
                    Update { uuid, update, ret } => {
                        let _ = ret.send(self.handle_update(uuid, update).await);
                    }
                    ListUpdates { uuid, ret } => {
                        let _ = ret.send(self.handle_list_updates(uuid).await);
                    }
                    GetUpdate { uuid, ret, id } => {
                        let _ = ret.send(self.handle_get_update(uuid, id).await);
                    }
                    DeleteIndex { uuid, ret } => {
                        let _ = ret.send(self.handle_delete(uuid).await);
                    }
                    Snapshot { indexes, path, ret } => {
                        let _ = ret.send(self.handle_snapshot(indexes, path).await);
                    }
                    GetInfo { ret } => {
                        let _ = ret.send(self.handle_get_info().await);
                    }
                    Dump { indexes, path, ret } => {
                        let _ = ret.send(self.handle_dump(indexes, path).await);
                    }
                }
            })
            .await;
    }

    async fn handle_update(&self, index_uuid: Uuid, update: Update) -> Result<UpdateStatus> {
        let registration = match update {
            Update::DocumentAddition {
                mut payload,
                primary_key,
                method,
                format,
            } => {
                let mut buffer = Vec::new();
                while let Some(bytes) = payload.next().await {
                    match bytes {
                        Ok(bytes) => {
                            buffer.extend_from_slice(&bytes);
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                let (content_uuid, mut update_file) = self.update_file_store.new_update()?;
                tokio::task::spawn_blocking(move || -> Result<_> {
                    // check if the payload is empty, and return an error
                    if buffer.is_empty() {
                        return Err(UpdateLoopError::MissingPayload(format));
                    }

                    let reader = Cursor::new(buffer);
                    match format {
                        DocumentAdditionFormat::Json => read_json(reader, &mut *update_file)?,
                        DocumentAdditionFormat::Csv => read_csv(reader, &mut *update_file)?,
                        DocumentAdditionFormat::Ndjson => read_ndjson(reader, &mut *update_file)?,
                    }

                    update_file.persist()?;

                    Ok(())
                })
                .await??;

                store::Update::DocumentAddition {
                    primary_key,
                    method,
                    content_uuid,
                }
            }
            Update::Settings(settings) => store::Update::Settings(settings),
            Update::ClearDocuments => store::Update::ClearDocuments,
            Update::DeleteDocuments(ids) => store::Update::DeleteDocuments(ids),
        };

        let store = self.store.clone();
        let status =
            tokio::task::spawn_blocking(move || store.register_update(index_uuid, registration))
                .await??;

        Ok(status.into())
    }

    async fn handle_list_updates(&self, uuid: Uuid) -> Result<Vec<UpdateStatus>> {
        let update_store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let result = update_store.list(uuid)?;
            Ok(result)
        })
        .await?
    }

    async fn handle_get_update(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let result = store
                .meta(uuid, id)?
                .ok_or(UpdateLoopError::UnexistingUpdate(id))?;
            Ok(result)
        })
        .await?
    }

    async fn handle_delete(&self, uuid: Uuid) -> Result<()> {
        let store = self.store.clone();

        tokio::task::spawn_blocking(move || store.delete_all(uuid)).await??;

        Ok(())
    }

    async fn handle_snapshot(&self, indexes: Vec<Index>, path: PathBuf) -> Result<()> {
        let update_store = self.store.clone();

        tokio::task::spawn_blocking(move || update_store.snapshot(indexes, path)).await??;

        Ok(())
    }

    async fn handle_dump(&self, indexes: Vec<Index>, path: PathBuf) -> Result<()> {
        let update_store = self.store.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            update_store.dump(&indexes, path.to_path_buf())?;
            Ok(())
        })
        .await??;

        Ok(())
    }

    async fn handle_get_info(&self) -> Result<UpdateStoreInfo> {
        let update_store = self.store.clone();
        let info = tokio::task::spawn_blocking(move || -> Result<UpdateStoreInfo> {
            let info = update_store.get_info()?;
            Ok(info)
        })
        .await??;

        Ok(info)
    }
}

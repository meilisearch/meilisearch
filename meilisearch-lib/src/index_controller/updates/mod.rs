pub mod error;
mod message;
pub mod status;
pub mod store;

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use actix_web::error::PayloadError;
use async_stream::stream;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use log::trace;
use milli::documents::DocumentBatchBuilder;
use milli::update::IndexDocumentsMethod;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use self::error::{Result, UpdateLoopError};
pub use self::message::UpdateMsg;
use self::store::{UpdateStore, UpdateStoreInfo};
use crate::index::{Settings, Unchecked};
use crate::index_controller::update_file_store::UpdateFileStore;
use status::UpdateStatus;

use super::index_resolver::HardStateIndexResolver;
use super::{DocumentAdditionFormat, Payload, Update};

pub type UpdateSender = mpsc::Sender<UpdateMsg>;

pub fn create_update_handler(
    index_resolver: Arc<HardStateIndexResolver>,
    db_path: impl AsRef<Path>,
    update_store_size: usize,
) -> anyhow::Result<UpdateSender> {
    let path = db_path.as_ref().to_owned();
    let (sender, receiver) = mpsc::channel(100);
    let actor = UpdateLoop::new(update_store_size, receiver, path, index_resolver)?;

    tokio::task::spawn_local(actor.run());

    Ok(sender)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegisterUpdate {
    DeleteDocuments(Vec<String>),
    DocumentAddition {
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_uuid: Uuid,
    },
    Settings(Settings<Unchecked>),
    ClearDocuments,
}

/// A wrapper type to implement read on a `Stream<Result<Bytes, Error>>`.
struct StreamReader<S> {
    stream: S,
    current: Option<Bytes>,
}

impl<S> StreamReader<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            current: None,
        }
    }
}

impl<S: Stream<Item = std::result::Result<Bytes, PayloadError>> + Unpin> io::Read
    for StreamReader<S>
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // TODO: optimize buf filling
        match self.current.take() {
            Some(mut bytes) => {
                let copied = bytes.split_to(buf.len());
                buf.copy_from_slice(&copied);
                if !bytes.is_empty() {
                    self.current.replace(bytes);
                }
                Ok(copied.len())
            }
            None => match tokio::runtime::Handle::current().block_on(self.stream.next()) {
                Some(Ok(bytes)) => {
                    self.current.replace(bytes);
                    self.read(buf)
                }
                Some(Err(e)) => Err(io::Error::new(io::ErrorKind::BrokenPipe, e)),
                None => return Ok(0),
            },
        }
    }
}

pub struct UpdateLoop {
    store: Arc<UpdateStore>,
    inbox: Option<mpsc::Receiver<UpdateMsg>>,
    update_file_store: UpdateFileStore,
    index_resolver: Arc<HardStateIndexResolver>,
    must_exit: Arc<AtomicBool>,
}

impl UpdateLoop {
    pub fn new(
        update_db_size: usize,
        inbox: mpsc::Receiver<UpdateMsg>,
        path: impl AsRef<Path>,
        index_resolver: Arc<HardStateIndexResolver>,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_owned();
        std::fs::create_dir_all(&path)?;

        let mut options = heed::EnvOpenOptions::new();
        options.map_size(update_db_size);

        let must_exit = Arc::new(AtomicBool::new(false));

        let store = UpdateStore::open(options, &path, index_resolver.clone(), must_exit.clone())?;

        let inbox = Some(inbox);

        let update_file_store = UpdateFileStore::new(&path).unwrap();

        Ok(Self {
            store,
            inbox,
            must_exit,
            update_file_store,
            index_resolver,
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
                    Delete { uuid, ret } => {
                        let _ = ret.send(self.handle_delete(uuid).await);
                    }
                    Snapshot { uuids, path, ret } => {
                        let _ = ret.send(self.handle_snapshot(uuids, path).await);
                    }
                    GetInfo { ret } => {
                        let _ = ret.send(self.handle_get_info().await);
                    }
                    Dump { uuids, path, ret } => {
                        let _ = ret.send(self.handle_dump(uuids, path).await);
                    }
                }
            })
            .await;
    }

    async fn handle_update(&self, index_uuid: Uuid, update: Update) -> Result<UpdateStatus> {
        let registration = match update {
            Update::DocumentAddition {
                payload,
                primary_key,
                method,
                format,
            } => {
                let content_uuid = match format {
                    DocumentAdditionFormat::Json => self.documents_from_json(payload).await?,
                };

                RegisterUpdate::DocumentAddition {
                    primary_key,
                    method,
                    content_uuid,
                }
            }
            Update::Settings(settings) => RegisterUpdate::Settings(settings),
            Update::ClearDocuments => RegisterUpdate::ClearDocuments,
            Update::DeleteDocuments(ids) => RegisterUpdate::DeleteDocuments(ids),
        };

        let store = self.store.clone();
        let status =
            tokio::task::spawn_blocking(move || store.register_update(index_uuid, registration))
                .await??;

        Ok(status.into())
    }

    async fn documents_from_json(&self, payload: Payload) -> Result<Uuid> {
        let file_store = self.update_file_store.clone();
        tokio::task::spawn_blocking(move || {
            let (uuid, mut file) = file_store.new_update().unwrap();
            let mut builder = DocumentBatchBuilder::new(&mut *file).unwrap();

            let documents: Vec<Map<String, Value>> =
                serde_json::from_reader(StreamReader::new(payload))?;
            builder.add_documents(documents).unwrap();
            builder.finish().unwrap();

            file.persist();

            Ok(uuid)
        })
        .await?
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

    async fn handle_snapshot(&self, _uuids: HashSet<Uuid>,_pathh: PathBuf) -> Result<()> {
        todo!()
        //let index_handle = self.index_resolver.clone();
        //let update_store = self.store.clone();

        //tokio::task::spawn_blocking(move || update_store.snapshot(&uuids, &path, index_handle))
            //.await??;

        //Ok(())
    }

    async fn handle_dump(&self, uuids: HashSet<Uuid>, path: PathBuf) -> Result<()> {
        let index_handle = self.index_resolver.clone();
        let update_store = self.store.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            update_store.dump(&uuids, path.to_path_buf(), index_handle)?;
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

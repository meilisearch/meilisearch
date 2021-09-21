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
use serde_json::{Map, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::error::{Result, UpdateActorError};
use super::RegisterUpdate;
use super::{UpdateMsg, UpdateStore, UpdateStoreInfo, Update};
use crate::index_controller::index_actor::IndexActorHandle;
use crate::index_controller::update_file_store::UpdateFileStore;
use crate::index_controller::{DocumentAdditionFormat, Payload, UpdateStatus};

pub struct UpdateActor<I> {
    store: Arc<UpdateStore>,
    inbox: Option<mpsc::Receiver<UpdateMsg>>,
    update_file_store: UpdateFileStore,
    index_handle: I,
    must_exit: Arc<AtomicBool>,
}

struct StreamReader<S> {
    stream: S,
    current: Option<Bytes>,
}

impl<S> StreamReader<S> {
    fn new(stream: S) -> Self {
        Self { stream, current: None }
    }

}

impl<S: Stream<Item = std::result::Result<Bytes, PayloadError>> + Unpin> io::Read for StreamReader<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.current.take() {
            Some(mut bytes) => {
                let copied = bytes.split_to(buf.len());
                buf.copy_from_slice(&copied);
                if !bytes.is_empty() {
                    self.current.replace(bytes);
                }
                Ok(copied.len())
            }
            None => {
                match tokio::runtime::Handle::current().block_on(self.stream.next()) {
                    Some(Ok(bytes)) => {
                        self.current.replace(bytes);
                        self.read(buf)
                    },
                    Some(Err(e)) => Err(io::Error::new(io::ErrorKind::BrokenPipe, e)),
                    None => return Ok(0),
                }
            }
        }
    }
}

impl<I> UpdateActor<I>
where
    I: IndexActorHandle + Clone + Sync + Send + 'static,
{
    pub fn new(
        update_db_size: usize,
        inbox: mpsc::Receiver<UpdateMsg>,
        path: impl AsRef<Path>,
        index_handle: I,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_owned();
        std::fs::create_dir_all(&path)?;

        let mut options = heed::EnvOpenOptions::new();
        options.map_size(update_db_size);

        let must_exit = Arc::new(AtomicBool::new(false));

        let store = UpdateStore::open(options, &path, index_handle.clone(), must_exit.clone())?;

        let inbox = Some(inbox);

        let update_file_store =  UpdateFileStore::new(&path).unwrap();

        Ok(Self {
            store,
            inbox,
            index_handle,
            must_exit,
            update_file_store
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
                    Update {
                        uuid,
                        update,
                        ret,
                    } => {
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

    async fn handle_update(
        &self,
        index_uuid: Uuid,
        update: Update,
    ) -> Result<UpdateStatus> {
        let registration = match update {
            Update::DocumentAddition { payload, primary_key, method, format } => {
                let content_uuid = match format {
                    DocumentAdditionFormat::Json => self.documents_from_json(payload).await?,
                };

                RegisterUpdate::DocumentAddition { primary_key, method, content_uuid }
            }
        };

        let store = self.store.clone();
        let status = tokio::task::spawn_blocking(move || store.register_update(index_uuid, registration)).await??;

        Ok(status.into())
    }

    async fn documents_from_json(&self, payload: Payload) -> Result<Uuid> {
        let file_store = self.update_file_store.clone();
        tokio::task::spawn_blocking(move || {
            let (uuid, mut file) = file_store.new_update().unwrap();
            let mut builder = DocumentBatchBuilder::new(&mut *file).unwrap();

            let documents: Vec<Map<String, Value>> = serde_json::from_reader(StreamReader::new(payload))?;
            builder.add_documents(documents).unwrap();
            builder.finish().unwrap();

            file.persist();

            Ok(uuid)
        }).await?
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
                .ok_or(UpdateActorError::UnexistingUpdate(id))?;
            Ok(result)
        })
        .await?
    }

    async fn handle_delete(&self, uuid: Uuid) -> Result<()> {
        let store = self.store.clone();

        tokio::task::spawn_blocking(move || store.delete_all(uuid)).await??;

        Ok(())
    }

    async fn handle_snapshot(&self, uuids: HashSet<Uuid>, path: PathBuf) -> Result<()> {
        let index_handle = self.index_handle.clone();
        let update_store = self.store.clone();

        tokio::task::spawn_blocking(move || update_store.snapshot(&uuids, &path, index_handle))
            .await??;

        Ok(())
    }

    async fn handle_dump(&self, uuids: HashSet<Uuid>, path: PathBuf) -> Result<()> {
        let index_handle = self.index_handle.clone();
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

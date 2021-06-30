use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use async_stream::stream;
use futures::StreamExt;
use log::trace;
use oxidized_json_checker::JsonChecker;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::error::{Result, UpdateActorError};
use super::{PayloadData, UpdateMsg, UpdateStore, UpdateStoreInfo};
use crate::index_controller::index_actor::IndexActorHandle;
use crate::index_controller::{UpdateMeta, UpdateStatus};

pub struct UpdateActor<D, I> {
    path: PathBuf,
    store: Arc<UpdateStore>,
    inbox: Option<mpsc::Receiver<UpdateMsg<D>>>,
    index_handle: I,
    must_exit: Arc<AtomicBool>,
}

impl<D, I> UpdateActor<D, I>
where
    D: AsRef<[u8]> + Sized + 'static,
    I: IndexActorHandle + Clone + Send + Sync + 'static,
{
    pub fn new(
        update_db_size: usize,
        inbox: mpsc::Receiver<UpdateMsg<D>>,
        path: impl AsRef<Path>,
        index_handle: I,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().join("updates");

        std::fs::create_dir_all(&path)?;

        let mut options = heed::EnvOpenOptions::new();
        options.map_size(update_db_size);

        let must_exit = Arc::new(AtomicBool::new(false));

        let store = UpdateStore::open(options, &path, index_handle.clone(), must_exit.clone())?;
        std::fs::create_dir_all(path.join("update_files"))?;
        let inbox = Some(inbox);
        Ok(Self {
            path,
            store,
            inbox,
            index_handle,
            must_exit,
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
                        meta,
                        data,
                        ret,
                    } => {
                        let _ = ret.send(self.handle_update(uuid, meta, data).await);
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
        uuid: Uuid,
        meta: UpdateMeta,
        payload: mpsc::Receiver<PayloadData<D>>,
    ) -> Result<UpdateStatus> {
        let file_path = match meta {
            UpdateMeta::DocumentsAddition { .. } => {
                let update_file_id = uuid::Uuid::new_v4();
                let path = self
                    .path
                    .join(format!("update_files/update_{}", update_file_id));
                let mut file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&path)
                    .await?;

                async fn write_to_file<D>(
                    file: &mut fs::File,
                    mut payload: mpsc::Receiver<PayloadData<D>>,
                ) -> Result<usize>
                where
                    D: AsRef<[u8]> + Sized + 'static,
                {
                    let mut file_len = 0;

                    while let Some(bytes) = payload.recv().await {
                        let bytes = bytes?;
                        file_len += bytes.as_ref().len();
                        file.write_all(bytes.as_ref()).await?;
                    }

                    file.flush().await?;

                    Ok(file_len)
                }

                let file_len = write_to_file(&mut file, payload).await;

                match file_len {
                    Ok(len) if len > 0 => {
                        let file = file.into_std().await;
                        Some((file, update_file_id))
                    }
                    Err(e) => {
                        fs::remove_file(&path).await?;
                        return Err(e);
                    }
                    _ => {
                        fs::remove_file(&path).await?;
                        None
                    }
                }
            }
            _ => None,
        };

        let update_store = self.store.clone();

        tokio::task::spawn_blocking(move || {
            use std::io::{copy, sink, BufReader, Seek};

            // If the payload is empty, ignore the check.
            let update_uuid = if let Some((mut file, uuid)) = file_path {
                // set the file back to the beginning
                file.seek(SeekFrom::Start(0))?;
                // Check that the json payload is valid:
                let reader = BufReader::new(&mut file);
                let mut checker = JsonChecker::new(reader);

                if copy(&mut checker, &mut sink()).is_err() || checker.finish().is_err() {
                    // The json file is invalid, we use Serde to get a nice error message:
                    file.seek(SeekFrom::Start(0))?;
                    let _: serde_json::Value = serde_json::from_reader(file)
                        .map_err(|e| UpdateActorError::InvalidPayload(Box::new(e)))?;
                }
                Some(uuid)
            } else {
                None
            };

            // The payload is valid, we can register it to the update store.
            let status = update_store
                .register_update(meta, update_uuid, uuid)
                .map(UpdateStatus::Enqueued)?;
            Ok(status)
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

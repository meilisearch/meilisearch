use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::info;
use oxidized_json_checker::JsonChecker;
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use uuid::Uuid;
use futures::StreamExt;

use super::{PayloadData, Result, UpdateError, UpdateMsg, UpdateStore};
use crate::index_controller::index_actor::{IndexActorHandle, CONCURRENT_INDEX_MSG};
use crate::index_controller::{UpdateMeta, UpdateStatus};

pub struct UpdateActor<D, I> {
    path: PathBuf,
    store: Arc<UpdateStore>,
    inbox: mpsc::Receiver<UpdateMsg<D>>,
    index_handle: I,
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
        let path = path.as_ref().to_owned().join("updates");

        std::fs::create_dir_all(&path)?;

        let mut options = heed::EnvOpenOptions::new();
        options.map_size(update_db_size);

        let handle = index_handle.clone();
        let store = UpdateStore::open(options, &path, move |uuid, meta, file| {
            futures::executor::block_on(handle.update(uuid, meta, file))
        })
        .map_err(|e| UpdateError::Error(e.into()))?;
        std::fs::create_dir_all(path.join("update_files"))?;
        assert!(path.exists());
        Ok(Self {
            store,
            inbox,
            path,
            index_handle,
        })
    }

    pub async fn run(mut self) {
        use UpdateMsg::*;

        info!("Started update actor.");

        loop {
            match self.inbox.recv().await {
                Some(Update {
                    uuid,
                    meta,
                    data,
                    ret,
                }) => {
                    let _ = ret.send(self.handle_update(uuid, meta, data).await);
                }
                Some(ListUpdates { uuid, ret }) => {
                    let _ = ret.send(self.handle_list_updates(uuid).await);
                }
                Some(GetUpdate { uuid, ret, id }) => {
                    let _ = ret.send(self.handle_get_update(uuid, id).await);
                }
                Some(Delete { uuid, ret }) => {
                    let _ = ret.send(self.handle_delete(uuid).await);
                }
                Some(Snapshot { uuids, path, ret }) => {
                    let _ = ret.send(self.handle_snapshot(uuids, path).await);
                }
                Some(GetSize { ret }) => {
                    let _ = ret.send(self.handle_get_size().await);
                }
                None => break,
            }
        }
    }

    async fn handle_update(
        &self,
        uuid: Uuid,
        meta: UpdateMeta,
        mut payload: mpsc::Receiver<PayloadData<D>>,
    ) -> Result<UpdateStatus> {
        let update_file_id = uuid::Uuid::new_v4();
        let path = self
            .path
            .join(format!("update_files/update_{}", update_file_id));
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .map_err(|e| UpdateError::Error(Box::new(e)))?;

        while let Some(bytes) = payload.recv().await {
            match bytes {
                Ok(bytes) => {
                    file.write_all(bytes.as_ref())
                        .await
                        .map_err(|e| UpdateError::Error(Box::new(e)))?;
                }
                Err(e) => {
                    return Err(UpdateError::Error(e));
                }
            }
        }

        file.flush()
            .await
            .map_err(|e| UpdateError::Error(Box::new(e)))?;

        file.seek(SeekFrom::Start(0))
            .await
            .map_err(|e| UpdateError::Error(Box::new(e)))?;

        let mut file = file.into_std().await;

        let update_store = self.store.clone();

        tokio::task::spawn_blocking(move || {
            use std::io::{copy, sink, BufReader, Seek};

            // If the payload is empty, ignore the check.
            if file
                .metadata()
                .map_err(|e| UpdateError::Error(Box::new(e)))?
                .len()
                > 0
            {
                // Check that the json payload is valid:
                let reader = BufReader::new(&mut file);
                let mut checker = JsonChecker::new(reader);

                if copy(&mut checker, &mut sink()).is_err() || checker.finish().is_err() {
                    // The json file is invalid, we use Serde to get a nice error message:
                    file.seek(SeekFrom::Start(0))
                        .map_err(|e| UpdateError::Error(Box::new(e)))?;
                    let _: serde_json::Value = serde_json::from_reader(file)
                        .map_err(|e| UpdateError::Error(Box::new(e)))?;
                }
            }

            // The payload is valid, we can register it to the update store.
            update_store
                .register_update(meta, path, uuid)
                .map(UpdateStatus::Enqueued)
                .map_err(|e| UpdateError::Error(Box::new(e)))
        })
        .await
        .map_err(|e| UpdateError::Error(Box::new(e)))?
    }

    async fn handle_list_updates(&self, uuid: Uuid) -> Result<Vec<UpdateStatus>> {
        let update_store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let result = update_store
                .list(uuid)
                .map_err(|e| UpdateError::Error(e.into()))?;
            Ok(result)
        })
        .await
        .map_err(|e| UpdateError::Error(Box::new(e)))?
    }

    async fn handle_get_update(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus> {
        let store = self.store.clone();
        let result = store
            .meta(uuid, id)
            .map_err(|e| UpdateError::Error(Box::new(e)))?
            .ok_or(UpdateError::UnexistingUpdate(id))?;
        Ok(result)
    }

    async fn handle_delete(&self, uuid: Uuid) -> Result<()> {
        let store = self.store.clone();

        tokio::task::spawn_blocking(move || store.delete_all(uuid))
            .await
            .map_err(|e| UpdateError::Error(e.into()))?
            .map_err(|e| UpdateError::Error(e.into()))?;

        Ok(())
    }

    async fn handle_snapshot(&self, uuids: Vec<Uuid>, path: PathBuf) -> Result<()> {
        let index_handle = self.index_handle.clone();
        let update_store = self.store.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // acquire write lock to prevent further writes during snapshot
            // the update lock must be acquired BEFORE the write lock to prevent dead lock
            let _lock = update_store.update_lock.lock();
            let mut txn = update_store.env.write_txn()?;

            // create db snapshot
            update_store.snapshot(&mut txn, &path)?;

            // Perform the snapshot of each index concurently. Only a third of the capabilities of
            // the index actor at a time not to put too much pressure on the index actor
            let path = &path;
            let handle = &index_handle;

            let mut stream = futures::stream::iter(uuids.iter())
                .map(|&uuid| handle.snapshot(uuid, path.clone()))
                .buffer_unordered(CONCURRENT_INDEX_MSG / 3);

            futures::executor::block_on(async {
                while let Some(res) = stream.next().await {
                    res?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| UpdateError::Error(e.into()))?
        .map_err(|e| UpdateError::Error(e.into()))?;

        Ok(())
    }

    async fn handle_get_size(&self) -> Result<u64> {
        let update_store = self.store.clone();
        let size = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
            let txn = update_store.env.read_txn()?;

            update_store.get_size(&txn)
        })
        .await
        .map_err(|e| UpdateError::Error(e.into()))?
        .map_err(|e| UpdateError::Error(e.into()))?;

        Ok(size)
    }
}

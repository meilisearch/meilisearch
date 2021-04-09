use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use log::info;
use oxidized_json_checker::JsonChecker;
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::index_controller::index_actor::IndexActorHandle;
use crate::index_controller::{get_arc_ownership_blocking, UpdateMeta, UpdateStatus};

use super::{PayloadData, Result, UpdateError, UpdateMsg, UpdateStoreStore};

pub struct UpdateActor<D, S, I> {
    path: PathBuf,
    store: S,
    inbox: mpsc::Receiver<UpdateMsg<D>>,
    index_handle: I,
}

impl<D, S, I> UpdateActor<D, S, I>
where
    D: AsRef<[u8]> + Sized + 'static,
    S: UpdateStoreStore,
    I: IndexActorHandle + Clone + Send + Sync + 'static,
{
    pub fn new(
        store: S,
        inbox: mpsc::Receiver<UpdateMsg<D>>,
        path: impl AsRef<Path>,
        index_handle: I,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_owned();
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
                Some(Create { uuid, ret }) => {
                    let _ = ret.send(self.handle_create(uuid).await);
                }
                Some(Snapshot { uuid, path, ret }) => {
                    let _ = ret.send(self.handle_snapshot(uuid, path).await);
                }
                Some(GetSize { uuid, ret }) => {
                    let _ = ret.send(self.handle_get_size(uuid).await);
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
        let update_store = self.store.get_or_create(uuid).await?;
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
        let update_store = self.store.get(uuid).await?;
        tokio::task::spawn_blocking(move || {
            let result = update_store
                .ok_or(UpdateError::UnexistingIndex(uuid))?
                .list()
                .map_err(|e| UpdateError::Error(e.into()))?;
            Ok(result)
        })
        .await
        .map_err(|e| UpdateError::Error(Box::new(e)))?
    }

    async fn handle_get_update(&self, uuid: Uuid, id: u64) -> Result<UpdateStatus> {
        let store = self
            .store
            .get(uuid)
            .await?
            .ok_or(UpdateError::UnexistingIndex(uuid))?;
        let result = store
            .meta(id)
            .map_err(|e| UpdateError::Error(Box::new(e)))?
            .ok_or(UpdateError::UnexistingUpdate(id))?;
        Ok(result)
    }

    async fn handle_delete(&self, uuid: Uuid) -> Result<()> {
        let store = self.store.delete(uuid).await?;

        if let Some(store) = store {
            tokio::task::spawn(async move {
                let store = get_arc_ownership_blocking(store).await;
                tokio::task::spawn_blocking(move || {
                    store.prepare_for_closing().wait();
                    info!("Update store {} was closed.", uuid);
                });
            });
        }

        Ok(())
    }

    async fn handle_create(&self, uuid: Uuid) -> Result<()> {
        let _ = self.store.get_or_create(uuid).await?;
        Ok(())
    }

    async fn handle_snapshot(&self, uuid: Uuid, path: PathBuf) -> Result<()> {
        let index_handle = self.index_handle.clone();
        if let Some(update_store) = self.store.get(uuid).await? {
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                // acquire write lock to prevent further writes during snapshot
                // the update lock must be acquired BEFORE the write lock to prevent dead lock
                let _lock = update_store.update_lock.lock();
                let mut txn = update_store.env.write_txn()?;

                // create db snapshot
                update_store.snapshot(&mut txn, &path, uuid)?;

                futures::executor::block_on(
                    async move { index_handle.snapshot(uuid, path).await },
                )?;
                Ok(())
            })
            .await
            .map_err(|e| UpdateError::Error(e.into()))?
            .map_err(|e| UpdateError::Error(e.into()))?;
        }

        Ok(())
    }

    async fn handle_get_size(&self, uuid: Uuid) -> Result<u64> {
        let size = match self.store.get(uuid).await? {
            Some(update_store) => tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
                let txn = update_store.env.read_txn()?;

                update_store.get_size(&txn)
            })
            .await
            .map_err(|e| UpdateError::Error(e.into()))?
            .map_err(|e| UpdateError::Error(e.into()))?,
            None => 0,
        };

        Ok(size)
    }
}

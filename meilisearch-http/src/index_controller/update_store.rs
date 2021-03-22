use std::fs::{remove_file, create_dir_all, copy};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use heed::types::{DecodeIgnore, OwnedType, SerdeJson};
use heed::{Database, Env, EnvOpenOptions, CompactionOption};
use parking_lot::{RwLock, Mutex};
use serde::{Deserialize, Serialize};
use std::fs::File;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::index_controller::updates::*;

type BEU64 = heed::zerocopy::U64<heed::byteorder::BE>;

#[derive(Clone)]
pub struct UpdateStore<M, N, E> {
    pub env: Env,
    pending_meta: Database<OwnedType<BEU64>, SerdeJson<Pending<M>>>,
    pending: Database<OwnedType<BEU64>, SerdeJson<PathBuf>>,
    processed_meta: Database<OwnedType<BEU64>, SerdeJson<Processed<M, N>>>,
    failed_meta: Database<OwnedType<BEU64>, SerdeJson<Failed<M, E>>>,
    aborted_meta: Database<OwnedType<BEU64>, SerdeJson<Aborted<M>>>,
    processing: Arc<RwLock<Option<Processing<M>>>>,
    notification_sender: mpsc::Sender<()>,
    /// A lock on the update loop. This is meant to prevent a snapshot to occur while an update is
    /// processing, while not preventing writes all together during an update
    pub update_lock: Arc<Mutex<()>>,
}

pub trait HandleUpdate<M, N, E> {
    fn handle_update(
        &mut self,
        meta: Processing<M>,
        content: File,
    ) -> anyhow::Result<Result<Processed<M, N>, Failed<M, E>>>;
}

impl<M, N, E, F> HandleUpdate<M, N, E> for F
where
    F: FnMut(Processing<M>, File) -> anyhow::Result<Result<Processed<M, N>, Failed<M, E>>>,
{
    fn handle_update(
        &mut self,
        meta: Processing<M>,
        content: File,
    ) -> anyhow::Result<Result<Processed<M, N>, Failed<M, E>>> {
        self(meta, content)
    }
}

impl<M, N, E> UpdateStore<M, N, E>
where
    M: for<'a> Deserialize<'a> + Serialize + 'static + Send + Sync + Clone,
    N: for<'a> Deserialize<'a> + Serialize + 'static + Send + Sync,
    E: for<'a> Deserialize<'a> + Serialize + 'static + Send + Sync,
{
    pub fn open<P, U>(
        mut options: EnvOpenOptions,
        path: P,
        update_handler: U,
    ) -> heed::Result<Arc<Self>>
    where
        P: AsRef<Path>,
        U: HandleUpdate<M, N, E> + Sync + Clone + Send + 'static,
    {
        options.max_dbs(5);

        let env = options.open(path)?;
        let pending_meta = env.create_database(Some("pending-meta"))?;
        let pending = env.create_database(Some("pending"))?;
        let processed_meta = env.create_database(Some("processed-meta"))?;
        let aborted_meta = env.create_database(Some("aborted-meta"))?;
        let failed_meta = env.create_database(Some("failed-meta"))?;
        let processing = Arc::new(RwLock::new(None));

        let (notification_sender, mut notification_receiver) = mpsc::channel(10);
        // Send a first notification to trigger the process.
        let _ = notification_sender.send(());

        let update_lock = Arc::new(Mutex::new(()));

        let update_store = Arc::new(UpdateStore {
            env,
            pending,
            pending_meta,
            processed_meta,
            aborted_meta,
            notification_sender,
            failed_meta,
            processing,
            update_lock,
        });

        // We need a weak reference so we can take ownership on the arc later when we
        // want to close the index.
        let update_store_weak = Arc::downgrade(&update_store);
        tokio::task::spawn(async move {
            // Block and wait for something to process.
            'outer: while notification_receiver.recv().await.is_some() {
                loop {
                    match update_store_weak.upgrade() {
                        Some(update_store) => {
                            let handler = update_handler.clone();
                            let res = tokio::task::spawn_blocking(move || {
                                update_store.process_pending_update(handler)
                            })
                            .await
                            .expect("Fatal error processing update.");
                            match res {
                                Ok(Some(_)) => (),
                                Ok(None) => break,
                                Err(e) => eprintln!("error while processing update: {}", e),
                            }
                        }
                        // the ownership on the arc has been taken, we need to exit.
                        None => break 'outer,
                    }
                }
            }
        });

        Ok(update_store)
    }

    pub fn prepare_for_closing(self) -> heed::EnvClosingEvent {
        self.env.prepare_for_closing()
    }

    /// Returns the new biggest id to use to store the new update.
    fn new_update_id(&self, txn: &heed::RoTxn) -> heed::Result<u64> {
        let last_pending = self
            .pending_meta
            .remap_data_type::<DecodeIgnore>()
            .last(txn)?
            .map(|(k, _)| k.get());

        let last_processed = self
            .processed_meta
            .remap_data_type::<DecodeIgnore>()
            .last(txn)?
            .map(|(k, _)| k.get());

        let last_aborted = self
            .aborted_meta
            .remap_data_type::<DecodeIgnore>()
            .last(txn)?
            .map(|(k, _)| k.get());

        let last_update_id = [last_pending, last_processed, last_aborted]
            .iter()
            .copied()
            .flatten()
            .max();

        match last_update_id {
            Some(last_id) => Ok(last_id + 1),
            None => Ok(0),
        }
    }

    /// Registers the update content in the pending store and the meta
    /// into the pending-meta store. Returns the new unique update id.
    pub fn register_update(
        &self,
        meta: M,
        content: impl AsRef<Path>,
        index_uuid: Uuid,
    ) -> heed::Result<Pending<M>> {
        let mut wtxn = self.env.write_txn()?;

        // We ask the update store to give us a new update id, this is safe,
        // no other update can have the same id because we use a write txn before
        // asking for the id and registering it so other update registering
        // will be forced to wait for a new write txn.
        let update_id = self.new_update_id(&wtxn)?;
        let update_key = BEU64::new(update_id);

        let meta = Pending::new(meta, update_id, index_uuid);
        self.pending_meta.put(&mut wtxn, &update_key, &meta)?;
        self.pending
            .put(&mut wtxn, &update_key, &content.as_ref().to_owned())?;

        wtxn.commit()?;

        self.notification_sender
            .blocking_send(())
            .expect("Update store loop exited.");
        Ok(meta)
    }
    /// Executes the user provided function on the next pending update (the one with the lowest id).
    /// This is asynchronous as it let the user process the update with a read-only txn and
    /// only writing the result meta to the processed-meta store *after* it has been processed.
    fn process_pending_update<U>(&self, mut handler: U) -> anyhow::Result<Option<()>>
    where
        U: HandleUpdate<M, N, E>,
    {
        let _lock = self.update_lock.lock();
        // Create a read transaction to be able to retrieve the pending update in order.
        let rtxn = self.env.read_txn()?;
        let first_meta = self.pending_meta.first(&rtxn)?;

        // If there is a pending update we process and only keep
        // a reader while processing it, not a writer.
        match first_meta {
            Some((first_id, pending)) => {
                let content_path = self
                    .pending
                    .get(&rtxn, &first_id)?
                    .expect("associated update content");

                // we change the state of the update from pending to processing before we pass it
                // to the update handler. Processing store is non persistent to be able recover
                // from a failure
                let processing = pending.processing();
                self.processing.write().replace(processing.clone());
                let file = File::open(&content_path)?;
                // Process the pending update using the provided user function.
                let result = handler.handle_update(processing, file)?;
                drop(rtxn);

                // Once the pending update have been successfully processed
                // we must remove the content from the pending and processing stores and
                // write the *new* meta to the processed-meta store and commit.
                let mut wtxn = self.env.write_txn()?;
                self.processing.write().take();
                self.pending_meta.delete(&mut wtxn, &first_id)?;
                remove_file(&content_path)?;
                self.pending.delete(&mut wtxn, &first_id)?;
                match result {
                    Ok(processed) => self.processed_meta.put(&mut wtxn, &first_id, &processed)?,
                    Err(failed) => self.failed_meta.put(&mut wtxn, &first_id, &failed)?,
                }
                wtxn.commit()?;

                Ok(Some(()))
            }
            None => Ok(None),
        }
    }

    pub fn list(&self) -> anyhow::Result<Vec<UpdateStatus<M, N, E>>> {
        let rtxn = self.env.read_txn()?;
        let mut updates = Vec::new();

        let processing = self.processing.read();
        if let Some(ref processing) = *processing {
            let update = UpdateStatus::from(processing.clone());
            updates.push(update);
        }

        let pending = self
            .pending_meta
            .iter(&rtxn)?
            .filter_map(Result::ok)
            .filter_map(|(_, p)| (Some(p.id()) != processing.as_ref().map(|p| p.id())).then(|| p))
            .map(UpdateStatus::from);

        updates.extend(pending);

        let aborted = self
            .aborted_meta
            .iter(&rtxn)?
            .filter_map(Result::ok)
            .map(|(_, p)| p)
            .map(UpdateStatus::from);

        updates.extend(aborted);

        let processed = self
            .processed_meta
            .iter(&rtxn)?
            .filter_map(Result::ok)
            .map(|(_, p)| p)
            .map(UpdateStatus::from);

        updates.extend(processed);

        let failed = self
            .failed_meta
            .iter(&rtxn)?
            .filter_map(Result::ok)
            .map(|(_, p)| p)
            .map(UpdateStatus::from);

        updates.extend(failed);

        updates.sort_by_key(|u| u.id());

        Ok(updates)
    }

    /// Returns the update associated meta or `None` if the update doesn't exist.
    pub fn meta(&self, update_id: u64) -> heed::Result<Option<UpdateStatus<M, N, E>>> {
        let rtxn = self.env.read_txn()?;
        let key = BEU64::new(update_id);

        if let Some(ref meta) = *self.processing.read() {
            if meta.id() == update_id {
                return Ok(Some(UpdateStatus::Processing(meta.clone())));
            }
        }

        if let Some(meta) = self.pending_meta.get(&rtxn, &key)? {
            return Ok(Some(UpdateStatus::Pending(meta)));
        }

        if let Some(meta) = self.processed_meta.get(&rtxn, &key)? {
            return Ok(Some(UpdateStatus::Processed(meta)));
        }

        if let Some(meta) = self.aborted_meta.get(&rtxn, &key)? {
            return Ok(Some(UpdateStatus::Aborted(meta)));
        }

        if let Some(meta) = self.failed_meta.get(&rtxn, &key)? {
            return Ok(Some(UpdateStatus::Failed(meta)));
        }

        Ok(None)
    }

    /// Aborts an update, an aborted update content is deleted and
    /// the meta of it is moved into the aborted updates database.
    ///
    /// Trying to abort an update that is currently being processed, an update
    /// that as already been processed or which doesn't actually exist, will
    /// return `None`.
    #[allow(dead_code)]
    pub fn abort_update(&self, update_id: u64) -> heed::Result<Option<Aborted<M>>> {
        let mut wtxn = self.env.write_txn()?;
        let key = BEU64::new(update_id);

        // We cannot abort an update that is currently being processed.
        if self.pending_meta.first(&wtxn)?.map(|(key, _)| key.get()) == Some(update_id) {
            return Ok(None);
        }

        let pending = match self.pending_meta.get(&wtxn, &key)? {
            Some(meta) => meta,
            None => return Ok(None),
        };

        let aborted = pending.abort();

        self.aborted_meta.put(&mut wtxn, &key, &aborted)?;
        self.pending_meta.delete(&mut wtxn, &key)?;
        self.pending.delete(&mut wtxn, &key)?;

        wtxn.commit()?;

        Ok(Some(aborted))
    }

    /// Aborts all the pending updates, and not the one being currently processed.
    /// Returns the update metas and ids that were successfully aborted.
    #[allow(dead_code)]
    pub fn abort_pendings(&self) -> heed::Result<Vec<(u64, Aborted<M>)>> {
        let mut wtxn = self.env.write_txn()?;
        let mut aborted_updates = Vec::new();

        // We skip the first pending update as it is currently being processed.
        for result in self.pending_meta.iter(&wtxn)?.skip(1) {
            let (key, pending) = result?;
            let id = key.get();
            aborted_updates.push((id, pending.abort()));
        }

        for (id, aborted) in &aborted_updates {
            let key = BEU64::new(*id);
            self.aborted_meta.put(&mut wtxn, &key, &aborted)?;
            self.pending_meta.delete(&mut wtxn, &key)?;
            self.pending.delete(&mut wtxn, &key)?;
        }

        wtxn.commit()?;

        Ok(aborted_updates)
    }

    pub fn snapshot(&self, txn: &mut heed::RwTxn, path: impl AsRef<Path>, uuid: Uuid) -> anyhow::Result<()> {
        let update_path = path.as_ref().join("updates");
        create_dir_all(&update_path)?;

        let mut snapshot_path = update_path.join(format!("update-{}", uuid));
        // acquire write lock to prevent further writes during snapshot
        create_dir_all(&snapshot_path)?;
        snapshot_path.push("data.mdb");

        // create db snapshot
        self.env.copy_to_path(&snapshot_path, CompactionOption::Enabled)?;

        let update_files_path = update_path.join("update_files");
        create_dir_all(&update_files_path)?;

        for path in self.pending.iter(&txn)? {
            let (_, path) = path?;
            let name = path.file_name().unwrap();
            let to = update_files_path.join(name);
            copy(path, to)?;
        }

        println!("done");

        Ok(())
    }
}

//#[cfg(test)]
//mod tests {
//use super::*;
//use std::thread;
//use std::time::{Duration, Instant};

//#[test]
//fn simple() {
//let dir = tempfile::tempdir().unwrap();
//let mut options = EnvOpenOptions::new();
//options.map_size(4096 * 100);
//let update_store = UpdateStore::open(
//options,
//dir,
//|meta: Processing<String>, _content: &_| -> Result<_, Failed<_, ()>> {
//let new_meta = meta.meta().to_string() + " processed";
//let processed = meta.process(new_meta);
//Ok(processed)
//},
//)
//.unwrap();

//let meta = String::from("kiki");
//let update = update_store.register_update(meta, &[]).unwrap();
//thread::sleep(Duration::from_millis(100));
//let meta = update_store.meta(update.id()).unwrap().unwrap();
//if let UpdateStatus::Processed(Processed { success, .. }) = meta {
//assert_eq!(success, "kiki processed");
//} else {
//panic!()
//}
//}

//#[test]
//#[ignore]
//fn long_running_update() {
//let dir = tempfile::tempdir().unwrap();
//let mut options = EnvOpenOptions::new();
//options.map_size(4096 * 100);
//let update_store = UpdateStore::open(
//options,
//dir,
//|meta: Processing<String>, _content: &_| -> Result<_, Failed<_, ()>> {
//thread::sleep(Duration::from_millis(400));
//let new_meta = meta.meta().to_string() + "processed";
//let processed = meta.process(new_meta);
//Ok(processed)
//},
//)
//.unwrap();

//let before_register = Instant::now();

//let meta = String::from("kiki");
//let update_kiki = update_store.register_update(meta, &[]).unwrap();
//assert!(before_register.elapsed() < Duration::from_millis(200));

//let meta = String::from("coco");
//let update_coco = update_store.register_update(meta, &[]).unwrap();
//assert!(before_register.elapsed() < Duration::from_millis(200));

//let meta = String::from("cucu");
//let update_cucu = update_store.register_update(meta, &[]).unwrap();
//assert!(before_register.elapsed() < Duration::from_millis(200));

//thread::sleep(Duration::from_millis(400 * 3 + 100));

//let meta = update_store.meta(update_kiki.id()).unwrap().unwrap();
//if let UpdateStatus::Processed(Processed { success, .. }) = meta {
//assert_eq!(success, "kiki processed");
//} else {
//panic!()
//}

//let meta = update_store.meta(update_coco.id()).unwrap().unwrap();
//if let UpdateStatus::Processed(Processed { success, .. }) = meta {
//assert_eq!(success, "coco processed");
//} else {
//panic!()
//}

//let meta = update_store.meta(update_cucu.id()).unwrap().unwrap();
//if let UpdateStatus::Processed(Processed { success, .. }) = meta {
//assert_eq!(success, "cucu processed");
//} else {
//panic!()
//}
//}
//}

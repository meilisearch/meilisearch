use std::path::Path;
use std::sync::{Arc, RwLock};

use crossbeam_channel::Sender;
use heed::types::{OwnedType, DecodeIgnore, SerdeJson, ByteSlice};
use heed::{EnvOpenOptions, Env, Database};
use serde::{Serialize, Deserialize};

use crate::index_controller::updates::*;

type BEU64 = heed::zerocopy::U64<heed::byteorder::BE>;

#[derive(Clone)]
pub struct UpdateStore<M, N, E> {
    env: Env,
    pending_meta: Database<OwnedType<BEU64>, SerdeJson<Pending<M>>>,
    pending: Database<OwnedType<BEU64>, ByteSlice>,
    processed_meta: Database<OwnedType<BEU64>, SerdeJson<Processed<M, N>>>,
    failed_meta: Database<OwnedType<BEU64>, SerdeJson<Failed<M, E>>>,
    aborted_meta: Database<OwnedType<BEU64>, SerdeJson<Aborted<M>>>,
    processing: Arc<RwLock<Option<Processing<M>>>>,
    notification_sender: Sender<()>,
}

pub trait HandleUpdate<M, N, E> {
    fn handle_update(&mut self, meta: Processing<M>, content: &[u8]) -> Result<Processed<M, N>, Failed<M, E>>;
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
        mut update_handler: U,
    ) -> heed::Result<Arc<Self>>
    where
        P: AsRef<Path>,
        U: HandleUpdate<M, N, E> + Send + 'static,
    {
        options.max_dbs(5);

        let env = options.open(path)?;
        let pending_meta = env.create_database(Some("pending-meta"))?;
        let pending = env.create_database(Some("pending"))?;
        let processed_meta = env.create_database(Some("processed-meta"))?;
        let aborted_meta = env.create_database(Some("aborted-meta"))?;
        let failed_meta = env.create_database(Some("failed-meta"))?;
        let processing = Arc::new(RwLock::new(None));

        let (notification_sender, notification_receiver) = crossbeam_channel::bounded(1);
        // Send a first notification to trigger the process.
        let _ = notification_sender.send(());

        let update_store = Arc::new(UpdateStore {
            env,
            pending,
            pending_meta,
            processed_meta,
            aborted_meta,
            notification_sender,
            failed_meta,
            processing,
        });

        let update_store_cloned = update_store.clone();
        std::thread::spawn(move || {
            // Block and wait for something to process.
            for () in notification_receiver {
                loop {
                    match update_store_cloned.process_pending_update(&mut update_handler) {
                        Ok(Some(_)) => (),
                        Ok(None) => break,
                        Err(e) => eprintln!("error while processing update: {}", e),
                    }
                }
            }
        });

        Ok(update_store)
    }

    /// Returns the new biggest id to use to store the new update.
    fn new_update_id(&self, txn: &heed::RoTxn) -> heed::Result<u64> {
        let last_pending = self.pending_meta
            .remap_data_type::<DecodeIgnore>()
            .last(txn)?
            .map(|(k, _)| k.get());

        let last_processed = self.processed_meta
            .remap_data_type::<DecodeIgnore>()
            .last(txn)?
            .map(|(k, _)| k.get());

        let last_aborted = self.aborted_meta
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
        content: &[u8]
    ) -> heed::Result<Pending<M>> {
        let mut wtxn = self.env.write_txn()?;

        // We ask the update store to give us a new update id, this is safe,
        // no other update can have the same id because we use a write txn before
        // asking for the id and registering it so other update registering
        // will be forced to wait for a new write txn.
        let update_id = self.new_update_id(&wtxn)?;
        let update_key = BEU64::new(update_id);

        let meta = Pending::new(meta, update_id);
        self.pending_meta.put(&mut wtxn, &update_key, &meta)?;
        self.pending.put(&mut wtxn, &update_key, content)?;

        wtxn.commit()?;

        if let Err(e) = self.notification_sender.try_send(()) {
            assert!(!e.is_disconnected(), "update notification channel is disconnected");
        }
        Ok(meta)
    }
    /// Executes the user provided function on the next pending update (the one with the lowest id).
    /// This is asynchronous as it let the user process the update with a read-only txn and
    /// only writing the result meta to the processed-meta store *after* it has been processed.
    fn process_pending_update<U>(&self, handler: &mut U) -> heed::Result<Option<()>>
    where
        U: HandleUpdate<M, N, E> + Send + 'static,
    {
        // Create a read transaction to be able to retrieve the pending update in order.
        let rtxn = self.env.read_txn()?;
        let first_meta = self.pending_meta.first(&rtxn)?;

        // If there is a pending update we process and only keep
        // a reader while processing it, not a writer.
        match first_meta {
            Some((first_id, pending)) => {
                let first_content = self.pending
                    .get(&rtxn, &first_id)?
                    .expect("associated update content");

                // we change the state of the update from pending to processing before we pass it
                // to the update handler. Processing store is non persistent to be able recover
                // from a failure
                let processing = pending.processing();
                self.processing
                    .write()
                    .unwrap()
                    .replace(processing.clone());
                // Process the pending update using the provided user function.
                let result = handler.handle_update(processing, first_content);
                drop(rtxn);

                // Once the pending update have been successfully processed
                // we must remove the content from the pending and processing stores and
                // write the *new* meta to the processed-meta store and commit.
                let mut wtxn = self.env.write_txn()?;
                self.processing
                    .write()
                    .unwrap()
                    .take();
                self.pending_meta.delete(&mut wtxn, &first_id)?;
                self.pending.delete(&mut wtxn, &first_id)?;
                match result {
                    Ok(processed) => self.processed_meta.put(&mut wtxn, &first_id, &processed)?,
                    Err(failed) => self.failed_meta.put(&mut wtxn, &first_id, &failed)?,
                }
                wtxn.commit()?;

                Ok(Some(()))
            },
            None => Ok(None)
        }
    }

    /// Execute the user defined function with the meta-store iterators, the first
    /// iterator is the *processed* meta one, the second the *aborted* meta one
    /// and, the last is the *pending* meta one.
    pub fn iter_metas<F, T>(&self, mut f: F) -> heed::Result<T>
    where
        F: for<'a> FnMut(
            Option<Processing<M>>,
            heed::RoIter<'a, OwnedType<BEU64>, SerdeJson<Processed<M, N>>>,
            heed::RoIter<'a, OwnedType<BEU64>, SerdeJson<Aborted<M>>>,
            heed::RoIter<'a, OwnedType<BEU64>, SerdeJson<Pending<M>>>,
            heed::RoIter<'a, OwnedType<BEU64>, SerdeJson<Failed<M, E>>>,
        ) -> heed::Result<T>,
    {
        let rtxn = self.env.read_txn()?;

        // We get the pending, processed and aborted meta iterators.
        let processed_iter = self.processed_meta.iter(&rtxn)?;
        let aborted_iter = self.aborted_meta.iter(&rtxn)?;
        let pending_iter = self.pending_meta.iter(&rtxn)?;
        let processing = self.processing.read().unwrap().clone();
        let failed_iter = self.failed_meta.iter(&rtxn)?;

        // We execute the user defined function with both iterators.
        (f)(processing, processed_iter, aborted_iter, pending_iter, failed_iter)
    }

    /// Returns the update associated meta or `None` if the update doesn't exist.
    pub fn meta(&self, update_id: u64) -> heed::Result<Option<UpdateStatus<M, N, E>>> {
        let rtxn = self.env.read_txn()?;
        let key = BEU64::new(update_id);

        if let Some(ref meta) = *self.processing.read().unwrap() {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

    impl<M, N, F, E> HandleUpdate<M, N, E> for F
        where F: FnMut(Processing<M>, &[u8]) -> Result<Processed<M, N>, Failed<M, E>> + Send + 'static {
            fn handle_update(&mut self, meta: Processing<M>, content: &[u8]) -> Result<Processed<M, N>, Failed<M, E>> {
                self(meta, content)
            }
        }

    #[test]
    fn simple() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(4096 * 100);
        let update_store = UpdateStore::open(options, dir, |meta: Processing<String>, _content: &_| -> Result<_, Failed<_, ()>> {
            let new_meta = meta.meta().to_string() + " processed";
            let processed = meta.process(new_meta);
            Ok(processed)
        }).unwrap();

        let meta = String::from("kiki");
        let update = update_store.register_update(meta, &[]).unwrap();
        thread::sleep(Duration::from_millis(100));
        let meta = update_store.meta(update.id()).unwrap().unwrap();
        if let UpdateStatus::Processed(Processed { success, .. }) = meta {
            assert_eq!(success, "kiki processed");
        } else {
            panic!()
        }
    }

    #[test]
    #[ignore]
    fn long_running_update() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(4096 * 100);
        let update_store = UpdateStore::open(options, dir, |meta: Processing<String>, _content:&_| -> Result<_, Failed<_, ()>> {
            thread::sleep(Duration::from_millis(400));
            let new_meta = meta.meta().to_string() + "processed";
            let processed = meta.process(new_meta);
            Ok(processed)
        }).unwrap();

        let before_register = Instant::now();

        let meta = String::from("kiki");
        let update_kiki = update_store.register_update(meta, &[]).unwrap();
        assert!(before_register.elapsed() < Duration::from_millis(200));

        let meta = String::from("coco");
        let update_coco = update_store.register_update(meta, &[]).unwrap();
        assert!(before_register.elapsed() < Duration::from_millis(200));

        let meta = String::from("cucu");
        let update_cucu = update_store.register_update(meta, &[]).unwrap();
        assert!(before_register.elapsed() < Duration::from_millis(200));

        thread::sleep(Duration::from_millis(400 * 3 + 100));

        let meta = update_store.meta(update_kiki.id()).unwrap().unwrap();
        if let UpdateStatus::Processed(Processed { success, .. }) = meta {
            assert_eq!(success, "kiki processed");
        } else {
            panic!()
        }

        let meta = update_store.meta(update_coco.id()).unwrap().unwrap();
        if let UpdateStatus::Processed(Processed { success, .. }) = meta {
            assert_eq!(success, "coco processed");
        } else {
            panic!()
        }

        let meta = update_store.meta(update_cucu.id()).unwrap().unwrap();
        if let UpdateStatus::Processed(Processed { success, .. }) = meta {
            assert_eq!(success, "cucu processed");
        } else {
            panic!()
        }
    }
}

use std::path::Path;
use std::sync::Arc;

use crossbeam_channel::Sender;
use heed::types::{OwnedType, DecodeIgnore, SerdeBincode, ByteSlice};
use heed::{EnvOpenOptions, Env, Database};
use serde::{Serialize, Deserialize};

use crate::BEU64;

#[derive(Clone)]
pub struct UpdateStore<M> {
    env: Env,
    pending_meta: Database<OwnedType<BEU64>, SerdeBincode<M>>,
    pending: Database<OwnedType<BEU64>, ByteSlice>,
    processed_meta: Database<OwnedType<BEU64>, SerdeBincode<M>>,
    notification_sender: Sender<()>,
}

impl<M: 'static> UpdateStore<M> {
    pub fn open<P, F>(
        mut options: EnvOpenOptions,
        path: P,
        mut update_function: F,
    ) -> heed::Result<Arc<UpdateStore<M>>>
    where
        P: AsRef<Path>,
        F: FnMut(u64, M, &[u8]) -> heed::Result<M> + Send + 'static,
        M: for<'a> Deserialize<'a> + Serialize,
    {
        options.max_dbs(3);
        let env = options.open(path)?;
        let pending_meta = env.create_database(Some("pending-meta"))?;
        let pending = env.create_database(Some("pending"))?;
        let processed_meta = env.create_database(Some("processed-meta"))?;

        let (notification_sender, notification_receiver) = crossbeam_channel::bounded(1);
        let update_store = Arc::new(UpdateStore {
            env,
            pending,
            pending_meta,
            processed_meta,
            notification_sender,
        });

        let update_store_cloned = update_store.clone();
        std::thread::spawn(move || {
            // Block and wait for something to process.
            for () in notification_receiver {
                loop {
                    match update_store_cloned.process_pending_update(&mut update_function) {
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
            .as_polymorph()
            .last::<_, OwnedType<BEU64>, DecodeIgnore>(txn)?
            .map(|(k, _)| k.get());

        if let Some(last_id) = last_pending {
            return Ok(last_id + 1);
        }

        let last_processed = self.processed_meta
            .as_polymorph()
            .last::<_, OwnedType<BEU64>, DecodeIgnore>(txn)?
            .map(|(k, _)| k.get());

        match last_processed {
            Some(last_id) => Ok(last_id + 1),
            None => Ok(0),
        }
    }

    /// Registers the update content in the pending store and the meta
    /// into the pending-meta store. Returns the new unique update id.
    pub fn register_update(&self, meta: &M, content: &[u8]) -> heed::Result<u64>
    where M: Serialize,
    {
        let mut wtxn = self.env.write_txn()?;

        // We ask the update store to give us a new update id, this is safe,
        // no other update can have the same id because we use a write txn before
        // asking for the id and registering it so other update registering
        // will be forced to wait for a new write txn.
        let update_id = self.new_update_id(&wtxn)?;
        let update_key = BEU64::new(update_id);

        self.pending_meta.put(&mut wtxn, &update_key, meta)?;
        self.pending.put(&mut wtxn, &update_key, content)?;

        wtxn.commit()?;

        if let Err(e) = self.notification_sender.try_send(()) {
            assert!(!e.is_disconnected(), "update notification channel is disconnected");
        }

        Ok(update_id)
    }

    /// Executes the user provided function on the next pending update (the one with the lowest id).
    /// This is asynchronous as it let the user process the update with a read-only txn and
    /// only writing the result meta to the processed-meta store *after* it has been processed.
    fn process_pending_update<F>(&self, mut f: F) -> heed::Result<Option<(u64, M)>>
    where
        F: FnMut(u64, M, &[u8]) -> heed::Result<M>,
        M: for<'a> Deserialize<'a> + Serialize,
    {
        // Create a read transaction to be able to retrieve the pending update in order.
        let rtxn = self.env.read_txn()?;
        let first_meta = self.pending_meta.first(&rtxn)?;

        // If there is a pending update we process and only keep
        // a reader while processing it, not a writer.
        match first_meta {
            Some((first_id, first_meta)) => {
                let first_content = self.pending
                    .get(&rtxn, &first_id)?
                    .expect("associated update content");

                // Process the pending update using the provided user function.
                let new_meta = (f)(first_id.get(), first_meta, first_content)?;
                drop(rtxn);

                // Once the pending update have been successfully processed
                // we must remove the content from the pending stores and
                // write the *new* meta to the processed-meta store and commit.
                let mut wtxn = self.env.write_txn()?;
                self.pending_meta.delete(&mut wtxn, &first_id)?;
                self.pending.delete(&mut wtxn, &first_id)?;
                self.processed_meta.put(&mut wtxn, &first_id, &new_meta)?;
                wtxn.commit()?;

                Ok(Some((first_id.get(), new_meta)))
            },
            None => Ok(None)
        }
    }

    /// Execute the user defined function with both meta-store iterators, the first
    /// iterator is the *processed* meta one and the secind is the *pending* meta one.
    pub fn iter_metas<F, T>(&self, mut f: F) -> heed::Result<T>
    where
        M: for<'a> Deserialize<'a>,
        F: for<'a> FnMut(
            heed::RoIter<'a, OwnedType<BEU64>, SerdeBincode<M>>,
            heed::RoIter<'a, OwnedType<BEU64>, SerdeBincode<M>>,
        ) -> heed::Result<T>,
    {
        let rtxn = self.env.read_txn()?;

        // We get both the pending and processed meta iterators.
        let processed_iter = self.processed_meta.iter(&rtxn)?;
        let pending_iter = self.pending_meta.iter(&rtxn)?;

        // We execute the user defined function with both iterators.
        (f)(processed_iter, pending_iter)
    }

    /// Returns the update associated meta or `None` if the update deosn't exist.
    pub fn meta(&self, update_id: u64) -> heed::Result<Option<M>>
    where M: for<'a> Deserialize<'a>,
    {
        let rtxn = self.env.read_txn()?;
        let key = BEU64::new(update_id);

        if let Some(meta) = self.pending_meta.get(&rtxn, &key)? {
            return Ok(Some(meta));
        }

        match self.processed_meta.get(&rtxn, &key)? {
            Some(meta) => Ok(Some(meta)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn simple() {
        let dir = tempfile::tempdir().unwrap();
        let options = EnvOpenOptions::new();
        let update_store = UpdateStore::open(options, dir, |id, meta: String, content| {
            Ok(meta + " processed")
        }).unwrap();

        let meta = String::from("kiki");
        let update_id = update_store.register_update(&meta, &[]).unwrap();

        thread::sleep(Duration::from_millis(100));

        let meta = update_store.meta(update_id).unwrap().unwrap();
        assert_eq!(meta, "kiki processed");
    }

    #[test]
    fn long_running_update() {
        let dir = tempfile::tempdir().unwrap();
        let options = EnvOpenOptions::new();
        let update_store = UpdateStore::open(options, dir, |id, meta: String, content| {
            thread::sleep(Duration::from_millis(400));
            Ok(meta + " processed")
        }).unwrap();

        let before_register = Instant::now();

        let meta = String::from("kiki");
        let update_id_kiki = update_store.register_update(&meta, &[]).unwrap();
        assert!(before_register.elapsed() < Duration::from_millis(200));

        let meta = String::from("coco");
        let update_id_coco = update_store.register_update(&meta, &[]).unwrap();
        assert!(before_register.elapsed() < Duration::from_millis(200));

        let meta = String::from("cucu");
        let update_id_cucu = update_store.register_update(&meta, &[]).unwrap();
        assert!(before_register.elapsed() < Duration::from_millis(200));

        thread::sleep(Duration::from_millis(400 * 3 + 100));

        let meta = update_store.meta(update_id_kiki).unwrap().unwrap();
        assert_eq!(meta, "kiki processed");

        let meta = update_store.meta(update_id_coco).unwrap().unwrap();
        assert_eq!(meta, "coco processed");

        let meta = update_store.meta(update_id_cucu).unwrap().unwrap();
        assert_eq!(meta, "cucu processed");
    }
}

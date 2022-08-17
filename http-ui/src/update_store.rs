#![allow(unused)]

use std::path::Path;
use std::sync::Arc;

use crossbeam_channel::Sender;
use heed::types::{ByteSlice, DecodeIgnore, OwnedType, SerdeJson};
use heed::{Database, Env, EnvOpenOptions};
use milli::heed;
use serde::{Deserialize, Serialize};

pub type BEU64 = heed::zerocopy::U64<heed::byteorder::BE>;

#[derive(Clone)]
pub struct UpdateStore<M, N> {
    env: Env,
    pending_meta: Database<OwnedType<BEU64>, SerdeJson<M>>,
    pending: Database<OwnedType<BEU64>, ByteSlice>,
    processed_meta: Database<OwnedType<BEU64>, SerdeJson<N>>,
    aborted_meta: Database<OwnedType<BEU64>, SerdeJson<M>>,
    notification_sender: Sender<()>,
}

pub trait UpdateHandler<M, N> {
    fn handle_update(&mut self, update_id: u64, meta: M, content: &[u8]) -> heed::Result<N>;
}

impl<M, N, F> UpdateHandler<M, N> for F
where
    F: FnMut(u64, M, &[u8]) -> heed::Result<N> + Send + 'static,
{
    fn handle_update(&mut self, update_id: u64, meta: M, content: &[u8]) -> heed::Result<N> {
        self(update_id, meta, content)
    }
}

impl<M: 'static, N: 'static> UpdateStore<M, N> {
    pub fn open<P, U>(
        mut options: EnvOpenOptions,
        path: P,
        mut update_handler: U,
    ) -> heed::Result<Arc<UpdateStore<M, N>>>
    where
        P: AsRef<Path>,
        U: UpdateHandler<M, N> + Send + 'static,
        M: for<'a> Deserialize<'a>,
        N: Serialize,
    {
        options.max_dbs(4);
        let env = options.open(path)?;
        let pending_meta = env.create_database(Some("pending-meta"))?;
        let pending = env.create_database(Some("pending"))?;
        let processed_meta = env.create_database(Some("processed-meta"))?;
        let aborted_meta = env.create_database(Some("aborted-meta"))?;

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
        let last_pending =
            self.pending_meta.remap_data_type::<DecodeIgnore>().last(txn)?.map(|(k, _)| k.get());

        let last_processed =
            self.processed_meta.remap_data_type::<DecodeIgnore>().last(txn)?.map(|(k, _)| k.get());

        let last_aborted =
            self.aborted_meta.remap_data_type::<DecodeIgnore>().last(txn)?.map(|(k, _)| k.get());

        let last_update_id =
            [last_pending, last_processed, last_aborted].iter().copied().flatten().max();

        match last_update_id {
            Some(last_id) => Ok(last_id + 1),
            None => Ok(0),
        }
    }

    /// Registers the update content in the pending store and the meta
    /// into the pending-meta store. Returns the new unique update id.
    pub fn register_update(&self, meta: &M, content: &[u8]) -> heed::Result<u64>
    where
        M: Serialize,
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
    fn process_pending_update<U>(&self, handler: &mut U) -> heed::Result<Option<(u64, N)>>
    where
        U: UpdateHandler<M, N>,
        M: for<'a> Deserialize<'a>,
        N: Serialize,
    {
        // Create a read transaction to be able to retrieve the pending update in order.
        let rtxn = self.env.read_txn()?;
        let first_meta = self.pending_meta.first(&rtxn)?;

        // If there is a pending update we process and only keep
        // a reader while processing it, not a writer.
        match first_meta {
            Some((first_id, first_meta)) => {
                let first_content =
                    self.pending.get(&rtxn, &first_id)?.expect("associated update content");

                // Process the pending update using the provided user function.
                let new_meta = handler.handle_update(first_id.get(), first_meta, first_content)?;
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
            }
            None => Ok(None),
        }
    }

    /// The id and metadata of the update that is currently being processed,
    /// `None` if no update is being processed.
    pub fn processing_update(&self) -> heed::Result<Option<(u64, M)>>
    where
        M: for<'a> Deserialize<'a>,
    {
        let rtxn = self.env.read_txn()?;
        match self.pending_meta.first(&rtxn)? {
            Some((key, meta)) => Ok(Some((key.get(), meta))),
            None => Ok(None),
        }
    }

    /// Execute the user defined function with the meta-store iterators, the first
    /// iterator is the *processed* meta one, the second the *aborted* meta one
    /// and, the last is the *pending* meta one.
    pub fn iter_metas<F, T>(&self, mut f: F) -> heed::Result<T>
    where
        M: for<'a> Deserialize<'a>,
        N: for<'a> Deserialize<'a>,
        F: for<'a> FnMut(
            heed::RoIter<'a, OwnedType<BEU64>, SerdeJson<N>>,
            heed::RoIter<'a, OwnedType<BEU64>, SerdeJson<M>>,
            heed::RoIter<'a, OwnedType<BEU64>, SerdeJson<M>>,
        ) -> heed::Result<T>,
    {
        let rtxn = self.env.read_txn()?;

        // We get the pending, processed and aborted meta iterators.
        let processed_iter = self.processed_meta.iter(&rtxn)?;
        let aborted_iter = self.aborted_meta.iter(&rtxn)?;
        let pending_iter = self.pending_meta.iter(&rtxn)?;

        // We execute the user defined function with both iterators.
        (f)(processed_iter, aborted_iter, pending_iter)
    }

    /// Returns the update associated meta or `None` if the update doesn't exist.
    pub fn meta(&self, update_id: u64) -> heed::Result<Option<UpdateStatusMeta<M, N>>>
    where
        M: for<'a> Deserialize<'a>,
        N: for<'a> Deserialize<'a>,
    {
        let rtxn = self.env.read_txn()?;
        let key = BEU64::new(update_id);

        if let Some(meta) = self.pending_meta.get(&rtxn, &key)? {
            return Ok(Some(UpdateStatusMeta::Pending(meta)));
        }

        if let Some(meta) = self.processed_meta.get(&rtxn, &key)? {
            return Ok(Some(UpdateStatusMeta::Processed(meta)));
        }

        if let Some(meta) = self.aborted_meta.get(&rtxn, &key)? {
            return Ok(Some(UpdateStatusMeta::Aborted(meta)));
        }

        Ok(None)
    }

    /// Aborts an update, an aborted update content is deleted and
    /// the meta of it is moved into the aborted updates database.
    ///
    /// Trying to abort an update that is currently being processed, an update
    /// that as already been processed or which doesn't actually exist, will
    /// return `None`.
    pub fn abort_update(&self, update_id: u64) -> heed::Result<Option<M>>
    where
        M: Serialize + for<'a> Deserialize<'a>,
    {
        let mut wtxn = self.env.write_txn()?;
        let key = BEU64::new(update_id);

        // We cannot abort an update that is currently being processed.
        if self.pending_meta.first(&wtxn)?.map(|(key, _)| key.get()) == Some(update_id) {
            return Ok(None);
        }

        let meta = match self.pending_meta.get(&wtxn, &key)? {
            Some(meta) => meta,
            None => return Ok(None),
        };

        self.aborted_meta.put(&mut wtxn, &key, &meta)?;
        self.pending_meta.delete(&mut wtxn, &key)?;
        self.pending.delete(&mut wtxn, &key)?;

        wtxn.commit()?;

        Ok(Some(meta))
    }

    /// Aborts all the pending updates, and not the one being currently processed.
    /// Returns the update metas and ids that were successfully aborted.
    pub fn abort_pendings(&self) -> heed::Result<Vec<(u64, M)>>
    where
        M: Serialize + for<'a> Deserialize<'a>,
    {
        let mut wtxn = self.env.write_txn()?;
        let mut aborted_updates = Vec::new();

        // We skip the first pending update as it is currently being processed.
        for result in self.pending_meta.iter(&wtxn)?.skip(1) {
            let (key, meta) = result?;
            let id = key.get();
            aborted_updates.push((id, meta));
        }

        for (id, meta) in &aborted_updates {
            let key = BEU64::new(*id);
            self.aborted_meta.put(&mut wtxn, &key, &meta)?;
            self.pending_meta.delete(&mut wtxn, &key)?;
            self.pending.delete(&mut wtxn, &key)?;
        }

        wtxn.commit()?;

        Ok(aborted_updates)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum UpdateStatusMeta<M, N> {
    Pending(M),
    Processed(N),
    Aborted(M),
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn simple() {
        let dir = tempfile::tempdir().unwrap();
        let options = EnvOpenOptions::new();
        let update_store = UpdateStore::open(options, dir, |_id, meta: String, _content: &_| {
            Ok(meta + " processed")
        })
        .unwrap();

        let meta = String::from("kiki");
        let update_id = update_store.register_update(&meta, &[]).unwrap();

        thread::sleep(Duration::from_millis(100));

        let meta = update_store.meta(update_id).unwrap().unwrap();
        assert_eq!(meta, UpdateStatusMeta::Processed(format!("kiki processed")));
    }

    #[test]
    #[ignore]
    fn long_running_update() {
        let dir = tempfile::tempdir().unwrap();
        let options = EnvOpenOptions::new();
        let update_store = UpdateStore::open(options, dir, |_id, meta: String, _content: &_| {
            thread::sleep(Duration::from_millis(400));
            Ok(meta + " processed")
        })
        .unwrap();

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
        assert_eq!(meta, UpdateStatusMeta::Processed(format!("kiki processed")));

        let meta = update_store.meta(update_id_coco).unwrap().unwrap();
        assert_eq!(meta, UpdateStatusMeta::Processed(format!("coco processed")));

        let meta = update_store.meta(update_id_cucu).unwrap().unwrap();
        assert_eq!(meta, UpdateStatusMeta::Processed(format!("cucu processed")));
    }
}

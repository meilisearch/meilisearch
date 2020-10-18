use std::path::Path;
use std::sync::Arc;

use crossbeam_channel::{bounded, Sender, Receiver};
use heed::types::{OwnedType, DecodeIgnore, SerdeJson, ByteSlice};
use heed::{EnvOpenOptions, Env, Database};
use once_cell::sync::OnceCell;
use serde::{Serialize, Deserialize};

use crate::BEU64;

#[derive(Clone)]
pub struct UpdateStore<M> {
    env: Env,
    pending_meta: Database<OwnedType<BEU64>, SerdeJson<M>>,
    pending: Database<OwnedType<BEU64>, ByteSlice>,
    processed_meta: Database<OwnedType<BEU64>, SerdeJson<M>>,
    notification_sender: Sender<()>,
}

impl<M: 'static + Send + Sync> UpdateStore<M> {
    pub fn open<P, F>(
        options: EnvOpenOptions,
        path: P,
        mut update_function: F,
    ) -> heed::Result<Arc<UpdateStore<M>>>
    where
        P: AsRef<Path>,
        F: FnMut(u64, M, &[u8]) -> heed::Result<M> + Send + 'static,
        M: for<'a> Deserialize<'a> + Serialize,
    {
        let env = options.open(path)?;
        let pending_meta = env.create_database(Some("pending-meta"))?;
        let pending = env.create_database(Some("pending"))?;
        let processed_meta = env.create_database(Some("processed-meta"))?;

        let (notification_sender, notification_receiver) = bounded(1);
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

    /// Iterate over the pending and the processed metas one after the other,
    /// calling the user defined callback for each meta.
    pub fn iter_meta<F>(&self, mut f: F) -> heed::Result<()>
    where
        M: for<'a> Deserialize<'a>,
        F: FnMut(u64, M),
    {
        let rtxn = self.env.read_txn()?;

        // We iterate over the pending updates.
        for result in self.pending_meta.iter(&rtxn)? {
            let (key, meta) = result?;
            (f)(key.get(), meta);
        }

        // We iterate over the processed updates.
        for result in self.processed_meta.iter(&rtxn)? {
            let (key, meta) = result?;
            (f)(key.get(), meta);
        }

        Ok(())
    }

    /// Returns the update associated meta or `None` if the update deosn't exist.
    pub fn update_meta(&self, update_id: u64) -> heed::Result<Option<M>>
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

use std::borrow::Cow;
use std::convert::TryInto;
use std::fs::{copy, create_dir_all, remove_file, File};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use bytemuck::{Pod, Zeroable};
use heed::types::{ByteSlice, DecodeIgnore, SerdeJson};
use heed::{BytesDecode, BytesEncode, CompactionOption, Database, Env, EnvOpenOptions};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::helpers::EnvSizer;
use crate::index_controller::updates::*;

#[allow(clippy::upper_case_acronyms)]
type BEU64 = heed::zerocopy::U64<heed::byteorder::BE>;

struct IndexUuidUpdateIdCodec;

#[repr(C)]
#[derive(Copy, Clone)]
struct IndexUuidUpdateId(Uuid, BEU64);

// Is Uuid really zeroable (semantically)?
unsafe impl Zeroable for IndexUuidUpdateId {}
unsafe impl Pod for IndexUuidUpdateId {}

impl IndexUuidUpdateId {
    fn new(uuid: Uuid, update_id: u64) -> Self {
        Self(uuid, BEU64::new(update_id))
    }
}

const UUID_SIZE: usize = size_of::<Uuid>();
const U64_SIZE: usize = size_of::<BEU64>();

impl<'a> BytesEncode<'a> for IndexUuidUpdateIdCodec {
    type EItem = IndexUuidUpdateId;

    fn bytes_encode(item: &'a Self::EItem) -> Option<std::borrow::Cow<'a, [u8]>> {
        let bytes = bytemuck::cast_ref::<IndexUuidUpdateId, [u8; UUID_SIZE + U64_SIZE]>(item);
        Some(Cow::Borrowed(&bytes[..]))
    }
}

impl<'a> BytesDecode<'a> for IndexUuidUpdateIdCodec {
    type DItem = (Uuid, u64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let bytes = bytes.try_into().ok()?;
        let IndexUuidUpdateId(uuid, id) =
            bytemuck::cast_ref::<[u8; UUID_SIZE + U64_SIZE], IndexUuidUpdateId>(bytes);
        Some((*uuid, id.get()))
    }
}

#[derive(Clone)]
pub struct UpdateStore<M, N, E> {
    pub env: Env,
    pending_meta: Database<ByteSlice, SerdeJson<Enqueued<M>>>,
    pending: Database<ByteSlice, SerdeJson<PathBuf>>,
    processed_meta: Database<ByteSlice, SerdeJson<Processed<M, N>>>,
    failed_meta: Database<ByteSlice, SerdeJson<Failed<M, E>>>,
    aborted_meta: Database<ByteSlice, SerdeJson<Aborted<M>>>,
    pub processing: Arc<RwLock<Option<(Uuid, Processing<M>)>>>,
    notification_sender: mpsc::Sender<()>,
    /// A lock on the update loop. This is meant to prevent a snapshot to occur while an update is
    /// processing, while not preventing writes all together during an update
    pub update_lock: Arc<Mutex<()>>,
}

pub trait HandleUpdate<M, N, E> {
    fn handle_update(
        &mut self,
        index_uuid: Uuid,
        meta: Processing<M>,
        content: File,
    ) -> anyhow::Result<Result<Processed<M, N>, Failed<M, E>>>;
}

impl<M, N, E, F> HandleUpdate<M, N, E> for F
where
    F: FnMut(Uuid, Processing<M>, File) -> anyhow::Result<Result<Processed<M, N>, Failed<M, E>>>,
{
    fn handle_update(
        &mut self,
        index_uuid: Uuid,
        meta: Processing<M>,
        content: File,
    ) -> anyhow::Result<Result<Processed<M, N>, Failed<M, E>>> {
        self(index_uuid, meta, content)
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
    ) -> anyhow::Result<Arc<Self>>
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

        // Init update loop to perform any pending updates at launch.
        // Since we just launched the update store, and we still own the receiving end of the
        // channel, this call is guarenteed to succeed.
        notification_sender.try_send(()).expect("Failed to init update store");

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

    /// Returns the new biggest id to use to store the new update.
    fn new_update_id(&self, txn: &heed::RoTxn, index_uuid: Uuid) -> heed::Result<u64> {
        // TODO: this is a very inneficient process for finding the next update id for each index,
        // and needs to be made better.
        let last_pending = self
            .pending_meta
            .remap_data_type::<DecodeIgnore>()
            .prefix_iter(txn, index_uuid.as_bytes())?
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .last()
            .transpose()?
            .map(|((_, id), _)| id);

        let last_processed = self
            .processed_meta
            .remap_data_type::<DecodeIgnore>()
            .prefix_iter(txn, index_uuid.as_bytes())?
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .last()
            .transpose()?
            .map(|((_, id), _)| id);

        let last_aborted = self
            .aborted_meta
            .remap_data_type::<DecodeIgnore>()
            .prefix_iter(txn, index_uuid.as_bytes())?
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .last()
            .transpose()?
            .map(|((_, id), _)| id);

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
    ) -> heed::Result<Enqueued<M>> {
        let mut wtxn = self.env.write_txn()?;

        // We ask the update store to give us a new update id, this is safe,
        // no other update can have the same id because we use a write txn before
        // asking for the id and registering it so other update registering
        // will be forced to wait for a new write txn.
        let update_id = self.new_update_id(&wtxn, index_uuid)?;
        let meta = Enqueued::new(meta, update_id);
        let key = IndexUuidUpdateId::new(index_uuid, update_id);
        self.pending_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .put(&mut wtxn, &key, &meta)?;

        self.pending
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .put(&mut wtxn, &key, &content.as_ref().to_owned())?;

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

        let first_meta = self
            .pending_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .first(&rtxn)?;

        // If there is a pending update we process and only keep
        // a reader while processing it, not a writer.
        match first_meta {
            Some(((index_uuid, update_id), pending)) => {
                let key = IndexUuidUpdateId::new(index_uuid, update_id);
                let content_path = self
                    .pending
                    .remap_key_type::<IndexUuidUpdateIdCodec>()
                    .get(&rtxn, &key)?
                    .expect("associated update content");

                // we change the state of the update from pending to processing before we pass it
                // to the update handler. Processing store is non persistent to be able recover
                // from a failure
                let processing = pending.processing();
                self.processing
                    .write()
                    .replace((index_uuid, processing.clone()));
                let file = File::open(&content_path)
                    .with_context(|| format!("file at path: {:?}", &content_path))?;
                // Process the pending update using the provided user function.
                let result = handler.handle_update(index_uuid, processing, file)?;
                drop(rtxn);

                // Once the pending update have been successfully processed
                // we must remove the content from the pending and processing stores and
                // write the *new* meta to the processed-meta store and commit.
                let mut wtxn = self.env.write_txn()?;
                self.processing.write().take();
                self.pending_meta
                    .remap_key_type::<IndexUuidUpdateIdCodec>()
                    .delete(&mut wtxn, &key)?;

                remove_file(&content_path)?;

                self.pending
                    .remap_key_type::<IndexUuidUpdateIdCodec>()
                    .delete(&mut wtxn, &key)?;
                match result {
                    Ok(processed) => self
                        .processed_meta
                        .remap_key_type::<IndexUuidUpdateIdCodec>()
                        .put(&mut wtxn, &key, &processed)?,
                    Err(failed) => self
                        .failed_meta
                        .remap_key_type::<IndexUuidUpdateIdCodec>()
                        .put(&mut wtxn, &key, &failed)?,
                }
                wtxn.commit()?;

                Ok(Some(()))
            }
            None => Ok(None),
        }
    }

    pub fn list(&self, index_uuid: Uuid) -> anyhow::Result<Vec<UpdateStatus<M, N, E>>> {
        let rtxn = self.env.read_txn()?;
        let mut updates = Vec::new();

        let processing = self.processing.read();
        if let Some((uuid, ref processing)) = *processing {
            if uuid == index_uuid {
                let update = UpdateStatus::from(processing.clone());
                updates.push(update);
            }
        }

        let pending = self
            .pending_meta
            .prefix_iter(&rtxn, index_uuid.as_bytes())?
            .filter_map(Result::ok)
            .filter_map(|(_, p)| {
                if let Some((uuid, ref processing)) = *processing {
                    // Filter out the currently processing update if it is from this index.
                    if uuid == index_uuid && processing.id() == p.id() {
                        None
                    } else {
                        Some(p)
                    }
                } else {
                    Some(p)
                }
            })
            .map(UpdateStatus::from);

        updates.extend(pending);

        let aborted = self
            .aborted_meta
            .prefix_iter(&rtxn, index_uuid.as_bytes())?
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
    pub fn meta(
        &self,
        index_uuid: Uuid,
        update_id: u64,
    ) -> heed::Result<Option<UpdateStatus<M, N, E>>> {
        let rtxn = self.env.read_txn()?;
        let key = IndexUuidUpdateId::new(index_uuid, update_id);

        if let Some((uuid, ref meta)) = *self.processing.read() {
            if uuid == index_uuid && meta.id() == update_id {
                return Ok(Some(UpdateStatus::Processing(meta.clone())));
            }
        }

        if let Some(meta) = self
            .pending_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .get(&rtxn, &key)?
        {
            return Ok(Some(UpdateStatus::Enqueued(meta)));
        }

        if let Some(meta) = self
            .processed_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .get(&rtxn, &key)?
        {
            return Ok(Some(UpdateStatus::Processed(meta)));
        }

        if let Some(meta) = self
            .aborted_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .get(&rtxn, &key)?
        {
            return Ok(Some(UpdateStatus::Aborted(meta)));
        }

        if let Some(meta) = self
            .failed_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .get(&rtxn, &key)?
        {
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
    pub fn abort_update(
        &self,
        index_uuid: Uuid,
        update_id: u64,
    ) -> heed::Result<Option<Aborted<M>>> {
        let mut wtxn = self.env.write_txn()?;
        let key = IndexUuidUpdateId::new(index_uuid, update_id);

        // We cannot abort an update that is currently being processed.
        if self
            .pending_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .first(&wtxn)?
            .map(|((_, id), _)| id)
            == Some(update_id)
        {
            return Ok(None);
        }

        let pending = match self
            .pending_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .get(&wtxn, &key)?
        {
            Some(meta) => meta,
            None => return Ok(None),
        };

        let aborted = pending.abort();

        self.aborted_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .put(&mut wtxn, &key, &aborted)?;
        self.pending_meta
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .delete(&mut wtxn, &key)?;
        self.pending
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .delete(&mut wtxn, &key)?;

        wtxn.commit()?;

        Ok(Some(aborted))
    }

    /// Aborts all the pending updates, and not the one being currently processed.
    /// Returns the update metas and ids that were successfully aborted.
    #[allow(dead_code)]
    pub fn abort_pendings(&self, index_uuid: Uuid) -> heed::Result<Vec<(u64, Aborted<M>)>> {
        let mut wtxn = self.env.write_txn()?;
        let mut aborted_updates = Vec::new();

        // We skip the first pending update as it is currently being processed.
        for result in self
            .pending_meta
            .prefix_iter(&wtxn, index_uuid.as_bytes())?
            .remap_key_type::<IndexUuidUpdateIdCodec>()
            .skip(1)
        {
            let ((_, update_id), pending) = result?;
            aborted_updates.push((update_id, pending.abort()));
        }

        for (id, aborted) in &aborted_updates {
            let key = IndexUuidUpdateId::new(index_uuid, *id);
            self.aborted_meta
                .remap_key_type::<IndexUuidUpdateIdCodec>()
                .put(&mut wtxn, &key, &aborted)?;
            self.pending_meta
                .remap_key_type::<IndexUuidUpdateIdCodec>()
                .delete(&mut wtxn, &key)?;
            self.pending
                .remap_key_type::<IndexUuidUpdateIdCodec>()
                .delete(&mut wtxn, &key)?;
        }

        wtxn.commit()?;

        Ok(aborted_updates)
    }

    pub fn delete_all(&self, uuid: Uuid) -> anyhow::Result<()> {
        fn delete_all<A>(
            txn: &mut heed::RwTxn,
            uuid: Uuid,
            db: Database<ByteSlice, A>,
        ) -> anyhow::Result<()>
        where
            A: for<'a> heed::BytesDecode<'a>,
        {
            let mut iter = db.prefix_iter_mut(txn, uuid.as_bytes())?;
            while let Some(_) = iter.next() {
                iter.del_current()?;
            }
            Ok(())
        }

        let mut txn = self.env.write_txn()?;

        delete_all(&mut txn, uuid, self.pending)?;
        delete_all(&mut txn, uuid, self.pending_meta)?;
        delete_all(&mut txn, uuid, self.processed_meta)?;
        delete_all(&mut txn, uuid, self.aborted_meta)?;
        delete_all(&mut txn, uuid, self.failed_meta)?;

        let processing = self.processing.upgradable_read();
        if let Some((processing_uuid, _)) = *processing {
            if processing_uuid == uuid {
                parking_lot::RwLockUpgradableReadGuard::upgrade(processing).take();
            }
        }
        Ok(())
    }

    pub fn snapshot(
        &self,
        txn: &mut heed::RwTxn,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let update_path = path.as_ref().join("updates");
        create_dir_all(&update_path)?;

        // acquire write lock to prevent further writes during snapshot
        create_dir_all(&update_path)?;
        let db_path = update_path.join("data.mdb");

        // create db snapshot
        self.env
            .copy_to_path(&db_path, CompactionOption::Enabled)?;

        let update_files_path = update_path.join("update_files");
        create_dir_all(&update_files_path)?;

        for path in self.pending.iter(&txn)? {
            let (_, path) = path?;
            let name = path.file_name().unwrap();
            let to = update_files_path.join(name);
            copy(path, to)?;
        }

        Ok(())
    }

    pub fn get_size(&self, txn: &heed::RoTxn) -> anyhow::Result<u64> {
        let mut size = self.env.size();

        for path in self.pending.iter(txn)? {
            let (_, path) = path?;

            if let Ok(metadata) = path.metadata() {
                size += metadata.len()
            }
        }

        Ok(size)
    }
}

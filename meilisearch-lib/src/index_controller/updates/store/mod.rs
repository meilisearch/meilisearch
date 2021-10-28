mod codec;
pub mod dump;

use std::fs::create_dir_all;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    time::Duration,
};

use arc_swap::ArcSwap;
use heed::types::{ByteSlice, OwnedType, SerdeJson};
use heed::zerocopy::U64;
use heed::{CompactionOption, Database, Env, EnvOpenOptions};
use log::error;
use parking_lot::{Mutex, MutexGuard};
use rayon::prelude::*;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::timeout;
use uuid::Uuid;

use codec::*;

use super::error::Result;
use super::status::{Enqueued, Processing};
use crate::index::Index;
use crate::index_controller::index_resolver::index_store::IndexStore;
use crate::index_controller::index_resolver::uuid_store::UuidStore;
use crate::index_controller::updates::*;
use crate::EnvSizer;

#[allow(clippy::upper_case_acronyms)]
type BEU64 = U64<heed::byteorder::BE>;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Update {
    DeleteDocuments(Vec<String>),
    DocumentAddition {
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_uuid: Uuid,
    },
    Settings(Settings<Unchecked>),
    ClearDocuments,
}

#[derive(Debug)]
pub struct UpdateStoreInfo {
    /// Size of the update store in bytes.
    pub size: u64,
    /// Uuid of the currently processing update if it exists
    pub processing: Option<Uuid>,
}

/// A data structure that allows concurrent reads AND exactly one writer.
pub struct StateLock {
    lock: Mutex<()>,
    data: ArcSwap<State>,
}

pub struct StateLockGuard<'a> {
    _lock: MutexGuard<'a, ()>,
    state: &'a StateLock,
}

impl StateLockGuard<'_> {
    pub fn swap(&self, state: State) -> Arc<State> {
        self.state.data.swap(Arc::new(state))
    }
}

impl StateLock {
    fn from_state(state: State) -> Self {
        let lock = Mutex::new(());
        let data = ArcSwap::from(Arc::new(state));
        Self { lock, data }
    }

    pub fn read(&self) -> Arc<State> {
        self.data.load().clone()
    }

    pub fn write(&self) -> StateLockGuard {
        let _lock = self.lock.lock();
        let state = &self;
        StateLockGuard { _lock, state }
    }
}

#[allow(clippy::large_enum_variant)]
pub enum State {
    Idle,
    Processing(Uuid, Processing),
    Snapshoting,
    Dumping,
}

#[derive(Clone)]
pub struct UpdateStore {
    pub env: Env,
    /// A queue containing the updates to process, ordered by arrival.
    /// The key are built as follow:
    /// | global_update_id | index_uuid | update_id |
    /// |     8-bytes      |  16-bytes  |  8-bytes  |
    pending_queue: Database<PendingKeyCodec, SerdeJson<Enqueued>>,
    /// Map indexes to the next available update id. If NextIdKey::Global is queried, then the next
    /// global update id is returned
    next_update_id: Database<NextIdCodec, OwnedType<BEU64>>,
    /// Contains all the performed updates meta, be they failed, aborted, or processed.
    /// The keys are built as follow:
    /// |    Uuid  |   id    |
    /// | 16-bytes | 8-bytes |
    updates: Database<UpdateKeyCodec, SerdeJson<UpdateStatus>>,
    /// Indicates the current state of the update store,
    state: Arc<StateLock>,
    /// Wake up the loop when a new event occurs.
    notification_sender: mpsc::Sender<()>,
    update_file_store: UpdateFileStore,
    path: PathBuf,
}

impl UpdateStore {
    fn new(
        mut options: EnvOpenOptions,
        path: impl AsRef<Path>,
        update_file_store: UpdateFileStore,
    ) -> anyhow::Result<(Self, mpsc::Receiver<()>)> {
        options.max_dbs(5);

        let update_path = path.as_ref().join("updates");
        std::fs::create_dir_all(&update_path)?;
        let env = options.open(update_path)?;
        let pending_queue = env.create_database(Some("pending-queue"))?;
        let next_update_id = env.create_database(Some("next-update-id"))?;
        let updates = env.create_database(Some("updates"))?;

        let state = Arc::new(StateLock::from_state(State::Idle));

        let (notification_sender, notification_receiver) = mpsc::channel(1);

        Ok((
            Self {
                env,
                pending_queue,
                next_update_id,
                updates,
                state,
                notification_sender,
                path: path.as_ref().to_owned(),
                update_file_store,
            },
            notification_receiver,
        ))
    }

    pub fn open<U, I>(
        options: EnvOpenOptions,
        path: impl AsRef<Path>,
        index_resolver: Arc<IndexResolver<U, I>>,
        must_exit: Arc<AtomicBool>,
        update_file_store: UpdateFileStore,
    ) -> anyhow::Result<Arc<Self>>
    where
        U: UuidStore + Sync + Send + 'static,
        I: IndexStore + Sync + Send + 'static,
    {
        let (update_store, mut notification_receiver) =
            Self::new(options, path, update_file_store)?;
        let update_store = Arc::new(update_store);

        // Send a first notification to trigger the process.
        if let Err(TrySendError::Closed(())) = update_store.notification_sender.try_send(()) {
            panic!("Failed to init update store");
        }

        // We need a weak reference so we can take ownership on the arc later when we
        // want to close the index.
        let duration = Duration::from_secs(10 * 60); // 10 minutes
        let update_store_weak = Arc::downgrade(&update_store);
        tokio::task::spawn_local(async move {
            // Block and wait for something to process with a timeout. The timeout
            // function returns a Result and we must just unlock the loop on Result.
            'outer: while timeout(duration, notification_receiver.recv())
                .await
                .map_or(true, |o| o.is_some())
            {
                loop {
                    match update_store_weak.upgrade() {
                        Some(update_store) => {
                            let handler = index_resolver.clone();
                            let res = tokio::task::spawn_blocking(move || {
                                update_store.process_pending_update(handler)
                            })
                            .await
                            .expect("Fatal error processing update.");
                            match res {
                                Ok(Some(_)) => (),
                                Ok(None) => break,
                                Err(e) => {
                                    error!("Fatal error while processing an update that requires the update store to shutdown: {}", e);
                                    must_exit.store(true, Ordering::SeqCst);
                                    break 'outer;
                                }
                            }
                        }
                        // the ownership on the arc has been taken, we need to exit.
                        None => break 'outer,
                    }
                }
            }

            error!("Update store loop exited.");
        });

        Ok(update_store)
    }

    /// Returns the next global update id and the next update id for a given `index_uuid`.
    fn next_update_id(&self, txn: &mut heed::RwTxn, index_uuid: Uuid) -> heed::Result<(u64, u64)> {
        let global_id = self
            .next_update_id
            .get(txn, &NextIdKey::Global)?
            .map(U64::get)
            .unwrap_or_default();

        self.next_update_id
            .put(txn, &NextIdKey::Global, &BEU64::new(global_id + 1))?;

        let update_id = self.next_update_id_raw(txn, index_uuid)?;

        Ok((global_id, update_id))
    }

    /// Returns the next next update id for a given `index_uuid` without
    /// incrementing the global update id. This is useful for the dumps.
    fn next_update_id_raw(&self, txn: &mut heed::RwTxn, index_uuid: Uuid) -> heed::Result<u64> {
        let update_id = self
            .next_update_id
            .get(txn, &NextIdKey::Index(index_uuid))?
            .map(U64::get)
            .unwrap_or_default();

        self.next_update_id.put(
            txn,
            &NextIdKey::Index(index_uuid),
            &BEU64::new(update_id + 1),
        )?;

        Ok(update_id)
    }

    /// Registers the update content in the pending store and the meta
    /// into the pending-meta store. Returns the new unique update id.
    pub fn register_update(&self, index_uuid: Uuid, update: Update) -> heed::Result<Enqueued> {
        let mut txn = self.env.write_txn()?;
        let (global_id, update_id) = self.next_update_id(&mut txn, index_uuid)?;
        let meta = Enqueued::new(update, update_id);

        self.pending_queue
            .put(&mut txn, &(global_id, index_uuid, update_id), &meta)?;

        txn.commit()?;

        if let Err(TrySendError::Closed(())) = self.notification_sender.try_send(()) {
            panic!("Update store loop exited");
        }

        Ok(meta)
    }

    /// Push already processed update in the UpdateStore without triggering the notification
    /// process. This is useful for the dumps.
    pub fn register_raw_updates(
        &self,
        wtxn: &mut heed::RwTxn,
        update: &UpdateStatus,
        index_uuid: Uuid,
    ) -> heed::Result<()> {
        match update {
            UpdateStatus::Enqueued(enqueued) => {
                let (global_id, _update_id) = self.next_update_id(wtxn, index_uuid)?;
                self.pending_queue.remap_key_type::<PendingKeyCodec>().put(
                    wtxn,
                    &(global_id, index_uuid, enqueued.id()),
                    enqueued,
                )?;
            }
            _ => {
                let _update_id = self.next_update_id_raw(wtxn, index_uuid)?;
                self.updates.put(wtxn, &(index_uuid, update.id()), update)?;
            }
        }
        Ok(())
    }

    /// Executes the user provided function on the next pending update (the one with the lowest id).
    /// This is asynchronous as it let the user process the update with a read-only txn and
    /// only writing the result meta to the processed-meta store *after* it has been processed.
    fn process_pending_update<U, I>(
        &self,
        index_resolver: Arc<IndexResolver<U, I>>,
    ) -> Result<Option<()>>
    where
        U: UuidStore + Sync + Send + 'static,
        I: IndexStore + Sync + Send + 'static,
    {
        // Create a read transaction to be able to retrieve the pending update in order.
        let rtxn = self.env.read_txn()?;
        let first_meta = self.pending_queue.first(&rtxn)?;
        drop(rtxn);

        // If there is a pending update we process and only keep
        // a reader while processing it, not a writer.
        match first_meta {
            Some(((global_id, index_uuid, _), pending)) => {
                let processing = pending.processing();
                // Acquire the state lock and set the current state to processing.
                // txn must *always* be acquired after state lock, or it will dead lock.
                let state = self.state.write();
                state.swap(State::Processing(index_uuid, processing.clone()));

                let result = self.perform_update(processing, index_resolver, index_uuid, global_id);

                state.swap(State::Idle);

                result
            }
            None => Ok(None),
        }
    }

    fn perform_update<U, I>(
        &self,
        processing: Processing,
        index_resolver: Arc<IndexResolver<U, I>>,
        index_uuid: Uuid,
        global_id: u64,
    ) -> Result<Option<()>>
    where
        U: UuidStore + Sync + Send + 'static,
        I: IndexStore + Sync + Send + 'static,
    {
        // Process the pending update using the provided user function.
        let handle = Handle::current();
        let update_id = processing.id();
        //IndexMsg::update(index_resolver, index_uuid, processing.clone()
        let result = match handle.block_on(index_resolver.get_index_by_uuid(index_uuid)) {
            Ok(index) => index.handle_update(processing),
            Err(e) => Err(processing.fail(e)),
        };

        // Once the pending update have been successfully processed
        // we must remove the content from the pending and processing stores and
        // write the *new* meta to the processed-meta store and commit.
        let mut wtxn = self.env.write_txn()?;
        self.pending_queue
            .delete(&mut wtxn, &(global_id, index_uuid, update_id))?;

        let result = match result {
            Ok(res) => res.into(),
            Err(res) => res.into(),
        };

        self.updates
            .put(&mut wtxn, &(index_uuid, update_id), &result)?;

        wtxn.commit()?;

        Ok(Some(()))
    }

    /// List the updates for `index_uuid`.
    pub fn list(&self, index_uuid: Uuid) -> Result<Vec<UpdateStatus>> {
        let mut update_list = BTreeMap::<u64, UpdateStatus>::new();

        let txn = self.env.read_txn()?;

        let pendings = self.pending_queue.iter(&txn)?.lazily_decode_data();
        for entry in pendings {
            let ((_, uuid, id), pending) = entry?;
            if uuid == index_uuid {
                update_list.insert(id, pending.decode()?.into());
            }
        }

        let updates = self
            .updates
            .remap_key_type::<ByteSlice>()
            .prefix_iter(&txn, index_uuid.as_bytes())?;

        for entry in updates {
            let (_, update) = entry?;
            update_list.insert(update.id(), update);
        }

        // If the currently processing update is from this index, replace the corresponding pending update with this one.
        match *self.state.read() {
            State::Processing(uuid, ref processing) if uuid == index_uuid => {
                update_list.insert(processing.id(), processing.clone().into());
            }
            _ => (),
        }

        Ok(update_list.into_iter().map(|(_, v)| v).collect())
    }

    /// Returns the update associated meta or `None` if the update doesn't exist.
    pub fn meta(&self, index_uuid: Uuid, update_id: u64) -> heed::Result<Option<UpdateStatus>> {
        // Check if the update is the one currently processing
        match *self.state.read() {
            State::Processing(uuid, ref processing)
                if uuid == index_uuid && processing.id() == update_id =>
            {
                return Ok(Some(processing.clone().into()));
            }
            _ => (),
        }

        let txn = self.env.read_txn()?;
        // Else, check if it is in the updates database:
        let update = self.updates.get(&txn, &(index_uuid, update_id))?;

        if let Some(update) = update {
            return Ok(Some(update));
        }

        // If nothing was found yet, we resolve to iterate over the pending queue.
        let pendings = self.pending_queue.iter(&txn)?.lazily_decode_data();

        for entry in pendings {
            let ((_, uuid, id), pending) = entry?;
            if uuid == index_uuid && id == update_id {
                return Ok(Some(pending.decode()?.into()));
            }
        }

        // No update was found.
        Ok(None)
    }

    /// Delete all updates for an index from the update store. If the currently processing update
    /// is for `index_uuid`, the call will block until the update is terminated.
    pub fn delete_all(&self, index_uuid: Uuid) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        // Contains all the content file paths that we need to be removed if the deletion was successful.
        let mut uuids_to_remove = Vec::new();

        let mut pendings = self.pending_queue.iter_mut(&mut txn)?.lazily_decode_data();

        while let Some(Ok(((_, uuid, _), pending))) = pendings.next() {
            if uuid == index_uuid {
                let pending = pending.decode()?;
                if let Update::DocumentAddition { content_uuid, .. } = pending.meta() {
                    uuids_to_remove.push(*content_uuid);
                }

                //Invariant check: we can only delete the current entry when we don't hold
                //references to it anymore. This must be done after we have retrieved its content.
                unsafe {
                    pendings.del_current()?;
                }
            }
        }

        drop(pendings);

        let mut updates = self
            .updates
            .remap_key_type::<ByteSlice>()
            .prefix_iter_mut(&mut txn, index_uuid.as_bytes())?
            .lazily_decode_data();

        while let Some(_) = updates.next() {
            unsafe {
                updates.del_current()?;
            }
        }

        drop(updates);

        txn.commit()?;

        // If the currently processing update is from our index, we wait until it is
        // finished before returning. This ensure that no write to the index occurs after we delete it.
        if let State::Processing(uuid, _) = *self.state.read() {
            if uuid == index_uuid {
                // wait for a write lock, do nothing with it.
                self.state.write();
            }
        }

        // Finally, remove any outstanding update files. This must be done after waiting for the
        // last update to ensure that the update files are not deleted before the update needs
        // them.
        uuids_to_remove.iter().for_each(|uuid| {
            let _ = self.update_file_store.delete(*uuid);
        });

        Ok(())
    }

    pub fn snapshot(&self, indexes: Vec<Index>, path: impl AsRef<Path>) -> Result<()> {
        let state_lock = self.state.write();
        state_lock.swap(State::Snapshoting);

        let txn = self.env.write_txn()?;

        let update_path = path.as_ref().join("updates");
        create_dir_all(&update_path)?;

        // acquire write lock to prevent further writes during snapshot
        create_dir_all(&update_path)?;
        let db_path = update_path.join("data.mdb");

        // create db snapshot
        self.env.copy_to_path(&db_path, CompactionOption::Enabled)?;

        let pendings = self.pending_queue.iter(&txn)?.lazily_decode_data();

        let uuids: HashSet<_> = indexes.iter().map(|i| i.uuid()).collect();
        for entry in pendings {
            let ((_, uuid, _), pending) = entry?;
            if uuids.contains(&uuid) {
                if let Enqueued {
                    meta: Update::DocumentAddition { content_uuid, .. },
                    ..
                } = pending.decode()?
                {
                    self.update_file_store.snapshot(content_uuid, &path)?;
                }
            }
        }

        let path = path.as_ref().to_owned();
        indexes
            .par_iter()
            .try_for_each(|index| index.snapshot(&path))?;

        Ok(())
    }

    pub fn get_info(&self) -> Result<UpdateStoreInfo> {
        let mut size = self.env.size();
        let txn = self.env.read_txn()?;
        for entry in self.pending_queue.iter(&txn)? {
            let (_, pending) = entry?;
            if let Enqueued {
                meta: store::Update::DocumentAddition { content_uuid, .. },
                ..
            } = pending
            {
                let len = self.update_file_store.get_size(content_uuid)?;
                size += len;
            }
        }
        let processing = match *self.state.read() {
            State::Processing(uuid, _) => Some(uuid),
            _ => None,
        };

        Ok(UpdateStoreInfo { size, processing })
    }
}

#[cfg(test)]
mod test {
    use futures::future::ok;
    use mockall::predicate::eq;

    use crate::index::error::IndexError;
    use crate::index::test::Mocker;
    use crate::index_controller::index_resolver::index_store::MockIndexStore;
    use crate::index_controller::index_resolver::uuid_store::MockUuidStore;
    use crate::index_controller::updates::status::{Failed, Processed};

    use super::*;

    #[actix_rt::test]
    async fn test_next_id() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let mut options = EnvOpenOptions::new();
        let index_store = MockIndexStore::new();
        let uuid_store = MockUuidStore::new();
        let index_resolver = IndexResolver::new(uuid_store, index_store);
        let update_file_store = UpdateFileStore::new(dir.path()).unwrap();
        options.map_size(4096 * 100);
        let update_store = UpdateStore::open(
            options,
            dir.path(),
            Arc::new(index_resolver),
            Arc::new(AtomicBool::new(false)),
            update_file_store,
        )
        .unwrap();

        let index1_uuid = Uuid::new_v4();
        let index2_uuid = Uuid::new_v4();

        let mut txn = update_store.env.write_txn().unwrap();
        let ids = update_store.next_update_id(&mut txn, index1_uuid).unwrap();
        txn.commit().unwrap();
        assert_eq!((0, 0), ids);

        let mut txn = update_store.env.write_txn().unwrap();
        let ids = update_store.next_update_id(&mut txn, index2_uuid).unwrap();
        txn.commit().unwrap();
        assert_eq!((1, 0), ids);

        let mut txn = update_store.env.write_txn().unwrap();
        let ids = update_store.next_update_id(&mut txn, index1_uuid).unwrap();
        txn.commit().unwrap();
        assert_eq!((2, 1), ids);
    }

    #[actix_rt::test]
    async fn test_register_update() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let index_store = MockIndexStore::new();
        let uuid_store = MockUuidStore::new();
        let index_resolver = IndexResolver::new(uuid_store, index_store);
        let update_file_store = UpdateFileStore::new(dir.path()).unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(4096 * 100);
        let update_store = UpdateStore::open(
            options,
            dir.path(),
            Arc::new(index_resolver),
            Arc::new(AtomicBool::new(false)),
            update_file_store,
        )
        .unwrap();
        let update = Update::ClearDocuments;
        let uuid = Uuid::new_v4();
        let store_clone = update_store.clone();
        tokio::task::spawn_blocking(move || {
            store_clone.register_update(uuid, update).unwrap();
        })
        .await
        .unwrap();

        let txn = update_store.env.read_txn().unwrap();
        assert!(update_store
            .pending_queue
            .get(&txn, &(0, uuid, 0))
            .unwrap()
            .is_some());
    }

    #[actix_rt::test]
    async fn test_process_update_success() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let index_uuid = Uuid::new_v4();

        let mut index_store = MockIndexStore::new();
        index_store
            .expect_get()
            .with(eq(index_uuid))
            .returning(|_uuid| {
                let mocker = Mocker::default();
                mocker
                    .when::<Processing, std::result::Result<Processed, Failed>>("handle_update")
                    .once()
                    .then(|update| Ok(update.process(status::UpdateResult::Other)));

                Box::pin(ok(Some(Index::faux(mocker))))
            });

        let uuid_store = MockUuidStore::new();
        let index_resolver = Arc::new(IndexResolver::new(uuid_store, index_store));

        let update_file_store = UpdateFileStore::new(dir.path()).unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(4096 * 100);
        let store = UpdateStore::open(
            options,
            dir.path(),
            index_resolver.clone(),
            Arc::new(AtomicBool::new(false)),
            update_file_store,
        )
        .unwrap();

        // wait a bit for the event loop exit.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut txn = store.env.write_txn().unwrap();

        let update = Enqueued::new(Update::ClearDocuments, 0);

        store
            .pending_queue
            .put(&mut txn, &(0, index_uuid, 0), &update)
            .unwrap();

        txn.commit().unwrap();

        // Process the pending, and check that it has been moved to the update databases, and
        // removed from the pending database.
        let store_clone = store.clone();
        tokio::task::spawn_blocking(move || {
            store_clone.process_pending_update(index_resolver).unwrap();
        })
        .await
        .unwrap();

        let txn = store.env.read_txn().unwrap();

        assert!(store.pending_queue.first(&txn).unwrap().is_none());
        let update = store.updates.get(&txn, &(index_uuid, 0)).unwrap().unwrap();

        assert!(matches!(update, UpdateStatus::Processed(_)));
    }

    #[actix_rt::test]
    async fn test_process_update_failure() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let index_uuid = Uuid::new_v4();

        let mut index_store = MockIndexStore::new();
        index_store
            .expect_get()
            .with(eq(index_uuid))
            .returning(|_uuid| {
                let mocker = Mocker::default();
                mocker
                    .when::<Processing, std::result::Result<Processed, Failed>>("handle_update")
                    .once()
                    .then(|update| Err(update.fail(IndexError::DocumentNotFound("1".to_string()))));

                Box::pin(ok(Some(Index::faux(mocker))))
            });

        let uuid_store = MockUuidStore::new();
        let index_resolver = Arc::new(IndexResolver::new(uuid_store, index_store));

        let update_file_store = UpdateFileStore::new(dir.path()).unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(4096 * 100);
        let store = UpdateStore::open(
            options,
            dir.path(),
            index_resolver.clone(),
            Arc::new(AtomicBool::new(false)),
            update_file_store,
        )
        .unwrap();

        // wait a bit for the event loop exit.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut txn = store.env.write_txn().unwrap();

        let update = Enqueued::new(Update::ClearDocuments, 0);

        store
            .pending_queue
            .put(&mut txn, &(0, index_uuid, 0), &update)
            .unwrap();

        txn.commit().unwrap();

        // Process the pending, and check that it has been moved to the update databases, and
        // removed from the pending database.
        let store_clone = store.clone();
        tokio::task::spawn_blocking(move || {
            store_clone.process_pending_update(index_resolver).unwrap();
        })
        .await
        .unwrap();

        let txn = store.env.read_txn().unwrap();

        assert!(store.pending_queue.first(&txn).unwrap().is_none());
        let update = store.updates.get(&txn, &(index_uuid, 0)).unwrap().unwrap();

        assert!(matches!(update, UpdateStatus::Failed(_)));
    }
}

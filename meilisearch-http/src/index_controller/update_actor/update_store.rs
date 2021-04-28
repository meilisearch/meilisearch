use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;
use std::fs::{copy, create_dir_all, remove_file, File};
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use heed::types::{ByteSlice, OwnedType, SerdeJson};
use heed::zerocopy::U64;
use heed::{BytesDecode, BytesEncode, CompactionOption, Database, Env, EnvOpenOptions};
use parking_lot::{Mutex, MutexGuard};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::UpdateMeta;
use crate::helpers::EnvSizer;
use crate::index_controller::{IndexActorHandle, updates::*};

#[allow(clippy::upper_case_acronyms)]
type BEU64 = U64<heed::byteorder::BE>;

struct NextIdCodec;

enum NextIdKey {
    Global,
    Index(Uuid),
}

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

struct StateLockGuard<'a> {
    _lock: MutexGuard<'a, ()>,
    state: &'a StateLock,
}

impl StateLockGuard<'_> {
    fn swap(&self, state: State) -> Arc<State> {
        self.state.data.swap(Arc::new(state))
    }
}

impl StateLock {
    fn from_state(state: State) -> Self {
        let lock = Mutex::new(());
        let data = ArcSwap::from(Arc::new(state));
        Self { lock, data }
    }

    fn read(&self) -> Arc<State> {
        self.data.load().clone()
    }

    fn write(&self) -> StateLockGuard {
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
}

impl<'a> BytesEncode<'a> for NextIdCodec {
    type EItem = NextIdKey;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        match item {
            NextIdKey::Global => Some(Cow::Borrowed(b"__global__")),
            NextIdKey::Index(ref uuid) => Some(Cow::Borrowed(uuid.as_bytes())),
        }
    }
}

struct PendingKeyCodec;

impl<'a> BytesEncode<'a> for PendingKeyCodec {
    type EItem = (u64, Uuid, u64);

    fn bytes_encode((global_id, uuid, update_id): &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut bytes = Vec::with_capacity(size_of::<Self::EItem>());
        bytes.extend_from_slice(&global_id.to_be_bytes());
        bytes.extend_from_slice(uuid.as_bytes());
        bytes.extend_from_slice(&update_id.to_be_bytes());
        Some(Cow::Owned(bytes))
    }
}

impl<'a> BytesDecode<'a> for PendingKeyCodec {
    type DItem = (u64, Uuid, u64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let global_id_bytes = bytes.get(0..size_of::<u64>())?.try_into().ok()?;
        let global_id = u64::from_be_bytes(global_id_bytes);

        let uuid_bytes = bytes
            .get(size_of::<u64>()..(size_of::<u64>() + size_of::<Uuid>()))?
            .try_into()
            .ok()?;
        let uuid = Uuid::from_bytes(uuid_bytes);

        let update_id_bytes = bytes
            .get((size_of::<u64>() + size_of::<Uuid>())..)?
            .try_into()
            .ok()?;
        let update_id = u64::from_be_bytes(update_id_bytes);

        Some((global_id, uuid, update_id))
    }
}

struct UpdateKeyCodec;

impl<'a> BytesEncode<'a> for UpdateKeyCodec {
    type EItem = (Uuid, u64);

    fn bytes_encode((uuid, update_id): &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut bytes = Vec::with_capacity(size_of::<Self::EItem>());
        bytes.extend_from_slice(uuid.as_bytes());
        bytes.extend_from_slice(&update_id.to_be_bytes());
        Some(Cow::Owned(bytes))
    }
}

impl<'a> BytesDecode<'a> for UpdateKeyCodec {
    type DItem = (Uuid, u64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let uuid_bytes = bytes.get(0..size_of::<Uuid>())?.try_into().ok()?;
        let uuid = Uuid::from_bytes(uuid_bytes);

        let update_id_bytes = bytes.get(size_of::<Uuid>()..)?.try_into().ok()?;
        let update_id = u64::from_be_bytes(update_id_bytes);

        Some((uuid, update_id))
    }
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
    updates: Database<ByteSlice, SerdeJson<UpdateStatus>>,
    /// Indicates the current state of the update store,
    state: Arc<StateLock>,
    /// Wake up the loop when a new event occurs.
    notification_sender: mpsc::Sender<()>,
}

impl UpdateStore {
    pub fn open(
        mut options: EnvOpenOptions,
        path: impl AsRef<Path>,
        index_handle: impl IndexActorHandle + Clone + Sync + Send + 'static,
    ) -> anyhow::Result<Arc<Self>> {
        options.max_dbs(5);

        let env = options.open(path)?;
        let pending_queue = env.create_database(Some("pending-queue"))?;
        let next_update_id = env.create_database(Some("next-update-id"))?;
        let updates = env.create_database(Some("updates"))?;

        let (notification_sender, mut notification_receiver) = mpsc::channel(10);
        // Send a first notification to trigger the process.
        let _ = notification_sender.send(());

        let state = Arc::new(StateLock::from_state(State::Idle));

        // Init update loop to perform any pending updates at launch.
        // Since we just launched the update store, and we still own the receiving end of the
        // channel, this call is guaranteed to succeed.
        notification_sender
            .try_send(())
            .expect("Failed to init update store");

        let update_store = Arc::new(UpdateStore { env, pending_queue, next_update_id, updates, state, notification_sender });

        // We need a weak reference so we can take ownership on the arc later when we
        // want to close the index.
        let update_store_weak = Arc::downgrade(&update_store);
        tokio::task::spawn(async move {
            // Block and wait for something to process.
            'outer: while notification_receiver.recv().await.is_some() {
                loop {
                    match update_store_weak.upgrade() {
                        Some(update_store) => {
                            let handler = index_handle.clone();
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

    /// Returns the next global update id and the next update id for a given `index_uuid`.
    fn next_update_id(&self, txn: &mut heed::RwTxn, index_uuid: Uuid) -> heed::Result<(u64, u64)> {
        let global_id = self
            .next_update_id
            .get(txn, &NextIdKey::Global)?
            .map(U64::get)
            .unwrap_or_default();
        let update_id = self
            .next_update_id
            .get(txn, &NextIdKey::Index(index_uuid))?
            .map(U64::get)
            .unwrap_or_default();

        self.next_update_id
            .put(txn, &NextIdKey::Global, &BEU64::new(global_id + 1))?;
        self.next_update_id.put(
            txn,
            &NextIdKey::Index(index_uuid),
            &BEU64::new(update_id + 1),
        )?;

        Ok((global_id, update_id))
    }

    /// Registers the update content in the pending store and the meta
    /// into the pending-meta store. Returns the new unique update id.
    pub fn register_update(
        &self,
        meta: UpdateMeta,
        content: Option<impl AsRef<Path>>,
        index_uuid: Uuid,
    ) -> heed::Result<Enqueued> {
        let mut txn = self.env.write_txn()?;

        let (global_id, update_id) = self.next_update_id(&mut txn, index_uuid)?;
        let meta = Enqueued::new(meta, update_id, content.map(|p| p.as_ref().to_owned()));

        self.pending_queue
            .put(&mut txn, &(global_id, index_uuid, update_id), &meta)?;

        txn.commit()?;

        self.notification_sender
            .blocking_send(())
            .expect("Update store loop exited.");
        Ok(meta)
    }

    /// Executes the user provided function on the next pending update (the one with the lowest id).
    /// This is asynchronous as it let the user process the update with a read-only txn and
    /// only writing the result meta to the processed-meta store *after* it has been processed.
    fn process_pending_update(
        &self,
        index_handle: impl IndexActorHandle,
    ) -> anyhow::Result<Option<()>> {
        // Create a read transaction to be able to retrieve the pending update in order.
        let rtxn = self.env.read_txn()?;
        let first_meta = self.pending_queue.first(&rtxn)?;
        drop(rtxn);

        // If there is a pending update we process and only keep
        // a reader while processing it, not a writer.
        match first_meta {
            Some(((global_id, index_uuid, update_id), mut pending)) => {
                let content_path = pending.content.take();
                let processing = pending.processing();

                // Acquire the state lock and set the current state to processing.
                let state = self.state.write();
                state.swap(State::Processing(index_uuid, processing.clone()));

                let file = match content_path {
                    Some(ref path) => {
                        let file = File::open(path)
                            .with_context(|| format!("file at path: {:?}", &content_path))?;
                        Some(file)
                    }
                    None => None,
                };
                // Process the pending update using the provided user function.
                let result = Handle::current()
                    .block_on(index_handle.update(index_uuid, processing, file))?;

                // Once the pending update have been successfully processed
                // we must remove the content from the pending and processing stores and
                // write the *new* meta to the processed-meta store and commit.
                let mut wtxn = self.env.write_txn()?;
                self.pending_queue
                    .delete(&mut wtxn, &(global_id, index_uuid, update_id))?;

                if let Some(path) = content_path {
                    remove_file(&path)?;
                }

                let result = match result {
                    Ok(res) => res.into(),
                    Err(res) => res.into(),
                };

                self.updates.remap_key_type::<UpdateKeyCodec>().put(
                    &mut wtxn,
                    &(index_uuid, update_id),
                    &result,
                )?;

                wtxn.commit()?;
                state.swap(State::Idle);

                Ok(Some(()))
            }
            None => Ok(None),
        }
    }

    /// List the updates for `index_uuid`.
    pub fn list(&self, index_uuid: Uuid) -> anyhow::Result<Vec<UpdateStatus>> {
        let mut update_list = BTreeMap::<u64, UpdateStatus>::new();

        let txn = self.env.read_txn()?;

        let pendings = self.pending_queue.iter(&txn)?.lazily_decode_data();
        for entry in pendings {
            let ((_, uuid, id), pending) = entry?;
            if uuid == index_uuid {
                update_list.insert(id, pending.decode()?.into());
            }
        }

        let updates = self.updates.prefix_iter(&txn, index_uuid.as_bytes())?;
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
        let update = self
            .updates
            .remap_key_type::<UpdateKeyCodec>()
            .get(&txn, &(index_uuid, update_id))?;

        if let Some(update) = update {
            return Ok(Some(update));
        }

        // If nothing was found yet, we resolve to iterate over the pending queue.
        let pendings = self
            .pending_queue
            .remap_key_type::<UpdateKeyCodec>()
            .iter(&txn)?
            .lazily_decode_data();

        for entry in pendings {
            let ((uuid, id), pending) = entry?;
            if uuid == index_uuid && id == update_id {
                return Ok(Some(pending.decode()?.into()));
            }
        }

        // No update was found.
        Ok(None)
    }

    /// Delete all updates for an index from the update store.
    pub fn delete_all(&self, index_uuid: Uuid) -> anyhow::Result<()> {
        let mut txn = self.env.write_txn()?;
        // Contains all the content file paths that we need to be removed if the deletion was successful.
        let mut paths_to_remove = Vec::new();

        let mut pendings = self.pending_queue.iter_mut(&mut txn)?.lazily_decode_data();

        while let Some(Ok(((_, uuid, _), pending))) = pendings.next() {
            if uuid == index_uuid {
                pendings.del_current()?;
                let mut pending = pending.decode()?;
                if let Some(path) = pending.content.take() {
                    paths_to_remove.push(path);
                }
            }
        }

        drop(pendings);

        let mut updates = self
            .updates
            .prefix_iter_mut(&mut txn, index_uuid.as_bytes())?
            .lazily_decode_data();

        while let Some(_) = updates.next() {
            updates.del_current()?;
        }

        drop(updates);

        txn.commit()?;

        paths_to_remove.iter().for_each(|path| {
            let _ = remove_file(path);
        });

        // We don't care about the currently processing update, since it will be removed by itself
        // once its done processing, and we can't abort a running update.

        Ok(())
    }

    pub fn snapshot(&self, uuids: &HashSet<Uuid>, path: impl AsRef<Path>) -> anyhow::Result<()> {
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

        let update_files_path = update_path.join("update_files");
        create_dir_all(&update_files_path)?;

        let pendings = self.pending_queue.iter(&txn)?.lazily_decode_data();

        for entry in pendings {
            let ((_, uuid, _), pending) = entry?;
            if uuids.contains(&uuid) {
                if let Some(path) = pending.decode()?.content_path() {
                    let name = path.file_name().unwrap();
                    let to = update_files_path.join(name);
                    copy(path, to)?;
                }
            }
        }

        Ok(())
    }

    pub fn dump(
        &self,
        txn: &mut heed::RwTxn,
        path: impl AsRef<Path>,
        uuid: Uuid,
    ) -> anyhow::Result<()> {
        let update_path = path.as_ref().join("updates");
        create_dir_all(&update_path)?;

        let mut dump_path = update_path.join(format!("update-{}", uuid));
        // acquire write lock to prevent further writes during dump
        create_dir_all(&dump_path)?;
        dump_path.push("data.mdb");

        // create db dump
        self.env.copy_to_path(&dump_path, CompactionOption::Enabled)?;

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

    pub fn get_info(&self) -> anyhow::Result<UpdateStoreInfo> {
        let mut size = self.env.size();
        let txn = self.env.read_txn()?;
        for entry in self.pending_queue.iter(&txn)? {
            let (_, pending) = entry?;
            if let Some(path) = pending.content_path() {
                size += File::open(path)?.metadata()?.len();
            }
        }
        let processing = match *self.state.read() {
            State::Processing(uuid, _) => Some(uuid),
            _ => None,
        };

        Ok(UpdateStoreInfo { size, processing })
    }

    pub fn get_size(&self, txn: &heed::RoTxn) -> anyhow::Result<u64> {
        let mut size = self.env.size();
        let txn = self.env.read_txn()?;

        for entry in self.pending_queue.iter(&txn)? {
            let (_, pending) = entry?;
            if let Some(path) = pending.content_path() {
                size += File::open(path)?.metadata()?.len();
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
    use super::*;
    use crate::index_controller::{index_actor::MockIndexActorHandle, UpdateResult};

    use futures::future::ok;

    #[actix_rt::test]
    async fn test_next_id() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let mut options = EnvOpenOptions::new();
        let handle = Arc::new(MockIndexActorHandle::new());
        options.map_size(4096 * 100);
        let update_store = UpdateStore::open(options, dir.path(), handle).unwrap();

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
        let mut options = EnvOpenOptions::new();
        let handle = Arc::new(MockIndexActorHandle::new());
        options.map_size(4096 * 100);
        let update_store = UpdateStore::open(options, dir.path(), handle).unwrap();
        let meta = UpdateMeta::ClearDocuments;
        let uuid = Uuid::new_v4();
        let store_clone = update_store.clone();
        tokio::task::spawn_blocking(move || {
            store_clone
                .register_update(meta, Some("here"), uuid)
                .unwrap();
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
    async fn test_process_update() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let mut handle = MockIndexActorHandle::new();

        handle
            .expect_update()
            .times(2)
            .returning(|_index_uuid, processing, _file| {
                if processing.id() == 0 {
                    Box::pin(ok(Ok(processing.process(UpdateResult::Other))))
                } else {
                    Box::pin(ok(Err(processing.fail(String::from("err")))))
                }
            });

        let handle = Arc::new(handle);

        let mut options = EnvOpenOptions::new();
        options.map_size(4096 * 100);
        let store = UpdateStore::open(options, dir.path(), handle.clone()).unwrap();

        // wait a bit for the event loop exit.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut txn = store.env.write_txn().unwrap();

        let update = Enqueued::new(UpdateMeta::ClearDocuments, 0, None);
        let uuid = Uuid::new_v4();

        store
            .pending_queue
            .put(&mut txn, &(0, uuid, 0), &update)
            .unwrap();

        let update = Enqueued::new(UpdateMeta::ClearDocuments, 1, None);

        store
            .pending_queue
            .put(&mut txn, &(1, uuid, 1), &update)
            .unwrap();

        txn.commit().unwrap();

        // Process the pending, and check that it has been moved to the update databases, and
        // removed from the pending database.
        let store_clone = store.clone();
        tokio::task::spawn_blocking(move || {
            store_clone.process_pending_update(handle.clone()).unwrap();
            store_clone.process_pending_update(handle).unwrap();
        })
        .await
        .unwrap();

        let txn = store.env.read_txn().unwrap();

        assert!(store.pending_queue.first(&txn).unwrap().is_none());
        let update = store
            .updates
            .remap_key_type::<UpdateKeyCodec>()
            .get(&txn, &(uuid, 0))
            .unwrap()
            .unwrap();

        assert!(matches!(update, UpdateStatus::Processed(_)));
        let update = store
            .updates
            .remap_key_type::<UpdateKeyCodec>()
            .get(&txn, &(uuid, 1))
            .unwrap()
            .unwrap();

        assert!(matches!(update, UpdateStatus::Failed(_)));
    }
}

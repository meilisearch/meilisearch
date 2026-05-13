use std::collections::HashMap;
use std::env::VarError;
use std::io::{self, ErrorKind};
use std::ops::Not as _;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::oneshot;
use futures::future::Shared;
use futures::FutureExt;
use hashbrown::HashSet;
use meilisearch_types::heed::{EnvClosingEvent, EnvFlags, EnvOpenOptions};
use meilisearch_types::milli::{CreateOrOpen, Index, Result};
use time::OffsetDateTime;
use tokio::runtime;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tracing::warn;
use uuid::Uuid;

use super::IndexStatus::{self, Available, BeingDeleted, Closing, Missing};
use crate::clamp_to_page_size;
use crate::lru::{InsertionOutcome, LruMap};

type IndexTransferCompletion = Shared<oneshot::Receiver<Result<(), Arc<io::Error>>>>;
type IndexTransferCompletionNotifier = oneshot::Sender<Result<(), Arc<io::Error>>>;

/// Keep an internally consistent view of the open indexes in memory.
///
/// This view is made of an LRU cache that will evict the least frequently used indexes when new indexes are opened.
/// Indexes that are being closed (for resizing or due to cache eviction) or deleted cannot be evicted from the cache and
/// are stored separately.
///
/// This view provides operations to change the state of the index as it is known in memory:
/// open an index (making it available for queries), close an index (specifying the new size it should be opened with),
/// delete an index.
///
/// External consistency with the other bits of data of an index is provided by the `IndexMapper` parent structure.
pub struct IndexMap {
    /// A LRU map of indexes that are in the open state and available for queries.
    available: LruMap<Uuid, Index>,
    /// A map of indexes that are not available for queries, either because they are being deleted
    /// or because they are being closed.
    ///
    /// If they are being deleted, the UUID points to `None`.
    unavailable: LruMap<Uuid, ClosingIndex>,
    /// A set of indexes that have been deleted.
    deleting: HashSet<Uuid>,

    /// The set of indexes that are currently being uploaded and downloaded.
    ///
    /// One must wait for the upload to complete before opening an index that is being uploaded.
    /// Note that opening it after the upload has completed will block until the **download** completes.
    ///
    /// One must wait for the download to complete before opening an index that is being downloaded.
    transferring: HashMap<Uuid, IndexTransferCompletion>,

    /// The channel used to send index transfer requests to the transfer task.
    transfer_sender: Option<tokio::sync::mpsc::Sender<IndexTransferRequest>>,

    /// A monotonically increasing generation number, used to differentiate between multiple successive index closing requests.
    ///
    /// Because multiple readers could be waiting on an index to close, the following could theoretically happen:
    ///
    /// 1. Multiple readers wait for the index closing to occur.
    /// 2. One of them "wins the race", takes the lock and then removes the index that finished closing from the map.
    /// 3. The index is reopened, but must be closed again (such as being resized again).
    /// 4. One reader that "lost the race" in (2) wakes up and tries to take the lock and remove the index from the map.
    ///
    /// In that situation, the index may or may not have finished closing. The `generation` field allows to remember which
    /// closing request was made, so the reader that "lost the race" has the old generation and will need to wait again for the index
    /// to close.
    generation: usize,
}

/// The request sent to the transfer task to download or upload an index.
#[derive(Debug)]
pub enum IndexTransferRequest {
    /// Request to download an index to S3.
    Download { uuid: Uuid, answer: IndexTransferCompletionNotifier },
    /// Request to upload an index to S3.
    Upload { uuid: Uuid, answer: IndexTransferCompletionNotifier },
}

#[derive(Clone)]
pub enum TransferState {
    Downloading(IndexTransferCompletion),
    Uploading(IndexTransferCompletion),
}

#[derive(Clone)]
pub struct ClosingIndex {
    uuid: Uuid,
    closing_event: EnvClosingEvent,
    enable_mdb_writemap: bool,
    map_size: usize,
    generation: usize,
}

impl ClosingIndex {
    /// Waits for the index to be definitely closed.
    ///
    /// To avoid blocking, users should relinquish their locks to the IndexMap before calling this function.
    ///
    /// After the index is physically closed, the in memory map must still be updated to take this into account.
    /// To do so, a `ReopenableIndex` is returned, that can be used to either definitely close or definitely open
    /// the index without waiting anymore.
    pub fn wait_timeout(self, timeout: Duration) -> Option<ReopenableIndex> {
        self.closing_event.wait_timeout(timeout).then_some(ReopenableIndex {
            uuid: self.uuid,
            enable_mdb_writemap: self.enable_mdb_writemap,
            map_size: self.map_size,
            generation: self.generation,
        })
    }
}

pub struct ReopenableIndex {
    uuid: Uuid,
    enable_mdb_writemap: bool,
    map_size: usize,
    generation: usize,
}

impl ReopenableIndex {
    /// Attempts to reopen the index, which can result in the index being reopened again or not
    /// (e.g. if another thread already opened and closed the index again).
    ///
    /// Use get again on the IndexMap to get the updated status.
    ///
    /// Fails if the underlying index creation fails.
    ///
    /// # Status table
    ///
    /// | Previous Status | New Status                                   |
    /// |-----------------|----------------------------------------------|
    /// | Missing         | Missing                                      |
    /// | BeingDeleted    | BeingDeleted                                 |
    /// | Closing         | Available or Closing depending on generation |
    /// | Available       | Available                                    |
    ///
    pub async fn reopen(self, map: &mut IndexMap, path: &Path) -> Result<()> {
        if let Closing(reopen) = map.get(&self.uuid).await {
            if reopen.generation != self.generation {
                return Ok(());
            }
            map.unavailable.remove(&self.uuid);
            map.create(
                self.uuid,
                path,
                None,
                self.enable_mdb_writemap,
                self.map_size,
                CreateOrOpen::Open,
            )
            .await?;
        }
        Ok(())
    }

    /// Attempts to close the index, which may or may not result in the index being closed
    /// (e.g. if another thread already reopened the index again).
    ///
    /// Use get again on the IndexMap to get the updated status.
    ///
    /// # Status table
    ///
    /// | Previous Status | New Status                                 |
    /// |-----------------|--------------------------------------------|
    /// | Missing         | Missing                                    |
    /// | BeingDeleted    | BeingDeleted                               |
    /// | Closing         | Missing or Closing depending on generation |
    /// | Available       | Available                                  |
    pub async fn close(self, map: &mut IndexMap) {
        if let Closing(reopen) = map.get(&self.uuid).await {
            if reopen.generation != self.generation {
                return;
            }
            map.unavailable.remove(&self.uuid);
        }
    }
}

impl IndexMap {
    /// Creates a new `IndexMap` with the specified number of opened and on-disk indexes.
    ///
    /// If `runtime` is `None`, the transfer task will not be spawned and index offloading
    /// will not be supported.
    ///
    /// ## Panics
    ///
    /// Panics if `on_disk_cap` is smaller than `opened_cap`.
    pub fn new(
        opened_cap: usize,
        on_disk_cap: usize,
        runtime: Option<runtime::Handle>,
        indexes_folder: PathBuf,
    ) -> IndexMap {
        let unavailable = match on_disk_cap.checked_sub(opened_cap) {
            Some(unavailable) => unavailable,
            None => panic!(
                "on-disk capacity ({on_disk_cap}) must be greater than or equal to opened capacity ({opened_cap})"
            ),
        };

        let transfer_sender = match runtime {
            Some(runtime) => {
                // TODO why limiting to 10?
                let (transfer_sender, transfer_receiver) = tokio::sync::mpsc::channel(10);
                runtime.spawn(process_index_transfers(indexes_folder, transfer_receiver));
                Some(transfer_sender)
            }
            None => None,
        };

        Self {
            available: LruMap::new(opened_cap),
            unavailable: LruMap::new(unavailable),
            deleting: HashSet::default(),
            transferring: HashMap::default(),
            transfer_sender,
            generation: 0,
        }
    }

    /// Gets the current status of an index in the map.
    ///
    /// If the index is available it can be accessed from the returned status.
    pub async fn get(&self, uuid: &Uuid) -> IndexStatus {
        match self.available.get(uuid) {
            Some(index) => Available(index.clone()),
            None => self.get_unavailable(uuid).await,
        }
    }

    async fn get_unavailable(&self, uuid: &Uuid) -> IndexStatus {
        match self.unavailable.get(uuid) {
            Some(reopen) => Closing(reopen.clone()),
            None if self.deleting.get(uuid).is_some() => BeingDeleted,
            None => match self.transferring.get(uuid) {
                Some(_) => todo!(),
                None => Missing,
            },
        }
    }

    /// Attempts to create a new index that wasn't existing before.
    ///
    /// None is returned if the index is being downloaded.
    ///
    /// # Status table
    ///
    /// | Previous Status | New Status |
    /// |-----------------|------------|
    /// | Missing         | Available  |
    /// | BeingDeleted    | panics     |
    /// | Closing         | panics     |
    /// | Available       | panics     |
    ///
    pub async fn create(
        &mut self,
        uuid: Uuid,
        path: &Path,
        date: Option<(OffsetDateTime, OffsetDateTime)>,
        enable_mdb_writemap: bool,
        map_size: usize,
        create_or_open: CreateOrOpen,
    ) -> Result<Option<Index>> {
        if !matches!(self.get_unavailable(&uuid).await, Missing) {
            panic!("Attempt to open an index that was unavailable");
        }

        let index = match create_or_open {
            CreateOrOpen::Open => {
                match self.transfer_sender.as_ref() {
                    Some(transfer_sender) if path.try_exists()?.not() => {
                        // if index is not on disk, register a download task and wait for
                        // it to complete Warning: we must NOT wait for the download to
                        // complete here or we will block the entire index management system.
                        // We *MUST* return immediately and make the caller to find out
                        // that the index is being downloaded and wait for it to complete.
                        let (answer, receiver) = oneshot::channel();
                        // TODO do not unwrap
                        transfer_sender
                            .send(IndexTransferRequest::Download { uuid, answer })
                            .await
                            .unwrap();
                        if self.transferring.insert(uuid, receiver.shared()).is_some() {
                            panic!(
                                "Attempt to download an index that was already being transfered"
                            );
                        }
                        return Ok(None);
                    }
                    _ => create_or_open_index(
                        path,
                        date,
                        enable_mdb_writemap,
                        map_size,
                        create_or_open,
                    )?,
                }
            }
            create_or_open @ CreateOrOpen::Create { .. } => {
                create_or_open_index(path, date, enable_mdb_writemap, map_size, create_or_open)?
            }
        };

        match self.available.insert(uuid, index.clone()) {
            InsertionOutcome::InsertedNew => (),
            InsertionOutcome::Evicted(evicted_uuid, evicted_index) => {
                self.close(evicted_uuid, evicted_index, enable_mdb_writemap, 0);
            }
            InsertionOutcome::Replaced(_) => {
                panic!("Attempt to open an index that was already opened")
            }
        }

        Ok(Some(index))
    }

    /// Increases the current generation. See documentation for this field.
    ///
    /// In the unlikely event that the 2^64 generations would have been exhausted, we simply wrap-around.
    ///
    /// For this to cause an issue, one should be able to stop a reader in time after it got a `ReopenableIndex` and before it takes the lock
    /// to remove it from the unavailable map, and keep the reader in this frozen state for 2^64 closing of other indexes.
    ///
    /// This seems overwhelmingly impossible to achieve in practice.
    fn next_generation(&mut self) -> usize {
        self.generation = self.generation.wrapping_add(1);
        self.generation
    }

    /// Attempts to close an index.
    ///
    /// # Status table
    ///
    /// | Previous Status | New Status    |
    /// |-----------------|---------------|
    /// | Missing         | Missing       |
    /// | BeingDeleted    | BeingDeleted  |
    /// | Closing         | Closing       |
    /// | Available       | Closing       |
    ///
    pub fn close_for_resize(
        &mut self,
        uuid: &Uuid,
        enable_mdb_writemap: bool,
        map_size_growth: usize,
    ) {
        let Some(index) = self.available.remove(uuid) else {
            return;
        };
        self.close(*uuid, index, enable_mdb_writemap, map_size_growth);
    }

    fn close(
        &mut self,
        uuid: Uuid,
        index: Index,
        enable_mdb_writemap: bool,
        map_size_growth: usize,
    ) {
        let map_size = index.map_size() + map_size_growth;
        let closing_event = index.prepare_for_closing();
        let generation = self.next_generation();
        let closing_index =
            ClosingIndex { uuid, closing_event, enable_mdb_writemap, map_size, generation };
        match self.unavailable.insert(uuid, closing_index) {
            InsertionOutcome::InsertedNew => (),
            InsertionOutcome::Evicted(evicted_uuid, evicted_closing_index) => {
                // In case of no runtime, we simply keep the index on disk but
                // don't store it in the transferring map nor run the transfer task.
                // This way, next time the index is needed, we can simply load it from disk.
                if let Some(transfer_sender) = self.transfer_sender.as_ref() {
                    // What should we do about the index being closed?
                    // Should I give the closing_event to the uploader?
                    // I think I don't care about the closing_event, right?
                    let (answer, receiver) = oneshot::channel();
                    // TODO We would probably prefer using the async `send` instead
                    transfer_sender
                        .blocking_send(IndexTransferRequest::Upload { uuid: evicted_uuid, answer })
                        // TODO do not unwrap here
                        .unwrap();
                    self.transferring.insert(evicted_uuid, receiver.shared());
                }
            }
            InsertionOutcome::Replaced(_) => unreachable!(),
        }
    }

    /// Attempts to delete and index.
    ///
    ///  `end_deletion` must be called just after.
    ///
    /// # Status table
    ///
    /// | Previous Status | New Status   | Return value                |
    /// |-----------------|--------------|-----------------------------|
    /// | Missing         | BeingDeleted | Ok(None)                    |
    /// | BeingDeleted    | BeingDeleted | Err(None)                   |
    /// | Closing         | Closing      | Err(Some(reopen))           |
    /// | Available       | BeingDeleted | Ok(Some(env_closing_event)) |
    pub fn start_deletion(
        &mut self,
        uuid: &Uuid,
    ) -> std::result::Result<Option<EnvClosingEvent>, Option<ClosingIndex>> {
        if let Some(index) = self.available.remove(uuid) {
            self.deleting.insert(*uuid);
            return Ok(Some(index.prepare_for_closing()));
        }
        match self.unavailable.remove(uuid) {
            Some(reopen) => Err(Some(reopen)),
            None if self.deleting.contains(uuid) => Err(None),
            None => Ok(None),
        }
    }

    /// Marks that an index deletion finished.
    ///
    /// Must be used after calling `start_deletion`.
    ///
    /// # Status table
    ///
    /// | Previous Status | New Status |
    /// |-----------------|------------|
    /// | Missing         | Missing    |
    /// | BeingDeleted    | Missing    |
    /// | Closing         | panics     |
    /// | Available       | panics     |
    pub fn end_deletion(&mut self, uuid: &Uuid) {
        assert!(
            self.available.get(uuid).is_none(),
            "Attempt to finish deletion of an index that was not being deleted"
        );
        self.deleting.remove(uuid);
        // Do not panic if the index was Missing or BeingDeleted
        assert!(
            !matches!(self.unavailable.remove(uuid), Some(_)),
            "Attempt to finish deletion of an index that was being closed"
        );
    }

    /// Finishes the download and upload of an index that is being transferred
    /// and do not insert it anywhere. The caller will receive Missing
    /// and will open the freshly downloaded index.
    ///
    /// # Panics
    ///
    /// Panics if the index is not being transferred or the transfer a
    /// copy of an `Arc` error is kept alive.
    pub async fn end_transferring(&mut self, uuid: &Uuid) -> Result<(), io::Error> {
        match self.transferring.remove(uuid) {
            Some(task) => {
                // safety: we must be the last ones to get this error
                task.await.unwrap().map_err(|err| Arc::into_inner(err).unwrap())
            }
            None => Ok(()),
        }
    }
}

// TODO move this the a `enterprise_edition` module.
async fn process_index_transfers(
    indexes: PathBuf,
    mut transfer_receiver: Receiver<IndexTransferRequest>,
) {
    use tokio::fs::{create_dir_all, metadata};

    /// It's 10 MB/s 🐌
    fn fake_transfer_speed(size: u64) -> Duration {
        Duration::from_secs(size / (10 * 1024 * 1024))
    }

    async fn download_index(
        indexes_folder: &Path,
        fake_s3_folder: &Path,
        uuid: Uuid,
    ) -> Result<(), io::Error> {
        let index_path = indexes_folder.join(uuid.to_string());
        let index_path_in_s3 = fake_s3_folder.join(uuid.to_string());
        let index_size = metadata(index_path_in_s3.join("data.ms")).await?.len();
        let download_duration = fake_transfer_speed(index_size);
        tokio::time::sleep(download_duration).await;
        tokio::fs::rename(index_path_in_s3, index_path).await?;
        Ok(())
    }

    async fn upload_index(
        indexes_folder: &Path,
        fake_s3_folder: &Path,
        uuid: Uuid,
    ) -> Result<(), io::Error> {
        let index_path = indexes_folder.join(uuid.to_string());
        let index_path_in_s3 = fake_s3_folder.join(uuid.to_string());
        let index_size = metadata(index_path.join("data.ms")).await?.len();
        let upload_duration = fake_transfer_speed(index_size);
        tokio::time::sleep(upload_duration).await;
        tokio::fs::rename(index_path, index_path_in_s3).await?;
        Ok(())
    }

    // Create the folder that fakes S3
    // TODO remove this once we actually upload to S3
    let fake_s3 = indexes.join("this-is-s3");
    if let Err(e) = create_dir_all(&fake_s3).await {
        if e.kind() != ErrorKind::AlreadyExists {
            panic!("Failed to create fake S3 folder: {e}");
        }
    }

    while let Some(request) = transfer_receiver.recv().await {
        match request {
            IndexTransferRequest::Download { uuid, answer } => {
                let result = download_index(&indexes, &fake_s3, uuid).await.map_err(Arc::new);
                if answer.send(result).is_err() {
                    warn!("Couldn't send the download status of index {uuid}: channel closed");
                }
            }
            IndexTransferRequest::Upload { uuid, answer } => {
                let result = upload_index(&indexes, &fake_s3, uuid).await.map_err(Arc::new);
                if answer.send(result).is_err() {
                    warn!("Couldn't send the upload status of index {uuid}: channel closed");
                }
            }
        }
    }
}

/// Create or open an index in the specified path.
/// The path *must* exist or an error will be thrown.
fn create_or_open_index(
    path: &Path,
    date: Option<(OffsetDateTime, OffsetDateTime)>,
    enable_mdb_writemap: bool,
    map_size: usize,
    create_or_open: CreateOrOpen,
) -> Result<Index> {
    let options = EnvOpenOptions::new();
    let mut options = options.read_txn_without_tls();
    options.map_size(clamp_to_page_size(map_size));

    // You can find more details about this experimental
    // environment variable on the following GitHub discussion:
    // <https://github.com/orgs/meilisearch/discussions/806>
    let max_readers = match std::env::var("MEILI_EXPERIMENTAL_INDEX_MAX_READERS") {
        Ok(value) => u32::from_str(&value).unwrap(),
        Err(VarError::NotPresent) => 1024,
        Err(VarError::NotUnicode(value)) => panic!(
            "Invalid unicode for the `MEILI_EXPERIMENTAL_INDEX_MAX_READERS` env var: {value:?}"
        ),
    };
    options.max_readers(max_readers);
    if enable_mdb_writemap {
        unsafe { options.flags(EnvFlags::WRITE_MAP) };
    }

    if let Some((created, updated)) = date {
        Ok(Index::new_with_creation_dates(options, path, created, updated, create_or_open)?)
    } else {
        Ok(Index::new(options, path, create_or_open)?)
    }
}

/// Putting the tests of the LRU down there so we have access to the cache's private members
#[cfg(test)]
mod tests {

    use meilisearch_types::heed::{Env, WithoutTls};
    use meilisearch_types::Index;
    use uuid::Uuid;

    use super::super::IndexMapper;
    use crate::index_mapper::index_map::ClosingIndex;
    use crate::test_utils::IndexSchedulerHandle;
    use crate::utils::clamp_to_page_size;
    use crate::IndexScheduler;

    impl IndexMapper {
        fn test() -> (Self, Env<WithoutTls>, IndexSchedulerHandle) {
            let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);
            (index_scheduler.index_mapper, index_scheduler.env, handle)
        }
    }

    fn check_first_unavailable(mapper: &IndexMapper, expected_uuid: Uuid, is_closing: bool) {
        let index_map = mapper.index_map.read().unwrap();
        // let (uuid, state) = index_map.unavailable.first_key_value().unwrap();
        let (uuid, state): (&Uuid, Option<ClosingIndex>) =
            todo!("check the first key value another way with the LRU");
        assert_eq!(uuid, &expected_uuid);
        assert_eq!(state.is_some(), is_closing);
    }

    #[tokio::test]
    async fn evict_indexes() {
        let (mapper, env, _handle) = IndexMapper::test();
        let mut uuids = vec![];
        // LRU cap + 1
        for i in 0..(5 + 1) {
            let index_name = format!("index-{i}");
            let wtxn = env.write_txn().unwrap();
            mapper.create_index(wtxn, &index_name, None, None).await.unwrap();
            let txn = env.read_txn().unwrap();
            uuids.push(mapper.index_mapping.get(&txn, &index_name).unwrap().unwrap());
        }
        // index-0 was evicted
        check_first_unavailable(&mapper, uuids[0], true);

        // get back the evicted index
        let wtxn = env.write_txn().unwrap();
        mapper.create_index(wtxn, "index-0", None, None).await.unwrap();

        // Least recently used is now index-1
        check_first_unavailable(&mapper, uuids[1], true);
    }

    #[tokio::test]
    async fn resize_index() {
        let (mapper, env, _handle) = IndexMapper::test();
        let index =
            mapper.create_index(env.write_txn().unwrap(), "index", None, None).await.unwrap();
        assert_index_size(index, mapper.index_base_map_size);

        mapper.resize_index(&env.read_txn().unwrap(), "index").unwrap();

        let index =
            mapper.create_index(env.write_txn().unwrap(), "index", None, None).await.unwrap();
        assert_index_size(index, mapper.index_base_map_size + mapper.index_growth_amount);

        mapper.resize_index(&env.read_txn().unwrap(), "index").unwrap();

        let index =
            mapper.create_index(env.write_txn().unwrap(), "index", None, None).await.unwrap();
        assert_index_size(index, mapper.index_base_map_size + mapper.index_growth_amount * 2);
    }

    fn assert_index_size(index: Index, expected: usize) {
        let expected = clamp_to_page_size(expected);
        let index_map_size = index.map_size();
        assert_eq!(index_map_size, expected);
    }
}

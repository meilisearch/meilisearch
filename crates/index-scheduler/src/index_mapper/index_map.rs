use std::collections::HashMap;
use std::env::VarError;
use std::io;
use std::path::Path;
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
use uuid::Uuid;

use super::IndexStatus::{self, Available, BeingDeleted, Closing, Missing};
use crate::clamp_to_page_size;
use crate::lru::{InsertionOutcome, LruMap};
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
    transferring: HashMap<Uuid, Shared<oneshot::Receiver<Result<(), Arc<io::Error>>>>>,

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

#[derive(Clone)]
pub enum TransferState {
    Downloading(Shared<oneshot::Receiver<Result<(), Arc<io::Error>>>>),
    Uploading(Shared<oneshot::Receiver<Result<(), Arc<io::Error>>>>),
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
                &self.uuid,
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
    /// ## Panics
    ///
    /// Panics if `on_disk_cap` is smaller than `opened_cap`.
    pub fn new(opened_cap: usize, on_disk_cap: usize) -> IndexMap {
        let unavailable = match on_disk_cap.checked_sub(opened_cap) {
            Some(unavailable) => unavailable,
            None => panic!(
                "on-disk capacity ({on_disk_cap}) must be greater than or equal to opened capacity ({opened_cap})"
            ),
        };

        Self {
            available: LruMap::new(opened_cap),
            unavailable: LruMap::new(unavailable),
            deleting: HashSet::default(),
            transferring: HashMap::default(),
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
        uuid: &Uuid,
        path: &Path,
        date: Option<(OffsetDateTime, OffsetDateTime)>,
        enable_mdb_writemap: bool,
        map_size: usize,
        create_or_open: CreateOrOpen,
    ) -> Result<Index> {
        if !matches!(self.get_unavailable(uuid).await, Missing) {
            panic!("Attempt to open an index that was unavailable");
        }
        let index =
            create_or_open_index(path, date, enable_mdb_writemap, map_size, create_or_open)?;
        match self.available.insert(*uuid, index.clone()) {
            InsertionOutcome::InsertedNew => (),
            InsertionOutcome::Evicted(evicted_uuid, evicted_index) => {
                self.close(evicted_uuid, evicted_index, enable_mdb_writemap, 0);
            }
            InsertionOutcome::Replaced(_) => {
                panic!("Attempt to open an index that was already opened")
            }
        }
        Ok(index)
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
                let (sender, receiver) = oneshot::channel();
                // What should we do about the index being closed?
                // Should I give the closing_event to the uploader?
                // I think I don't care about the closing_event, right?
                todo!("upload the index and delete it once the upload is successful");
                self.transferring.insert(evicted_uuid, receiver.shared());
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

    /// Finishes the download of an index that is being transferred
    /// and do not insert it anywhere. The caller will receive Missing
    /// and will open the freshly downloaded index.
    ///
    /// # Panics
    ///
    /// Panics if the index is not being transferred or the transfer a
    /// copy of an `Arc` error is kept alive.
    pub async fn end_downloading(&mut self, uuid: &Uuid) -> Result<(), io::Error> {
        assert!(
            self.available.get(uuid).is_none(),
            "Attempt to finish the download of an available index"
        );
        match self.transferring.remove(uuid) {
            Some(task) => {
                // safety: we must be the last ones to get this error
                task.await.unwrap().map_err(|err| Arc::into_inner(err).unwrap())
            }
            None => {
                panic!("Attempt to finish the download of an index that is not being transferred")
            }
        }
    }

    // TBD we could probably merge this with `end_downloading`
    pub async fn end_uploading(&mut self, uuid: &Uuid) -> Result<(), io::Error> {
        assert!(
            self.available.get(uuid).is_none(),
            "Attempt to finish the download of an available index"
        );
        match self.transferring.remove(uuid) {
            Some(task) => {
                // safety: we must be the last ones to get this error
                task.await.unwrap().map_err(|err| Arc::into_inner(err).unwrap())
            }
            None => {
                panic!("Attempt to finish the download of an index that is not being transferred")
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

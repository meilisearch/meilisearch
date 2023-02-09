use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{fs, thread};

use log::error;
use meilisearch_types::heed::types::Str;
use meilisearch_types::heed::{Database, Env, RoTxn, RwTxn};
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::Index;
use time::OffsetDateTime;
use uuid::Uuid;

use self::index_map::IndexMap;
use self::IndexStatus::{Available, BeingDeleted, Closing, Missing};
use crate::uuid_codec::UuidCodec;
use crate::{Error, Result};

const INDEX_MAPPING: &str = "index-mapping";

/// Structure managing meilisearch's indexes.
///
/// It is responsible for:
/// 1. Creating new indexes
/// 2. Opening indexes and storing references to these opened indexes
/// 3. Accessing indexes through their uuid
/// 4. Mapping a user-defined name to each index uuid.
///
/// # Implementation notes
///
/// An index exists as 3 bits of data:
/// 1. The index data on disk, that can exist in 3 states: Missing, Present, or BeingDeleted.
/// 2. The persistent database containing the association between the index' name and its UUID,
///    that can exist in 2 states: Missing or Present.
/// 3. The state of the index in the in-memory `IndexMap`, that can exist in multiple states:
///   - Missing
///   - Available
///   - Closing (because an index needs resizing or was evicted from the cache)
///   - BeingDeleted
///
/// All of this data should be kept consistent between index operations, which is achieved by the `IndexMapper`
/// with the use of the following primitives:
/// - A RwLock on the `IndexMap`.
/// - Transactions on the association database.
/// - ClosingEvent signals emitted when closing an environment.
#[derive(Clone)]
pub struct IndexMapper {
    /// Keep track of the opened indexes. Used mainly by the index resolver.
    index_map: Arc<RwLock<IndexMap>>,

    /// Map an index name with an index uuid currently available on disk.
    pub(crate) index_mapping: Database<Str, UuidCodec>,

    /// Path to the folder where the LMDB environments of each index are.
    base_path: PathBuf,
    /// The map size an index is opened with on the first time.
    index_base_map_size: usize,
    /// The quantity by which the map size of an index is incremented upon reopening, in bytes.
    index_growth_amount: usize,
    pub indexer_config: Arc<IndexerConfig>,
}

mod index_map {
    /// the map size to use when we don't succeed in reading it in indexes.
    const DEFAULT_MAP_SIZE: usize = 10 * 1024 * 1024 * 1024; // 10 GiB

    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::Duration;

    use meilisearch_types::heed::{EnvClosingEvent, EnvOpenOptions};
    use meilisearch_types::milli::Index;
    use time::OffsetDateTime;
    use uuid::Uuid;

    use super::IndexStatus::{self, Available, BeingDeleted, Closing, Missing};
    use crate::lru::{InsertionOutcome, LruMap};
    use crate::{clamp_to_page_size, Result};

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
        /// If they are being deleted, the UUID point to `None`.
        unavailable: BTreeMap<Uuid, Option<ClosingIndex>>,

        generation: usize,
    }

    #[derive(Clone)]
    pub struct ClosingIndex {
        uuid: Uuid,
        closing_event: EnvClosingEvent,
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
                map_size: self.map_size,
                generation: self.generation,
            })
        }
    }

    pub struct ReopenableIndex {
        uuid: Uuid,
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
        /// | Previous Status | New Status |
        /// |-----------------|------------|
        /// | Missing | Missing |
        /// | BeingDeleted | BeingDeleted |
        /// | Closing | Available or Closing depending on generation |
        /// | Available | Available |
        ///
        pub fn reopen(self, map: &mut IndexMap, path: &Path) -> Result<()> {
            if let Closing(reopen) = map.get(&self.uuid) {
                if reopen.generation != self.generation {
                    return Ok(());
                }
                map.unavailable.remove(&self.uuid);
                map.create(&self.uuid, path, None, self.map_size)?;
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
        /// | Previous Status | New Status |
        /// |-----------------|------------|
        /// | Missing | Missing |
        /// | BeingDeleted | BeingDeleted |
        /// | Closing | Missing or Closing depending on generation |
        /// | Available | Available |
        pub fn close(self, map: &mut IndexMap) {
            if let Closing(reopen) = map.get(&self.uuid) {
                if reopen.generation != self.generation {
                    return;
                }
                map.unavailable.remove(&self.uuid);
            }
        }
    }

    impl IndexMap {
        pub fn new(cap: usize) -> IndexMap {
            Self { unavailable: Default::default(), available: LruMap::new(cap), generation: 0 }
        }

        /// Gets the current status of an index in the map.
        ///
        /// If the index is available it can be accessed from the returned status.
        pub fn get(&self, uuid: &Uuid) -> IndexStatus {
            self.available
                .get(uuid)
                .map(|index| Available(index.clone()))
                .unwrap_or_else(|| self.get_unavailable(uuid))
        }

        fn get_unavailable(&self, uuid: &Uuid) -> IndexStatus {
            match self.unavailable.get(uuid) {
                Some(Some(reopen)) => Closing(reopen.clone()),
                Some(None) => BeingDeleted,
                None => Missing,
            }
        }

        /// Attempts to create a new index that wasn't existing before.
        ///
        /// # Status table
        ///
        /// | Previous Status | New Status |
        /// |-----------------|------------|
        /// | Missing | Available |
        /// | BeingDeleted | panics |
        /// | Closing | panics |
        /// | Available | panics |
        ///
        pub fn create(
            &mut self,
            uuid: &Uuid,
            path: &Path,
            date: Option<(OffsetDateTime, OffsetDateTime)>,
            map_size: usize,
        ) -> Result<Index> {
            if !matches!(self.get_unavailable(uuid), Missing) {
                panic!("Attempt to open an index that was unavailable");
            }
            let index = create_or_open_index(path, date, map_size)?;
            match self.available.insert(*uuid, index.clone()) {
                InsertionOutcome::InsertedNew => (),
                InsertionOutcome::Evicted(evicted_uuid, evicted_index) => {
                    self.close(evicted_uuid, evicted_index, 0);
                }
                InsertionOutcome::Replaced(_) => {
                    panic!("Attempt to open an index that was already opened")
                }
            }
            Ok(index)
        }

        fn next_generation(&mut self) -> usize {
            self.generation = self.generation.checked_add(1).unwrap();
            self.generation
        }

        /// Attempts to close an index.
        ///
        /// # Status table
        ///
        /// | Previous Status | New Status |
        /// |-----------------|------------|
        /// | Missing | Missing |
        /// | BeingDeleted | BeingDeleted |
        /// | Closing | Closing |
        /// | Available | Closing |
        ///
        pub fn close_for_resize(&mut self, uuid: &Uuid, map_size_growth: usize) {
            let Some(index) = self.available.remove(uuid) else { return; };
            self.close(*uuid, index, map_size_growth);
        }

        fn close(&mut self, uuid: Uuid, index: Index, map_size_growth: usize) {
            let map_size = index.map_size().unwrap_or(DEFAULT_MAP_SIZE) + map_size_growth;
            let closing_event = index.prepare_for_closing();
            let generation = self.next_generation();
            self.unavailable
                .insert(uuid, Some(ClosingIndex { uuid, closing_event, map_size, generation }));
        }

        /// Attempts to delete and index.
        ///
        /// # Status table
        ///
        /// | Previous Status | New Status | Return value |
        /// |-----------------|------------|--------------|
        /// | Missing | BeingDeleted | Ok(None) |
        /// | BeingDeleted | BeingDeleted | Err(None) |
        /// | Closing | Closing | Err(Some(reopen)) |
        /// | Available | BeingDeleted | Ok(Some(env_closing_event)) |
        pub fn start_deletion(
            &mut self,
            uuid: &Uuid,
        ) -> std::result::Result<Option<EnvClosingEvent>, Option<ClosingIndex>> {
            if let Some(index) = self.available.remove(uuid) {
                return Ok(Some(index.prepare_for_closing()));
            }
            match self.unavailable.remove(uuid) {
                Some(Some(reopen)) => Err(Some(reopen)),
                Some(None) => Err(None),
                None => Ok(None),
            }
        }

        /// Marks that an index finished deletion.
        ///
        /// # Status table
        ///
        /// | Previous Status | New Status |
        /// |-----------------|------------|
        /// | Missing | Missing |
        /// | BeingDeleted | Missing |
        /// | Closing | panics |
        /// | Available | panics |
        pub fn end_deletion(&mut self, uuid: &Uuid) {
            assert!(
                self.available.get(uuid).is_none(),
                "Attempt to finish deletion of an index that was not being deleted"
            );
            // Do not panic if the index was Missing or BeingDeleted
            assert!(
                !matches!(self.unavailable.remove(uuid), Some(Some(_))),
                "Attempt to finish deletion of an index that was being closed"
            );
        }
    }

    /// Create or open an index in the specified path.
    /// The path *must* exist or an error will be thrown.
    fn create_or_open_index(
        path: &Path,
        date: Option<(OffsetDateTime, OffsetDateTime)>,
        map_size: usize,
    ) -> Result<Index> {
        let mut options = EnvOpenOptions::new();
        options.map_size(clamp_to_page_size(map_size));
        options.max_readers(1024);

        if let Some((created, updated)) = date {
            Ok(Index::new_with_creation_dates(options, path, created, updated)?)
        } else {
            Ok(Index::new(options, path)?)
        }
    }

    /// Putting the tests of the LRU down there so we have access to the cache's private members
    #[cfg(test)]
    mod tests {

        use meilisearch_types::heed::Env;
        use meilisearch_types::Index;
        use uuid::Uuid;

        use super::super::IndexMapper;
        use crate::tests::IndexSchedulerHandle;
        use crate::utils::clamp_to_page_size;
        use crate::IndexScheduler;

        impl IndexMapper {
            fn test() -> (Self, Env, IndexSchedulerHandle) {
                let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);
                (index_scheduler.index_mapper, index_scheduler.env, handle)
            }
        }

        fn check_first_unavailable(mapper: &IndexMapper, expected_uuid: Uuid, is_closing: bool) {
            let index_map = mapper.index_map.read().unwrap();
            let (uuid, state) = index_map.unavailable.first_key_value().unwrap();
            assert_eq!(uuid, &expected_uuid);
            assert_eq!(state.is_some(), is_closing);
        }

        #[test]
        fn evict_indexes() {
            let (mapper, env, _handle) = IndexMapper::test();
            let mut uuids = vec![];
            // LRU cap + 1
            for i in 0..(5 + 1) {
                let index_name = format!("index-{i}");
                let wtxn = env.write_txn().unwrap();
                mapper.create_index(wtxn, &index_name, None).unwrap();
                let txn = env.read_txn().unwrap();
                uuids.push(mapper.index_mapping.get(&txn, &index_name).unwrap().unwrap());
            }
            // index-0 was evicted
            check_first_unavailable(&mapper, uuids[0], true);

            // get back the evicted index
            let wtxn = env.write_txn().unwrap();
            mapper.create_index(wtxn, "index-0", None).unwrap();

            // Least recently used is now index-1
            check_first_unavailable(&mapper, uuids[1], true);
        }

        #[test]
        fn resize_index() {
            let (mapper, env, _handle) = IndexMapper::test();
            let index = mapper.create_index(env.write_txn().unwrap(), "index", None).unwrap();
            assert_index_size(index, mapper.index_base_map_size);

            mapper.resize_index(&env.read_txn().unwrap(), "index").unwrap();

            let index = mapper.create_index(env.write_txn().unwrap(), "index", None).unwrap();
            assert_index_size(index, mapper.index_base_map_size + mapper.index_growth_amount);

            mapper.resize_index(&env.read_txn().unwrap(), "index").unwrap();

            let index = mapper.create_index(env.write_txn().unwrap(), "index", None).unwrap();
            assert_index_size(index, mapper.index_base_map_size + mapper.index_growth_amount * 2);
        }

        fn assert_index_size(index: Index, expected: usize) {
            let expected = clamp_to_page_size(expected);
            let index_map_size = index.map_size().unwrap();
            assert_eq!(index_map_size, expected);
        }
    }
}

/// Whether the index is available for use or is forbidden to be inserted back in the index map
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum IndexStatus {
    /// Not currently in the index map.
    Missing,
    /// Do not insert it back in the index map as it is currently being deleted.
    BeingDeleted,
    /// Temporarily do not insert the index in the index map as it is currently being resized/evicted from the map.
    Closing(index_map::ClosingIndex),
    /// You can use the index without worrying about anything.
    Available(Index),
}

impl IndexMapper {
    pub fn new(
        env: &Env,
        base_path: PathBuf,
        index_base_map_size: usize,
        index_growth_amount: usize,
        index_count: usize,
        indexer_config: IndexerConfig,
    ) -> Result<Self> {
        Ok(Self {
            index_map: Arc::new(RwLock::new(IndexMap::new(index_count))),
            index_mapping: env.create_database(Some(INDEX_MAPPING))?,
            base_path,
            index_base_map_size,
            index_growth_amount,
            indexer_config: Arc::new(indexer_config),
        })
    }

    /// Get or create the index.
    pub fn create_index(
        &self,
        mut wtxn: RwTxn,
        name: &str,
        date: Option<(OffsetDateTime, OffsetDateTime)>,
    ) -> Result<Index> {
        match self.index(&wtxn, name) {
            Ok(index) => {
                wtxn.commit()?;
                Ok(index)
            }
            Err(Error::IndexNotFound(_)) => {
                let uuid = Uuid::new_v4();
                self.index_mapping.put(&mut wtxn, name, &uuid)?;

                let index_path = self.base_path.join(uuid.to_string());
                fs::create_dir_all(&index_path)?;

                // Error if the UUIDv4 somehow already exists in the map, since it should be fresh.
                // This is very unlikely to happen in practice.
                // TODO: it would be better to lazily create the index. But we need an Index::open function for milli.
                let index = self.index_map.write().unwrap().create(
                    &uuid,
                    &index_path,
                    date,
                    self.index_base_map_size,
                )?;

                wtxn.commit()?;

                Ok(index)
            }
            error => error,
        }
    }

    /// Removes the index from the mapping table and the in-memory index map
    /// but keeps the associated tasks.
    pub fn delete_index(&self, mut wtxn: RwTxn, name: &str) -> Result<()> {
        let uuid = self
            .index_mapping
            .get(&wtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // Once we retrieved the UUID of the index we remove it from the mapping table.
        assert!(self.index_mapping.delete(&mut wtxn, name)?);

        wtxn.commit()?;

        let mut tries = 0;
        // We remove the index from the in-memory index map.
        let closing_event = loop {
            let mut lock = self.index_map.write().unwrap();
            match lock.start_deletion(&uuid) {
                Ok(env_closing) => break env_closing,
                Err(Some(reopen)) => {
                    // drop the lock here so that we don't synchronously wait for the index to close.
                    drop(lock);
                    tries += 1;
                    if tries >= 100 {
                        panic!("Too many attempts to close index {name} prior to deletion.")
                    }
                    let reopen = if let Some(reopen) = reopen.wait_timeout(Duration::from_secs(6)) {
                        reopen
                    } else {
                        continue;
                    };
                    reopen.close(&mut self.index_map.write().unwrap());
                    continue;
                }
                Err(None) => return Ok(()),
            }
        };

        let index_map = self.index_map.clone();
        let index_path = self.base_path.join(uuid.to_string());
        let index_name = name.to_string();
        thread::Builder::new()
            .name(String::from("index_deleter"))
            .spawn(move || {
                // We first wait to be sure that the previously opened index is effectively closed.
                // This can take a lot of time, this is why we do that in a separate thread.
                if let Some(closing_event) = closing_event {
                    closing_event.wait();
                }

                // Then we remove the content from disk.
                if let Err(e) = fs::remove_dir_all(&index_path) {
                    error!(
                        "An error happened when deleting the index {} ({}): {}",
                        index_name, uuid, e
                    );
                }

                // Finally we remove the entry from the index map.
                index_map.write().unwrap().end_deletion(&uuid);
            })
            .unwrap();

        Ok(())
    }

    pub fn exists(&self, rtxn: &RoTxn, name: &str) -> Result<bool> {
        Ok(self.index_mapping.get(rtxn, name)?.is_some())
    }

    /// Resizes the maximum size of the specified index to the double of its current maximum size.
    ///
    /// This operation involves closing the underlying environment and so can take a long time to complete.
    ///
    /// # Panics
    ///
    /// - If the Index corresponding to the passed name is concurrently being deleted/resized or cannot be found in the
    ///   in memory hash map.
    pub fn resize_index(&self, rtxn: &RoTxn, name: &str) -> Result<()> {
        let uuid = self
            .index_mapping
            .get(rtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // We remove the index from the in-memory index map.
        self.index_map.write().unwrap().close_for_resize(&uuid, self.index_growth_amount);

        Ok(())
    }

    /// Return an index, may open it if it wasn't already opened.
    pub fn index(&self, rtxn: &RoTxn, name: &str) -> Result<Index> {
        let uuid = self
            .index_mapping
            .get(rtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // we clone here to drop the lock before entering the match
        let mut tries = 0;
        let index = loop {
            tries += 1;
            if tries > 100 {
                panic!("Too many spurious wake ups while the index is being resized");
            }
            let index = self.index_map.read().unwrap().get(&uuid);

            match index {
                Available(index) => break index,
                Closing(reopen) => {
                    // Avoiding deadlocks: no lock taken while doing this operation.
                    let reopen = if let Some(reopen) = reopen.wait_timeout(Duration::from_secs(6)) {
                        reopen
                    } else {
                        continue;
                    };
                    let index_path = self.base_path.join(uuid.to_string());
                    // take the lock to reopen the environment.
                    reopen.reopen(&mut self.index_map.write().unwrap(), &index_path)?;
                    continue;
                }
                BeingDeleted => return Err(Error::IndexNotFound(name.to_string())),
                // since we're lazy, it's possible that the index has not been opened yet.
                Missing => {
                    let mut index_map = self.index_map.write().unwrap();
                    // between the read lock and the write lock it's not impossible
                    // that someone already opened the index (eg if two searches happen
                    // at the same time), thus before opening it we check a second time
                    // if it's not already there.
                    match index_map.get(&uuid) {
                        Missing => {
                            let index_path = self.base_path.join(uuid.to_string());

                            break index_map.create(
                                &uuid,
                                &index_path,
                                None,
                                self.index_base_map_size,
                            )?;
                        }
                        Available(index) => break index,
                        Closing(_) => {
                            // the reopening will be handled in the next loop operation
                            continue;
                        }
                        BeingDeleted => return Err(Error::IndexNotFound(name.to_string())),
                    }
                }
            }
        };

        Ok(index)
    }

    /// Return all indexes, may open them if they weren't already opened.
    pub fn indexes(&self, rtxn: &RoTxn) -> Result<Vec<(String, Index)>> {
        self.index_mapping
            .iter(rtxn)?
            .map(|ret| {
                ret.map_err(Error::from).and_then(|(name, _)| {
                    self.index(rtxn, name).map(|index| (name.to_string(), index))
                })
            })
            .collect()
    }

    /// Swap two index names.
    pub fn swap(&self, wtxn: &mut RwTxn, lhs: &str, rhs: &str) -> Result<()> {
        let lhs_uuid = self
            .index_mapping
            .get(wtxn, lhs)?
            .ok_or_else(|| Error::IndexNotFound(lhs.to_string()))?;
        let rhs_uuid = self
            .index_mapping
            .get(wtxn, rhs)?
            .ok_or_else(|| Error::IndexNotFound(rhs.to_string()))?;

        self.index_mapping.put(wtxn, lhs, &rhs_uuid)?;
        self.index_mapping.put(wtxn, rhs, &lhs_uuid)?;

        Ok(())
    }

    pub fn index_exists(&self, rtxn: &RoTxn, name: &str) -> Result<bool> {
        Ok(self.index_mapping.get(rtxn, name)?.is_some())
    }

    pub fn indexer_config(&self) -> &IndexerConfig {
        &self.indexer_config
    }
}

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{fs, thread};

use log::error;
use meilisearch_types::heed::types::Str;
use meilisearch_types::heed::{Database, Env, EnvClosingEvent, EnvOpenOptions, RoTxn, RwTxn};
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::Index;
use synchronoise::SignalEvent;
use time::OffsetDateTime;
use uuid::Uuid;

use self::IndexStatus::{Available, DefinitelyUnavailable, TemporarilyUnavailable};
use crate::lru::{InsertionOutcome, LruMap};
use crate::uuid_codec::UuidCodec;
use crate::{clamp_to_page_size, Error, Result};

const INDEX_MAPPING: &str = "index-mapping";

/// Structure managing meilisearch's indexes.
///
/// It is responsible for:
/// 1. Creating new indexes
/// 2. Opening indexes and storing references to these opened indexes
/// 3. Accessing indexes through their uuid
/// 4. Mapping a user-defined name to each index uuid.
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

struct IndexMap {
    unavailable: Vec<(Uuid, Option<ClosingSignal>)>,
    available: LruMap<Uuid, Index>,
}

enum Unavailable {
    Temporarily,
    Definitely,
}

#[derive(Clone)]
pub enum ClosingSignal {
    Resized(Arc<SignalEvent>),
    Closed(EnvClosingEvent),
}

impl ClosingSignal {
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        match self {
            ClosingSignal::Resized(signal) => signal.wait_timeout(timeout),
            ClosingSignal::Closed(signal) => signal.wait_timeout(timeout),
        }
    }
}

impl IndexMap {
    pub fn new(cap: usize) -> IndexMap {
        Self { unavailable: Vec::new(), available: LruMap::new(cap) }
    }

    pub fn get(&self, uuid: &Uuid) -> Option<IndexStatus> {
        self.get_if_unavailable(uuid)
            .map(|signal| {
                if let Some(signal) = signal {
                    IndexStatus::TemporarilyUnavailable(signal)
                } else {
                    IndexStatus::DefinitelyUnavailable
                }
            })
            .or_else(|| self.available.get(uuid).map(|index| IndexStatus::Available(index.clone())))
    }

    /// Inserts a new index as available
    ///
    /// # Panics
    ///
    /// - If the index is already present, but currently unavailable.
    pub fn insert(&mut self, uuid: &Uuid, index: Index) -> Option<Index> {
        assert!(
            matches!(self.get_if_unavailable(uuid), None),
            "Attempted to insert an index that was not available"
        );

        match self.available.insert(*uuid, index) {
            InsertionOutcome::InsertedNew => None,
            InsertionOutcome::Evicted(evicted_uuid, evicted_index) => {
                self.evict(evicted_uuid, evicted_index);
                None
            }
            InsertionOutcome::Replaced(replaced_index) => Some(replaced_index),
        }
    }

    fn evict(&mut self, uuid: Uuid, index: Index) {
        let closed = index.prepare_for_closing();
        self.unavailable.push((uuid, Some(ClosingSignal::Closed(closed))));
    }

    /// Makes an index temporarily or permanently unavailable.
    ///
    /// Does nothing if the target index is already unavailable.
    pub fn make_unavailable(&mut self, uuid: &Uuid, unavailability: Unavailable) -> Option<Index> {
        if self.get_if_unavailable(uuid).is_some() {
            return None;
        }

        let available_when = match unavailability {
            Unavailable::Temporarily => {
                Some(ClosingSignal::Resized(Arc::new(SignalEvent::manual(false))))
            }
            Unavailable::Definitely => None,
        };

        let index = self.available.remove(uuid)?;
        self.unavailable.push((*uuid, available_when));
        Some(index)
    }

    /// Makes an index available again.
    ///
    /// As the index becomes available again, it might evict another index from the cache. In that case, it is returned.
    ///
    /// # Panics
    ///
    /// - if the target index was not being temporarily resized.
    /// - the index was also in the list of available indexes.
    pub fn restore(&mut self, uuid: &Uuid, index: Index) -> ClosingSignal {
        let signal = self
            .pop_if_unavailable(uuid)
            .flatten()
            .expect("The index was not being temporarily resized");
        match self.available.insert(*uuid, index) {
            InsertionOutcome::Evicted(evicted_uuid, evicted_index) => {
                self.evict(evicted_uuid, evicted_index)
            }
            InsertionOutcome::InsertedNew => (),
            InsertionOutcome::Replaced(_) => panic!("Inconsistent map state"),
        }
        signal
    }

    /// Removes an unavailable index from the map.
    ///
    /// # Panics
    ///
    /// - if the target index was not unavailable.
    pub fn remove(&mut self, uuid: &Uuid) -> Option<ClosingSignal> {
        self.pop_if_unavailable(uuid).expect("The index could not be found")
    }

    fn get_if_unavailable(&self, uuid: &Uuid) -> Option<Option<ClosingSignal>> {
        self.unavailable
            .iter()
            .find_map(|(candidate_uuid, signal)| (uuid == candidate_uuid).then_some(signal.clone()))
    }

    fn pop_if_unavailable(&mut self, uuid: &Uuid) -> Option<Option<ClosingSignal>> {
        self.unavailable
            .iter()
            .position(|(candidate_uuid, _)| candidate_uuid == uuid)
            .map(|index| self.unavailable.swap_remove(index).1)
    }
}

/// Whether the index is available for use or is forbidden to be inserted back in the index map
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum IndexStatus {
    /// Do not insert it back in the index map as it is currently being deleted.
    DefinitelyUnavailable,
    /// Temporarily do not insert the index in the index map as it is currently being resized/evicted from the map.
    TemporarilyUnavailable(ClosingSignal),
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

    /// Create or open an index in the specified path.
    /// The path *must* exists or an error will be thrown.
    fn create_or_open_index(
        &self,
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

                let index =
                    self.create_or_open_index(&index_path, date, self.index_base_map_size)?;

                wtxn.commit()?;
                // Error if the UUIDv4 somehow already exists in the map, since it should be fresh.
                // This is very unlikely to happen in practice.
                // TODO: it would be better to lazily create the index. But we need an Index::open function for milli.
                if self.index_map.write().unwrap().insert(&uuid, index.clone()).is_some() {
                    panic!("Uuid v4 conflict: index with UUID {uuid} already exists.")
                }

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
            let resize_operation = match lock.get(&uuid) {
                Some(Available(index)) => {
                    lock.make_unavailable(&uuid, Unavailable::Definitely);
                    break index.prepare_for_closing();
                }
                Some(TemporarilyUnavailable(resize_operation)) => resize_operation.clone(),
                Some(DefinitelyUnavailable) | None => return Ok(()),
            };
            // Avoiding deadlock: we drop the lock before waiting on the resize operation.
            drop(lock);
            resize_operation.wait_timeout(Duration::from_secs(6));
            tries += 1;
            if tries > 100 {
                panic!("Too many spurious wakeups while waiting on a resize operation.")
            }
        };

        let index_map = self.index_map.clone();
        let index_path = self.base_path.join(uuid.to_string());
        let index_name = name.to_string();
        thread::Builder::new()
            .name(String::from("index_deleter"))
            .spawn(move || {
                // We first wait to be sure that the previously opened index is effectively closed.
                // This can take a lot of time, this is why we do that in a seperate thread.
                closing_event.wait();

                // Then we remove the content from disk.
                if let Err(e) = fs::remove_dir_all(&index_path) {
                    error!(
                        "An error happened when deleting the index {} ({}): {}",
                        index_name, uuid, e
                    );
                }

                // Finally we remove the entry from the index map.
                index_map.write().unwrap().remove(&uuid);
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
        // fixme: factor to a function?
        let uuid = self
            .index_mapping
            .get(rtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // We remove the index from the in-memory index map.
        let Some(index) = self.index_map.write().unwrap().make_unavailable(&uuid, Unavailable::Temporarily) else { return Ok(()) };

        let resize_succeeded = (move || {
            let current_size = index.map_size()?;
            let new_size = current_size + self.index_growth_amount;
            let closing_event = index.prepare_for_closing();

            log::debug!("Waiting for index {name} to close");

            if !closing_event.wait_timeout(std::time::Duration::from_secs(600)) {
                // fail after 10 minutes waiting
                panic!("Could not resize index {name} (unable to close it)");
            }

            log::info!("Resized index {name} from {current_size} to {new_size} bytes");
            let index_path = self.base_path.join(uuid.to_string());
            let index = self.create_or_open_index(&index_path, None, new_size)?;
            Ok(index)
        })();

        // Put the map back to a consistent state.
        // Even if there was an error we don't want to leave the map in an inconsistent state as it would cause
        // deadlocks.
        let mut lock = self.index_map.write().unwrap();
        let (resize_operation, resize_succeeded) = match resize_succeeded {
            Ok(index) => {
                // insert the resized index
                let resize_operation = lock.restore(&uuid, index);
                (resize_operation, Ok(()))
            }
            Err(error) => {
                // there was an error, not much we can do... delete the index from the in-memory map to prevent future errors
                let resize_operation = lock.remove(&uuid).expect("The index was not being resized");
                (resize_operation, Err(error))
            }
        };

        // drop the lock before signaling completion so that other threads don't immediately await on the lock after waking up.
        drop(lock);
        let ClosingSignal::Resized(resize_operation) = resize_operation else {
         panic!("Index was closed while being resized") };

        resize_operation.signal();

        resize_succeeded
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
                Some(Available(index)) => break index,
                Some(TemporarilyUnavailable(ref closing_signal)) => {
                    // Avoiding deadlocks: no lock taken while doing this operation.
                    closing_signal.wait_timeout(Duration::from_secs(6));
                    continue;
                }
                Some(DefinitelyUnavailable) => return Err(Error::IndexNotFound(name.to_string())),
                // since we're lazy, it's possible that the index has not been opened yet.
                None => {
                    let mut index_map = self.index_map.write().unwrap();
                    // between the read lock and the write lock it's not impossible
                    // that someone already opened the index (eg if two searches happen
                    // at the same time), thus before opening it we check a second time
                    // if it's not already there.
                    match index_map.get(&uuid) {
                        None => {
                            let index_path = self.base_path.join(uuid.to_string());

                            let index = self.create_or_open_index(
                                &index_path,
                                None,
                                self.index_base_map_size,
                            )?;
                            assert!(
                                index_map.insert(&uuid, index.clone()).is_none(),
                                "Inconsistent map state"
                            );
                            break index;
                        }
                        Some(Available(index)) => break index,
                        Some(TemporarilyUnavailable(resize_operation)) => {
                            // Avoiding the deadlock: we drop the lock before waiting
                            let resize_operation = resize_operation.clone();
                            drop(index_map);
                            resize_operation.wait_timeout(Duration::from_secs(6));
                            continue;
                        }
                        Some(DefinitelyUnavailable) => {
                            return Err(Error::IndexNotFound(name.to_string()))
                        }
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

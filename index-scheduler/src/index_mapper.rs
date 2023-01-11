use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::{fs, thread};

use log::error;
use meilisearch_types::heed::types::Str;
use meilisearch_types::heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::Index;
use synchronoise::SignalEvent;
use time::OffsetDateTime;
use uuid::Uuid;

use self::IndexStatus::{Available, BeingDeleted, BeingResized};
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
    index_size: usize,
    pub indexer_config: Arc<IndexerConfig>,
}

struct IndexMap {
    unavailable: Vec<(Uuid, Option<Arc<SignalEvent>>)>,
    available: LruMap<Uuid, Index>,
}

impl IndexMap {
    pub fn new(cap: usize) -> IndexMap {
        Self { unavailable: Vec::new(), available: LruMap::new(cap) }
    }

    pub fn get(&self, uuid: &Uuid) -> Option<IndexStatus> {
        self.get_if_unavailable(uuid)
            .map(|signal| {
                if let Some(signal) = signal {
                    IndexStatus::BeingResized(signal)
                } else {
                    IndexStatus::BeingDeleted
                }
            })
            .or_else(|| self.available.get(uuid).map(|index| IndexStatus::Available(index.clone())))
    }

    /// Inserts a new index as available
    ///
    /// # Panics
    ///
    /// - If the index is already present, but currently unavailable.
    pub fn insert(&mut self, uuid: &Uuid, index: Index) -> InsertionOutcome<Uuid, Index> {
        assert!(
            matches!(self.get_if_unavailable(uuid), None),
            "Attempted to insert an index that was not available"
        );

        self.available.insert(*uuid, index)
    }

    /// Begins a resize operation.
    ///
    /// Returns `None` if the index is already unavailable, or not present at all.
    pub fn start_resize(&mut self, uuid: &Uuid, signal: Arc<SignalEvent>) -> Option<Index> {
        if self.get_if_unavailable(uuid).is_some() {
            return None;
        }

        let index = self.available.remove(uuid)?;
        self.unavailable.push((*uuid, Some(signal)));
        Some(index)
    }

    /// Ends a resize operation that completed successfully.
    ///
    /// As the index becomes available again, it might evict another index from the cache. In that case, it is returned.
    ///
    /// # Panics
    ///
    /// - if the target index was not being resized.
    /// - the index was also in the list of available indexes.
    pub fn end_resize(
        &mut self,
        uuid: &Uuid,
        index: Index,
    ) -> (Arc<SignalEvent>, Option<(Uuid, Index)>) {
        let signal =
            self.pop_if_unavailable(uuid).flatten().expect("The index was not being resized");
        let evicted = match self.available.insert(*uuid, index) {
            InsertionOutcome::InsertedNew => None,
            InsertionOutcome::Evicted(uuid, index) => Some((uuid, index)),
            InsertionOutcome::Replaced(_) => panic!("Inconsistent map state"),
        };
        (signal, evicted)
    }

    /// Ends a resize operation that failed for some reason.
    ///
    /// # Panics
    ///
    /// - if the target index was not being resized.
    pub fn end_resize_failed(&mut self, uuid: &Uuid) -> Arc<SignalEvent> {
        self.pop_if_unavailable(uuid).flatten().expect("The index was not being resized")
    }

    /// Beings deleting an index.
    ///
    /// # Panics
    ///
    /// - if the index was already unavailable
    pub fn start_deletion(&mut self, uuid: &Uuid) -> Option<Index> {
        assert!(
            matches!(self.get_if_unavailable(uuid), None),
            "Attempt to start deleting an index that was already unavailable"
        );

        let index = self.available.remove(uuid)?;
        self.unavailable.push((*uuid, None));
        Some(index)
    }

    pub fn end_deletion(&mut self, uuid: &Uuid) {
        self.pop_if_unavailable(uuid)
            .expect("Attempted to delete an index that was not being deleted");
    }

    fn get_if_unavailable(&self, uuid: &Uuid) -> Option<Option<Arc<SignalEvent>>> {
        self.unavailable
            .iter()
            .find_map(|(candidate_uuid, signal)| (uuid == candidate_uuid).then_some(signal.clone()))
    }

    fn pop_if_unavailable(&mut self, uuid: &Uuid) -> Option<Option<Arc<SignalEvent>>> {
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
    BeingDeleted,
    /// Temporarily do not insert the index in the index map as it is currently being resized.
    BeingResized(Arc<SignalEvent>),
    /// You can use the index without worrying about anything.
    Available(Index),
}

impl IndexMapper {
    pub fn new(
        env: &Env,
        base_path: PathBuf,
        index_size: usize,
        indexer_config: IndexerConfig,
    ) -> Result<Self> {
        Ok(Self {
            index_map: Arc::new(RwLock::new(IndexMap::new(20))),
            index_mapping: env.create_database(Some(INDEX_MAPPING))?,
            base_path,
            index_size,
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

                let index = self.create_or_open_index(&index_path, date, self.index_size)?;

                wtxn.commit()?;
                // Error if the UUIDv4 somehow already exists in the map, since it should be fresh.
                // This is very unlikely to happen in practice.
                // TODO: it would be better to lazily create the index. But we need an Index::open function for milli.
                match self.index_map.write().unwrap().insert(&uuid, index.clone()) {
                    InsertionOutcome::Evicted(uuid, evicted_index) => {
                        log::info!("Closing index with UUID {uuid}");
                        evicted_index.prepare_for_closing();
                    }
                    InsertionOutcome::Replaced(_) => {
                        panic!("Uuid v4 conflict: index with UUID {uuid} already exists.")
                    }
                    _ => (),
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

        // We remove the index from the in-memory index map.
        let closing_event = loop {
            let mut lock = self.index_map.write().unwrap();
            let resize_operation = match lock.get(&uuid) {
                Some(Available(index)) => {
                    lock.start_deletion(&uuid);
                    break index.prepare_for_closing();
                }
                Some(BeingResized(resize_operation)) => resize_operation.clone(),
                Some(BeingDeleted) | None => return Ok(()),
            };
            drop(lock);
            resize_operation.wait();
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
        // fixme: factor to a function?
        let uuid = self
            .index_mapping
            .get(rtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // We remove the index from the in-memory index map.
        // signal that will be sent when the resize operation completes
        let resize_operation = Arc::new(SignalEvent::manual(false));
        let Some(index) = self.index_map.write().unwrap().start_resize(&uuid, resize_operation) else { return Ok(()) };

        let resize_succeeded = (move || {
            let current_size = index.map_size()?;
            let new_size = current_size * 2;
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
        let (resize_operation, resize_succeeded, evicted) = match resize_succeeded {
            Ok(index) => {
                // insert the resized index
                let (resize_operation, evicted) = lock.end_resize(&uuid, index);

                (resize_operation, Ok(()), evicted)
            }
            Err(error) => {
                // there was an error, not much we can do... delete the index from the in-memory map to prevent future errors
                let resize_operation = lock.end_resize_failed(&uuid);
                (resize_operation, Err(error), None)
            }
        };

        // drop the lock before signaling completion so that other threads don't immediately await on the lock after waking up.
        drop(lock);
        resize_operation.signal();

        if let Some((uuid, evicted_index)) = evicted {
            log::info!("Closing index with UUID {uuid}");
            evicted_index.prepare_for_closing();
        }

        resize_succeeded
    }

    /// Return an index, may open it if it wasn't already opened.
    pub fn index(&self, rtxn: &RoTxn, name: &str) -> Result<Index> {
        let uuid = self
            .index_mapping
            .get(rtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // we clone here to drop the lock before entering the match
        let (index, evicted_index) = loop {
            let index = self.index_map.read().unwrap().get(&uuid);

            match index {
                Some(Available(index)) => break (index, None),
                Some(BeingResized(ref resize_operation)) => {
                    // Avoiding deadlocks: no lock taken while doing this operation.
                    resize_operation.wait();
                    continue;
                }
                Some(BeingDeleted) => return Err(Error::IndexNotFound(name.to_string())),
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

                            let index =
                                self.create_or_open_index(&index_path, None, self.index_size)?;
                            match index_map.insert(&uuid, index.clone()) {
                                InsertionOutcome::InsertedNew => break (index, None),
                                InsertionOutcome::Evicted(evicted_uuid, evicted_index) => {
                                    break (index, Some((evicted_uuid, evicted_index)))
                                }
                                InsertionOutcome::Replaced(_) => {
                                    panic!("Inconsistent map state")
                                }
                            }
                        }
                        Some(Available(index)) => break (index, None),
                        Some(BeingResized(resize_operation)) => {
                            // Avoiding the deadlock: we drop the lock before waiting
                            let resize_operation = resize_operation.clone();
                            drop(index_map);
                            resize_operation.wait();
                            continue;
                        }
                        Some(BeingDeleted) => return Err(Error::IndexNotFound(name.to_string())),
                    }
                }
            }
        };

        if let Some((evicted_uuid, evicted_index)) = evicted_index {
            log::info!("Closing index with UUID {evicted_uuid}");
            evicted_index.prepare_for_closing();
        }

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

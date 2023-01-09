use std::collections::hash_map::Entry;
use std::collections::HashMap;
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
    index_map: Arc<RwLock<HashMap<Uuid, IndexStatus>>>,

    /// Map an index name with an index uuid currently available on disk.
    pub(crate) index_mapping: Database<Str, UuidCodec>,

    /// Path to the folder where the LMDB environments of each index are.
    base_path: PathBuf,
    index_size: usize,
    pub indexer_config: Arc<IndexerConfig>,
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
            index_map: Arc::default(),
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
                if self.index_map.write().unwrap().insert(uuid, Available(index.clone())).is_some()
                {
                    panic!("Uuid v4 conflict: index with UUID {uuid} already exists.");
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
            let resize_operation = match lock.insert(uuid, BeingDeleted) {
                Some(Available(index)) => break Some(index.prepare_for_closing()),
                // The target index is in the middle of a resize operation.
                // Wait for this operation to complete, then try again.
                Some(BeingResized(resize_operation)) => resize_operation.clone(),
                // The index is already being deleted or doesn't exist.
                // It's OK to remove it from the map again.
                _ => break None,
            };

            // Avoiding deadlocks: we need to drop the lock before waiting for the end of the resize, which
            // will involve operations on the very map we're locking.
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
                assert!(matches!(index_map.write().unwrap().remove(&uuid), Some(BeingDeleted)));
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
        let mut lock = self.index_map.write().unwrap();
        // signal that will be sent when the resize operation completes
        let resize_operation = Arc::new(SignalEvent::manual(false));
        let index = match lock.insert(uuid, BeingResized(resize_operation)) {
            Some(Available(index)) => index,
            Some(previous_status) => {
                lock.insert(uuid, previous_status);
                panic!(
                    "Attempting to resize index {name} that is already being resized or deleted."
                )
            }
            None => {
                panic!("Could not find the status of index {name} in the in-memory index mapper.")
            }
        };

        drop(lock);

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

        // Add back the resized index
        let mut lock = self.index_map.write().unwrap();
        let Some(BeingResized(resize_operation)) = lock.insert(uuid, Available(index)) else {
            panic!("Index state for index {name} was modified while it was being resized")
        };

        // drop the lock before signaling completion so that other threads don't immediately await on the lock after waking up.
        drop(lock);
        resize_operation.signal();

        Ok(())
    }

    /// Return an index, may open it if it wasn't already opened.
    pub fn index(&self, rtxn: &RoTxn, name: &str) -> Result<Index> {
        let uuid = self
            .index_mapping
            .get(rtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // we clone here to drop the lock before entering the match
        let index = loop {
            let index = self.index_map.read().unwrap().get(&uuid).cloned();

            match index {
                Some(Available(index)) => break index,
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
                    // that someone already opened the index (eg if two search happens
                    // at the same time), thus before opening it we check a second time
                    // if it's not already there.
                    // Since there is a good chance it's not already there we can use
                    // the entry method.
                    match index_map.entry(uuid) {
                        Entry::Vacant(entry) => {
                            let index_path = self.base_path.join(uuid.to_string());

                            let index =
                                self.create_or_open_index(&index_path, None, self.index_size)?;
                            entry.insert(Available(index.clone()));
                            break index;
                        }
                        Entry::Occupied(entry) => match entry.get() {
                            Available(index) => break index.clone(),
                            BeingResized(resize_operation) => {
                                // Avoiding the deadlock: we drop the lock before waiting
                                let resize_operation = resize_operation.clone();
                                drop(index_map);
                                resize_operation.wait();
                                continue;
                            }
                            BeingDeleted => return Err(Error::IndexNotFound(name.to_string())),
                        },
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

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
use time::OffsetDateTime;
use uuid::Uuid;

use self::IndexStatus::{Available, BeingDeleted};
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
                // TODO: it would be better to lazily create the index. But we need an Index::open function for milli.
                if let Some(BeingDeleted) =
                    self.index_map.write().unwrap().insert(uuid, Available(index.clone()))
                {
                    panic!("Uuid v4 conflict.");
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
        let mut lock = self.index_map.write().unwrap();
        let closing_event = match lock.insert(uuid, BeingDeleted) {
            Some(Available(index)) => Some(index.prepare_for_closing()),
            _ => None,
        };

        drop(lock);

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

    /// Return an index, may open it if it wasn't already opened.
    pub fn index(&self, rtxn: &RoTxn, name: &str) -> Result<Index> {
        let uuid = self
            .index_mapping
            .get(rtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // we clone here to drop the lock before entering the match
        let index = self.index_map.read().unwrap().get(&uuid).cloned();
        let index = match index {
            Some(Available(index)) => index,
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
                        index
                    }
                    Entry::Occupied(entry) => match entry.get() {
                        Available(index) => index.clone(),
                        BeingDeleted => return Err(Error::IndexNotFound(name.to_string())),
                    },
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

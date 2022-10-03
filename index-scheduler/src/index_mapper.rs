use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use index::Index;
use uuid::Uuid;

use milli::heed::types::SerdeBincode;
use milli::heed::types::Str;
use milli::heed::Database;
use milli::heed::Env;
use milli::heed::RoTxn;
use milli::heed::RwTxn;
use milli::update::IndexerConfig;

use crate::{Error, Result};

const INDEX_MAPPING: &str = "index-mapping";

#[derive(Clone)]
pub struct IndexMapper {
    // Keep track of the opened indexes and is used
    // mainly by the index resolver.
    index_map: Arc<RwLock<HashMap<Uuid, Index>>>,

    // Map an index name with an index uuid currentl available on disk.
    index_mapping: Database<Str, SerdeBincode<Uuid>>,

    base_path: PathBuf,
    index_size: usize,
    indexer_config: Arc<IndexerConfig>,
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

    /// Get or create the index.
    pub fn create_index(&self, wtxn: &mut RwTxn, name: &str) -> Result<Index> {
        let index = match self.index(wtxn, name) {
            Ok(index) => index,
            Err(Error::IndexNotFound(_)) => {
                let uuid = Uuid::new_v4();
                self.index_mapping.put(wtxn, name, &uuid)?;
                Index::open(
                    self.base_path.join(uuid.to_string()),
                    name.to_string(),
                    self.index_size,
                    self.indexer_config.clone(),
                )?
            }
            error => return error,
        };

        Ok(index)
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
            Some(index) => index,
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
                        let index = Index::open(
                            self.base_path.join(uuid.to_string()),
                            name.to_string(),
                            self.index_size,
                            self.indexer_config.clone(),
                        )?;
                        entry.insert(index.clone());
                        index
                    }
                    Entry::Occupied(entry) => entry.get().clone(),
                }
            }
        };

        Ok(index)
    }

    pub fn indexes(&self, rtxn: &RoTxn) -> Result<Vec<Index>> {
        self.index_mapping
            .iter(rtxn)?
            .map(|ret| {
                ret.map_err(Error::from)
                    .and_then(|(name, _)| self.index(rtxn, name))
            })
            .collect()
    }

    /// Swap two index name.
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

    pub fn indexer_config(&self) -> &IndexerConfig {
        &self.indexer_config
    }
}

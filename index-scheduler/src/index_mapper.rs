use std::collections::hash_map::Entry;
use std::sync::Arc;

use index::Index;
use milli::heed::RoTxn;
use milli::heed::RwTxn;
use uuid::Uuid;

use crate::Error;
use crate::IndexScheduler;
use crate::Result;

impl IndexScheduler {
    pub fn create_index(&self, rwtxn: &mut RwTxn, name: &str) -> Result<Index> {
        let index = match self.index_txn(rwtxn, name) {
            Ok(index) => index,
            Err(Error::IndexNotFound(_)) => {
                let uuid = Uuid::new_v4();
                // TODO: TAMO: take the arguments from somewhere
                Index::open(uuid.to_string(), name.to_string(), 100000, Arc::default())?
            }
            error => return error,
        };

        Ok(index)
    }

    pub fn index_txn(&self, rtxn: &RoTxn, name: &str) -> Result<Index> {
        let uuid = self
            .index_mapping
            .get(&rtxn, name)?
            .ok_or(Error::IndexNotFound(name.to_string()))?;

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
                        // TODO: TAMO: get the args from somewhere.
                        let index = Index::open(
                            uuid.to_string(),
                            name.to_string(),
                            100_000_000,
                            Arc::default(),
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
}

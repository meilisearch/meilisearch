mod batch;
pub mod error;
pub mod task;
mod utils;

pub use error::Error;
use milli::heed::types::{DecodeIgnore, OwnedType, SerdeBincode, Str};
pub use task::Task;
use task::{Kind, Status};

use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

use milli::heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use milli::{Index, RoaringBitmapCodec, BEU32};
use roaring::RoaringBitmap;
use serde::Deserialize;

pub type Result<T> = std::result::Result<T, Error>;
pub type TaskId = u32;
type IndexName = String;
type IndexUuid = String;

const DEFAULT_LIMIT: fn() -> u32 = || 20;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    #[serde(default = "DEFAULT_LIMIT")]
    limit: u32,
    from: Option<u32>,
    status: Option<Vec<Status>>,
    #[serde(rename = "type")]
    kind: Option<Vec<Kind>>,
    index_uid: Option<Vec<String>>,
}

/// This module is responsible for two things;
/// 1. Resolve the name of the indexes.
/// 2. Schedule the tasks.
#[derive(Clone)]
pub struct IndexScheduler {
    // Keep track of the opened indexes and is used
    // mainly by the index resolver.
    index_map: Arc<RwLock<HashMap<IndexUuid, Index>>>,

    /// The list of tasks currently processing.
    processing_tasks: Arc<RwLock<RoaringBitmap>>,

    /// The LMDB environment which the DBs are associated with.
    env: Env,

    // The main database, it contains all the tasks accessible by their Id.
    all_tasks: Database<OwnedType<BEU32>, SerdeBincode<Task>>,

    // All the tasks ids grouped by their status.
    status: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
    // All the tasks ids grouped by their kind.
    kind: Database<SerdeBincode<Kind>, RoaringBitmapCodec>,

    // Map an index name with an indexuuid.
    index_name_mapper: Database<Str, Str>,
    // Store the tasks associated to an index.
    index_tasks: Database<Str, RoaringBitmapCodec>,

    // set to true when there is work to do.
    wake_up: Arc<AtomicBool>,
}

impl IndexScheduler {
    pub fn index(&self, name: &str) -> Result<Index> {
        let rtxn = self.env.read_txn()?;
        let uuid = self
            .index_name_mapper
            .get(&rtxn, name)?
            .ok_or(Error::IndexNotFound)?;
        // we clone here to drop the lock before entering the match
        let index = self.index_map.read().unwrap().get(&*uuid).cloned();
        let index = match index {
            Some(index) => index,
            // since we're lazy, it's possible that the index doesn't exist yet.
            // We need to open it ourselves.
            None => {
                let mut index_map = self.index_map.write().unwrap();
                // between the read lock and the write lock it's not impossible
                // that someone already opened the index (eg if two search happens
                // at the same time), thus before opening it we check a second time
                // if it's not already there.
                // Since there is a good chance it's not already there we can use
                // the entry method.
                match index_map.entry(uuid.to_string()) {
                    Entry::Vacant(entry) => {
                        // TODO: TAMO: get the envopenoptions from somewhere
                        let index = milli::Index::new(EnvOpenOptions::new(), uuid)?;
                        entry.insert(index.clone());
                        index
                    }
                    Entry::Occupied(entry) => entry.get().clone(),
                }
            }
        };

        Ok(index)
    }

    fn last_task_id(&self, rtxn: &RoTxn) -> Result<Option<TaskId>> {
        Ok(self
            .all_tasks
            .remap_data_type::<DecodeIgnore>()
            .last(rtxn)?
            .map(|(k, _)| k.get() + 1))
    }

    fn next_task_id(&self, rtxn: &RoTxn) -> Result<TaskId> {
        Ok(self.last_task_id(rtxn)?.unwrap_or_default())
    }

    /// Returns the tasks corresponding to the query.
    pub fn get_tasks(&self, query: Query) -> Result<Vec<Task>> {
        let rtxn = self.env.read_txn()?;
        let last_task_id = match self.last_task_id(&rtxn)? {
            Some(tid) => query.from.map(|from| from.min(tid)).unwrap_or(tid),
            None => return Ok(Vec::new()),
        };

        // This is the list of all the tasks.
        let mut tasks = RoaringBitmap::from_iter(0..last_task_id);

        if let Some(status) = query.status {
            let mut status_tasks = RoaringBitmap::new();
            for status in status {
                status_tasks |= self.get_status(&rtxn, status)?;
            }
            tasks &= status_tasks;
        }

        if let Some(kind) = query.kind {
            let mut kind_tasks = RoaringBitmap::new();
            for kind in kind {
                kind_tasks |= self.get_kind(&rtxn, kind)?;
            }
            tasks &= kind_tasks;
        }

        if let Some(index) = query.index_uid {
            let mut index_tasks = RoaringBitmap::new();
            for index in index {
                index_tasks |= self.get_index(&rtxn, &index)?;
            }
            tasks &= index_tasks;
        }

        self.get_existing_tasks(&rtxn, tasks.into_iter().rev().take(query.limit as usize))
    }

    /// Register a new task in the scheduler. If it fails and data was associated with the task
    /// it tries to delete the file.
    pub fn register(&self, task: Task) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;

        let task_id = self.next_task_id(&wtxn)?;

        self.all_tasks
            .append(&mut wtxn, &BEU32::new(task_id), &task)?;

        if let Some(indexes) = task.indexes() {
            for index in indexes {
                self.update_index(&mut wtxn, index, |mut bitmap| {
                    bitmap.insert(task_id);
                    bitmap
                })?;
            }
        }

        self.update_status(&mut wtxn, Status::Enqueued, |mut bitmap| {
            bitmap.insert(task_id);
            bitmap
        })?;

        self.update_kind(&mut wtxn, task.kind.as_kind(), |mut bitmap| {
            bitmap.insert(task_id);
            bitmap
        })?;

        // we persist the file in last to be sure everything before was applied successfuly
        task.persist()?;

        match wtxn.commit() {
            Ok(()) => (),
            e @ Err(_) => {
                task.remove_data()?;
                e?;
            }
        }

        self.notify();

        Ok(())
    }

    pub fn notify(&self) {
        self.wake_up
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

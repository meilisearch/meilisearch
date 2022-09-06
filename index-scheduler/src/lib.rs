pub mod error;
pub mod task;

use error::Error;
use milli::heed::types::{DecodeIgnore, OwnedType, SerdeBincode, Str};
pub use task::Task;
use task::{Kind, Status};

use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

use anyhow::Result;
use milli::heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use milli::{Index, RoaringBitmapCodec, BEU32};
use roaring::RoaringBitmap;

pub type TaskId = u32;
type IndexName = String;
type IndexUuid = String;

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
    kind: Database<OwnedType<BEU32>, RoaringBitmapCodec>,

    // Map an index name with an indexuuid.
    index_name_mapper: Database<Str, Str>,
    // Store the tasks associated to an index.
    index_tasks: Database<IndexName, RoaringBitmap>,

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

    fn next_task_id(&self, rtxn: &RoTxn) -> Result<TaskId> {
        Ok(self
            .all_tasks
            .remap_data_type::<DecodeIgnore>()
            .last(rtxn)?
            .map(|(k, _)| k.get())
            .unwrap_or(0))
    }

    /// Register a new task in the scheduler. If it fails and data was associated with the task
    /// it tries to delete the file.
    pub fn register(&self, task: Task) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;

        let task_id = self.next_task_id(&wtxn)?;

        self.all_tasks
            .append(&mut wtxn, &BEU32::new(task_id), &task)?;

        self.update_status(&mut wtxn, Status::Enqueued, |mut bitmap| {
            bitmap.insert(task_id);
            bitmap
        })?;

        self.update_kind(&mut wtxn, &task.kind, |mut bitmap| {
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

    fn update_status(
        &self,
        wtxn: &mut RwTxn,
        status: Status,
        f: impl Fn(RoaringBitmap) -> RoaringBitmap,
    ) -> Result<()> {
        let tasks = self.status.get(&wtxn, &status)?.unwrap_or_default();
        let tasks = f(tasks);
        self.status.put(wtxn, &status, &tasks)?;

        Ok(())
    }

    fn update_kind(
        &self,
        wtxn: &mut RwTxn,
        kind: &Kind,
        f: impl Fn(RoaringBitmap) -> RoaringBitmap,
    ) -> Result<()> {
        let kind = BEU32::new(kind.to_u32());
        let tasks = self.kind.get(&wtxn, &kind)?.unwrap_or_default();
        let tasks = f(tasks);
        self.kind.put(wtxn, &kind, &tasks)?;

        Ok(())
    }
}

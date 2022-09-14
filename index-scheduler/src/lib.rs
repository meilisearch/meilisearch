mod autobatcher;
mod batch;
pub mod error;
mod index_mapper;
pub mod task;
mod utils;


pub use error::Error;
use file_store::FileStore;
use index::Index;
use index_mapper::IndexMapper;
use synchronoise::SignalEvent;
pub use task::Task;
use task::{Kind, Status};





use std::sync::Arc;
use std::{sync::RwLock};

use milli::heed::types::{OwnedType, SerdeBincode, Str};
use milli::heed::{Database, Env};

use milli::{RoaringBitmapCodec, BEU32};
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
    /// The list of tasks currently processing.
    processing_tasks: Arc<RwLock<RoaringBitmap>>,

    file_store: FileStore,

    /// The LMDB environment which the DBs are associated with.
    env: Env,

    // The main database, it contains all the tasks accessible by their Id.
    all_tasks: Database<OwnedType<BEU32>, SerdeBincode<Task>>,

    /// All the tasks ids grouped by their status.
    status: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
    /// All the tasks ids grouped by their kind.
    kind: Database<SerdeBincode<Kind>, RoaringBitmapCodec>,
    /// Store the tasks associated to an index.
    index_tasks: Database<Str, RoaringBitmapCodec>,

    /// In charge of creating and returning indexes.
    index_mapper: IndexMapper,

    // set to true when there is work to do.
    wake_up: Arc<SignalEvent>,
}

impl IndexScheduler {
    pub fn new() -> Self {
        // we want to start the loop right away in case meilisearch was ctrl+Ced while processing things
        let _wake_up = SignalEvent::auto(true);
        todo!()
    }

    /// Return the index corresponding to the name. If it wasn't opened before
    /// it'll be opened. But if it doesn't exist on disk it'll throw an
    /// `IndexNotFound` error.
    pub fn index(&self, name: &str) -> Result<Index> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.index(&rtxn, name)
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
                self.update_index(&mut wtxn, index, |bitmap| drop(bitmap.insert(task_id)))?;
            }
        }

        self.update_status(&mut wtxn, Status::Enqueued, |bitmap| {
            bitmap.insert(task_id);
        })?;

        self.update_kind(&mut wtxn, task.kind.as_kind(), |bitmap| {
            (bitmap.insert(task_id));
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

    /// This worker function must be run in a different thread and must be run only once.
    fn run(&self) {
        loop {
            self.wake_up.wait();

            let mut wtxn = match self.env.write_txn() {
                Ok(wtxn) => wtxn,
                Err(e) => {
                    log::error!("{}", e);
                    continue;
                }
            };
            let batch = match self.create_next_batch(&wtxn) {
                Ok(Some(batch)) => batch,
                Ok(None) => continue,
                Err(e) => {
                    log::error!("{}", e);
                    continue;
                }
            };
            // 1. store the starting date with the bitmap of processing tasks
            // 2. update the tasks with a starting date *but* do not write anything on disk

            // 3. process the tasks
            let _res = self.process_batch(&mut wtxn, batch);

            // 4. store the updated tasks on disk

            // TODO: TAMO: do this later
            // must delete the file on disk
            // in case of error, must update the tasks with the error
            // in case of « success » we must update all the task on disk
            // self.handle_batch_result(res);

            match wtxn.commit() {
                Ok(()) => log::info!("A batch of tasks was successfully completed."),
                Err(e) => {
                    log::error!("{}", e);
                    continue;
                }
            }
        }
    }

    #[cfg(truc)]
    fn process_batch(&self, wtxn: &mut RwTxn, batch: &mut Batch) -> Result<()> {
        match batch {
            Batch::One(task) => match &task.kind {
                KindWithContent::ClearAllDocuments { index_name } => {
                    self.index(&index_name)?.clear_documents()?;
                }
                KindWithContent::RenameIndex {
                    index_name: _,
                    new_name,
                } => {
                    if self.available_index.get(wtxn, &new_name)?.unwrap_or(false) {
                        return Err(Error::IndexAlreadyExists(new_name.to_string()));
                    }
                    todo!("wait for @guigui insight");
                }
                KindWithContent::CreateIndex {
                    index_name,
                    primary_key,
                } => {
                    if self
                        .available_index
                        .get(wtxn, &index_name)?
                        .unwrap_or(false)
                    {
                        return Err(Error::IndexAlreadyExists(index_name.to_string()));
                    }

                    self.available_index.put(wtxn, &index_name, &true)?;
                    // TODO: TAMO: give real info to the index
                    let index = Index::open(
                        index_name.to_string(),
                        index_name.to_string(),
                        100_000_000,
                        Arc::default(),
                    )?;
                    if let Some(primary_key) = primary_key {
                        index.update_primary_key(primary_key.to_string())?;
                    }
                    self.index_map
                        .write()
                        .map_err(|_| Error::CorruptedTaskQueue)?
                        .insert(index_name.to_string(), index.clone());
                }
                KindWithContent::DeleteIndex { index_name } => {
                    if !self.available_index.delete(wtxn, &index_name)? {
                        return Err(Error::IndexNotFound(index_name.to_string()));
                    }
                    if let Some(index) = self
                        .index_map
                        .write()
                        .map_err(|_| Error::CorruptedTaskQueue)?
                        .remove(index_name)
                    {
                        index.delete()?;
                    } else {
                        // TODO: TAMO: fix the path
                        std::fs::remove_file(index_name)?;
                    }
                }
                KindWithContent::SwapIndex { lhs, rhs } => {
                    if !self.available_index.get(wtxn, &lhs)?.unwrap_or(false) {
                        return Err(Error::IndexNotFound(lhs.to_string()));
                    }
                    if !self.available_index.get(wtxn, &rhs)?.unwrap_or(false) {
                        return Err(Error::IndexNotFound(rhs.to_string()));
                    }

                    let lhs_bitmap = self.index_tasks.get(wtxn, lhs)?;
                    let rhs_bitmap = self.index_tasks.get(wtxn, rhs)?;
                    // the bitmap are lazily created and thus may not exists.
                    if let Some(bitmap) = rhs_bitmap {
                        self.index_tasks.put(wtxn, lhs, &bitmap)?;
                    }
                    if let Some(bitmap) = lhs_bitmap {
                        self.index_tasks.put(wtxn, rhs, &bitmap)?;
                    }

                    let mut index_map = self
                        .index_map
                        .write()
                        .map_err(|_| Error::CorruptedTaskQueue)?;

                    let lhs_index = index_map.remove(lhs).unwrap();
                    let rhs_index = index_map.remove(rhs).unwrap();

                    index_map.insert(lhs.to_string(), rhs_index);
                    index_map.insert(rhs.to_string(), lhs_index);
                }
                _ => unreachable!(),
            },
            Batch::Cancel(_) => todo!(),
            Batch::Snapshot(_) => todo!(),
            Batch::Dump(_) => todo!(),
            Batch::Contiguous { tasks, kind } => {
                // it's safe because you can't batch 0 contiguous tasks.
                let first_task = &tasks[0];
                // and the two kind of tasks we batch MUST have ONE index name.
                let index_name = first_task.indexes().unwrap()[0];
                let index = self.index(index_name)?;

                match kind {
                    Kind::DocumentAddition => {
                        let content_files = tasks.iter().map(|task| match &task.kind {
                            KindWithContent::DocumentAddition { content_file, .. } => {
                                content_file.clone()
                            }
                            k => unreachable!(
                                "Internal error, `{:?}` is not supposed to be reachable here",
                                k.as_kind()
                            ),
                        });
                        let results = index.update_documents(
                            IndexDocumentsMethod::UpdateDocuments,
                            None,
                            self.file_store.clone(),
                            content_files,
                        )?;

                        for (task, result) in tasks.iter_mut().zip(results) {
                            task.finished_at = Some(OffsetDateTime::now_utc());
                            match result {
                                Ok(_) => task.status = Status::Succeeded,
                                Err(_) => task.status = Status::Succeeded,
                            }
                        }
                    }
                    Kind::DocumentDeletion => {
                        let ids: Vec<_> = tasks
                            .iter()
                            .flat_map(|task| match &task.kind {
                                KindWithContent::DocumentDeletion { documents_ids, .. } => {
                                    documents_ids.clone()
                                }
                                k => unreachable!(
                                    "Internal error, `{:?}` is not supposed to be reachable here",
                                    k.as_kind()
                                ),
                            })
                            .collect();

                        let result = index.delete_documents(&ids);

                        for task in tasks.iter_mut() {
                            task.finished_at = Some(OffsetDateTime::now_utc());
                            match result {
                                Ok(_) => task.status = Status::Succeeded,
                                Err(_) => task.status = Status::Succeeded,
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Batch::Empty => todo!(),
        }

        Ok(())
    }

    /// Notify the scheduler there is or may be work to do.
    pub fn notify(&self) {
        self.wake_up.signal()
    }
}

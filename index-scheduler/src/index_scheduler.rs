use crate::index_mapper::IndexMapper;
use crate::task::{Kind, KindWithContent, Status, Task, TaskView};
use crate::{Error, Result};
use file_store::FileStore;
use index::Index;
use milli::update::IndexerConfig;
use synchronoise::SignalEvent;
use time::OffsetDateTime;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use milli::heed::types::{OwnedType, SerdeBincode, Str};
use milli::heed::{self, Database, Env};

use milli::{RoaringBitmapCodec, BEU32};
use roaring::RoaringBitmap;
use serde::Deserialize;

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

pub mod db_name {
    pub const ALL_TASKS: &str = "all-tasks";
    pub const STATUS: &str = "status";
    pub const KIND: &str = "kind";
    pub const INDEX_TASKS: &str = "index-tasks";

    pub const INDEX_MAPPING: &str = "index-mapping";
}

/// This module is responsible for two things;
/// 1. Resolve the name of the indexes.
/// 2. Schedule the tasks.
#[derive(Clone)]
pub struct IndexScheduler {
    /// The list of tasks currently processing and their starting date.
    pub(crate) processing_tasks: Arc<RwLock<(OffsetDateTime, RoaringBitmap)>>,

    pub(crate) file_store: FileStore,

    /// The LMDB environment which the DBs are associated with.
    pub(crate) env: Env,

    // The main database, it contains all the tasks accessible by their Id.
    pub(crate) all_tasks: Database<OwnedType<BEU32>, SerdeBincode<Task>>,

    /// All the tasks ids grouped by their status.
    pub(crate) status: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
    /// All the tasks ids grouped by their kind.
    pub(crate) kind: Database<SerdeBincode<Kind>, RoaringBitmapCodec>,
    /// Store the tasks associated to an index.
    pub(crate) index_tasks: Database<Str, RoaringBitmapCodec>,

    /// In charge of creating, opening, storing and returning indexes.
    pub(crate) index_mapper: IndexMapper,

    // set to true when there is work to do.
    pub(crate) wake_up: Arc<SignalEvent>,
}

impl IndexScheduler {
    pub fn new(
        db_path: PathBuf,
        update_file_path: PathBuf,
        indexes_path: PathBuf,
        index_size: usize,
        indexer_config: IndexerConfig,
    ) -> Result<Self> {
        std::fs::create_dir_all(&db_path)?;
        std::fs::create_dir_all(&update_file_path)?;
        std::fs::create_dir_all(&indexes_path)?;

        let mut options = heed::EnvOpenOptions::new();
        options.max_dbs(6);

        let env = options.open(db_path)?;
        // we want to start the loop right away in case meilisearch was ctrl+Ced while processing things
        let wake_up = SignalEvent::auto(true);

        let processing_tasks = (OffsetDateTime::now_utc(), RoaringBitmap::new());

        Ok(Self {
            // by default there is no processing tasks
            processing_tasks: Arc::new(RwLock::new(processing_tasks)),
            file_store: FileStore::new(update_file_path)?,
            all_tasks: env.create_database(Some(db_name::ALL_TASKS))?,
            status: env.create_database(Some(db_name::STATUS))?,
            kind: env.create_database(Some(db_name::KIND))?,
            index_tasks: env.create_database(Some(db_name::INDEX_TASKS))?,
            index_mapper: IndexMapper::new(&env, indexes_path, index_size, indexer_config)?,
            env,
            wake_up: Arc::new(wake_up),
        })
    }

    /// Return the index corresponding to the name. If it wasn't opened before
    /// it'll be opened. But if it doesn't exist on disk it'll throw an
    /// `IndexNotFound` error.
    pub fn index(&self, name: &str) -> Result<Index> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.index(&rtxn, name)
    }

    /// Returns the tasks corresponding to the query.
    pub fn get_tasks(&self, query: Query) -> Result<Vec<TaskView>> {
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

        let tasks =
            self.get_existing_tasks(&rtxn, tasks.into_iter().rev().take(query.limit as usize))?;
        Ok(tasks.into_iter().map(|task| task.as_task_view()).collect())
    }

    /// Register a new task in the scheduler. If it fails and data was associated with the task
    /// it tries to delete the file.
    pub fn register(&self, task: KindWithContent) -> Result<TaskView> {
        let mut wtxn = self.env.write_txn()?;

        let task = Task {
            uid: self.next_task_id(&wtxn)?,
            enqueued_at: time::OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            error: None,
            details: None,
            status: Status::Enqueued,
            kind: task,
        };

        self.all_tasks
            .append(&mut wtxn, &BEU32::new(task.uid), &task)?;

        if let Some(indexes) = task.indexes() {
            for index in indexes {
                self.update_index(&mut wtxn, index, |bitmap| drop(bitmap.insert(task.uid)))?;
            }
        }

        self.update_status(&mut wtxn, Status::Enqueued, |bitmap| {
            bitmap.insert(task.uid);
        })?;

        self.update_kind(&mut wtxn, task.kind.as_kind(), |bitmap| {
            (bitmap.insert(task.uid));
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

        Ok(task.as_task_view())
    }

    /// This worker function must be run in a different thread and must be run only once.
    pub fn run(&self) -> ! {
        loop {
            self.wake_up.wait();

            match self.tick() {
                Ok(()) => (),
                Err(e) => log::error!("{}", e),
            }
        }
    }

    /// Create and execute and store the result of one batch of registered tasks.
    fn tick(&self) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        let batch = match self.create_next_batch(&wtxn)? {
            Some(batch) => batch,
            None => return Ok(()),
        };

        // 1. store the starting date with the bitmap of processing tasks.
        let mut ids = batch.ids();
        ids.sort_unstable();
        let processing_tasks = RoaringBitmap::from_sorted_iter(ids.iter().copied()).unwrap();
        let started_at = OffsetDateTime::now_utc();
        *self.processing_tasks.write().unwrap() = (started_at, processing_tasks);

        // 2. process the tasks
        let res = self.process_batch(&mut wtxn, batch);

        let finished_at = OffsetDateTime::now_utc();
        match res {
            Ok(tasks) => {
                for mut task in tasks {
                    task.started_at = Some(started_at);
                    task.finished_at = Some(finished_at);
                    task.status = Status::Succeeded;
                    // the info field should've been set by the process_batch function

                    self.update_task(&mut wtxn, &task)?;
                    task.remove_data()?;
                }
            }
            // In case of a failure we must get back and patch all the tasks with the error.
            Err(_err) => {
                for id in ids {
                    let mut task = self.get_task(&wtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                    task.started_at = Some(started_at);
                    task.finished_at = Some(finished_at);
                    task.status = Status::Failed;
                    // TODO: TAMO: set the error correctly
                    // task.error = Some(err);

                    self.update_task(&mut wtxn, &task)?;
                    task.remove_data()?;
                }
            }
        }

        // TODO: TAMO: do this later
        // must delete the file on disk
        // in case of error, must update the tasks with the error
        // in case of « success » we must update all the task on disk
        // self.handle_batch_result(res);

        wtxn.commit()?;
        log::info!("A batch of tasks was successfully completed.");

        Ok(())
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

#[cfg(test)]
mod tests {
    use big_s::S;
    use insta::assert_debug_snapshot;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::assert_smol_debug_snapshot;

    use super::*;

    fn new() -> IndexScheduler {
        let dir = TempDir::new().unwrap();
        IndexScheduler::new(
            dir.path().join("db_path"),
            dir.path().join("file_store"),
            dir.path().join("indexes"),
            100_000_000,
            IndexerConfig::default(),
        )
        .unwrap()
    }

    #[test]
    fn simple_new() {
        new();
    }

    #[test]
    fn register() {
        let index_scheduler = new();
        let kinds = [
            KindWithContent::IndexCreation {
                index_uid: S("catto"),
                primary_key: Some(S("mouse")),
            },
            KindWithContent::DocumentAddition {
                index_uid: S("catto"),
                primary_key: None,
                content_file: Uuid::new_v4(),
                documents_count: 12,
                allow_index_creation: true,
            },
            KindWithContent::CancelTask { tasks: vec![0, 1] },
            KindWithContent::DocumentAddition {
                index_uid: S("catto"),
                primary_key: None,
                content_file: Uuid::new_v4(),
                documents_count: 50,
                allow_index_creation: true,
            },
            KindWithContent::DocumentAddition {
                index_uid: S("doggo"),
                primary_key: Some(S("bone")),
                content_file: Uuid::new_v4(),
                documents_count: 5000,
                allow_index_creation: true,
            },
        ];
        let mut inserted_tasks = Vec::new();
        for (idx, kind) in kinds.into_iter().enumerate() {
            let k = kind.as_kind();
            let task = index_scheduler.register(kind).unwrap();

            assert_eq!(task.uid, idx as u32);
            assert_eq!(task.status, Status::Enqueued);
            assert_eq!(task.kind, k);

            inserted_tasks.push(task);
        }

        let rtxn = index_scheduler.env.read_txn().unwrap();
        let mut all_tasks = Vec::new();
        for ret in index_scheduler.all_tasks.iter(&rtxn).unwrap() {
            all_tasks.push(ret.unwrap().0);
        }

        // we can't assert on the content of the tasks because there is the date and uuid that changes everytime.
        assert_smol_debug_snapshot!(all_tasks, @"[U32(0), U32(1), U32(2), U32(3), U32(4)]");

        let mut status = Vec::new();
        for ret in index_scheduler.status.iter(&rtxn).unwrap() {
            status.push(ret.unwrap());
        }

        assert_smol_debug_snapshot!(status, @"[(Enqueued, RoaringBitmap<[0, 1, 2, 3, 4]>)]");

        let mut kind = Vec::new();
        for ret in index_scheduler.kind.iter(&rtxn).unwrap() {
            kind.push(ret.unwrap());
        }

        assert_smol_debug_snapshot!(kind, @"[(DocumentAddition, RoaringBitmap<[1, 3, 4]>), (IndexCreation, RoaringBitmap<[0]>), (CancelTask, RoaringBitmap<[2]>)]");

        let mut index_tasks = Vec::new();
        for ret in index_scheduler.index_tasks.iter(&rtxn).unwrap() {
            index_tasks.push(ret.unwrap());
        }

        assert_smol_debug_snapshot!(index_tasks, @r###"[("catto", RoaringBitmap<[0, 1, 3]>), ("doggo", RoaringBitmap<[4]>)]"###);
    }
}

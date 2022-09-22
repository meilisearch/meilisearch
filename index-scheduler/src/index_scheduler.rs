use crate::index_mapper::IndexMapper;
use crate::task::{Kind, KindWithContent, Status, Task, TaskView};
use crate::{Error, Result, TaskId};
use file_store::{File, FileStore};
use index::Index;
use milli::update::IndexerConfig;
use synchronoise::SignalEvent;
use time::OffsetDateTime;
use uuid::Uuid;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use milli::heed::types::{OwnedType, SerdeBincode, Str};
use milli::heed::{self, Database, Env};

use milli::{RoaringBitmapCodec, BEU32};
use roaring::RoaringBitmap;
use serde::Deserialize;

const DEFAULT_LIMIT: fn() -> u32 = || 20;

#[derive(derive_builder::Builder, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    #[serde(default = "DEFAULT_LIMIT")]
    pub limit: u32,
    pub from: Option<u32>,
    pub status: Option<Vec<Status>>,
    #[serde(rename = "type")]
    pub kind: Option<Vec<Kind>>,
    pub index_uid: Option<Vec<String>>,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            limit: DEFAULT_LIMIT(),
            from: None,
            status: None,
            kind: None,
            index_uid: None,
        }
    }
}

impl Query {
    pub fn with_status(self, status: Status) -> Self {
        let mut status_vec = self.status.unwrap_or_default();
        status_vec.push(status);
        Self {
            status: Some(status_vec),
            ..self
        }
    }

    pub fn with_kind(self, kind: Kind) -> Self {
        let mut kind_vec = self.kind.unwrap_or_default();
        kind_vec.push(kind);
        Self {
            kind: Some(kind_vec),
            ..self
        }
    }

    pub fn with_index(self, index_uid: String) -> Self {
        let mut index_vec = self.index_uid.unwrap_or_default();
        index_vec.push(index_uid);
        Self {
            index_uid: Some(index_vec),
            ..self
        }
    }
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
        tasks_path: PathBuf,
        update_file_path: PathBuf,
        indexes_path: PathBuf,
        index_size: usize,
        indexer_config: IndexerConfig,
    ) -> Result<Self> {
        std::fs::create_dir_all(&tasks_path)?;
        std::fs::create_dir_all(&update_file_path)?;
        std::fs::create_dir_all(&indexes_path)?;

        let mut options = heed::EnvOpenOptions::new();
        options.max_dbs(6);

        let env = options.open(tasks_path)?;
        // we want to start the loop right away in case meilisearch was ctrl+Ced while processing things
        let wake_up = SignalEvent::auto(true);

        let processing_tasks = (OffsetDateTime::now_utc(), RoaringBitmap::new());
        let file_store = FileStore::new(&update_file_path)?;

        Ok(Self {
            // by default there is no processing tasks
            processing_tasks: Arc::new(RwLock::new(processing_tasks)),
            file_store,
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

    /// Return and open all the indexes.
    pub fn indexes(&self) -> Result<Vec<Index>> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.indexes(&rtxn)
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

    pub fn create_update_file(&self) -> Result<(Uuid, File)> {
        Ok(self.file_store.new_update()?)
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

    /// Notify the scheduler there is or may be work to do.
    pub fn notify(&self) {
        self.wake_up.signal()
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use insta::*;
    use uuid::Uuid;

    use crate::{assert_smol_debug_snapshot, tests::index_scheduler};

    use super::*;

    #[test]
    fn register() {
        let (index_scheduler, _) = index_scheduler();
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

    #[test]
    fn document_addition() {
        let (index_scheduler, _dir) = index_scheduler();

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file().unwrap();
        document_formats::read_json(content.as_bytes(), file.as_file_mut()).unwrap();
        index_scheduler
            .register(KindWithContent::DocumentAddition {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                content_file: uuid,
                documents_count: 100,
                allow_index_creation: true,
            })
            .unwrap();
        file.persist().unwrap();

        index_scheduler.tick().unwrap();

        let doggos = index_scheduler.index("doggos").unwrap();

        let rtxn = doggos.read_txn().unwrap();
        let documents: Vec<_> = doggos
            .all_documents(&rtxn)
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();

        assert_smol_debug_snapshot!(documents, @r###"[{"id": Number(1), "doggo": String("bob")}]"###);
    }
}

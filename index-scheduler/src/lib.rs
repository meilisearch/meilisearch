mod autobatcher;
mod batch;
pub mod error;
mod index_mapper;
#[cfg(test)]
mod snapshot;
mod utils;

pub type Result<T> = std::result::Result<T, Error>;
pub type TaskId = u32;

use dump::{KindDump, TaskDump, UpdateFile};
pub use error::Error;
use meilisearch_types::milli::documents::DocumentsBatchBuilder;
use meilisearch_types::tasks::{Kind, KindWithContent, Status, Task};

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use file_store::FileStore;
use meilisearch_types::error::ResponseError;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use synchronoise::SignalEvent;
use time::OffsetDateTime;
use uuid::Uuid;

use meilisearch_types::heed::types::{OwnedType, SerdeBincode, SerdeJson, Str};
use meilisearch_types::heed::{self, Database, Env};
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::{Index, RoaringBitmapCodec, BEU32};

use crate::index_mapper::IndexMapper;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub limit: Option<u32>,
    pub from: Option<u32>,
    pub status: Option<Vec<Status>>,
    #[serde(rename = "type")]
    pub kind: Option<Vec<Kind>>,
    pub index_uid: Option<Vec<String>>,
    pub uid: Option<Vec<TaskId>>,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            limit: None,
            from: None,
            status: None,
            kind: None,
            index_uid: None,
            uid: None,
        }
    }
}

impl Query {
    /// Return `true` iff every field of the query is set to `None`, such that the query
    /// would match all tasks.
    pub fn is_empty(&self) -> bool {
        matches!(
            self,
            Query {
                limit: None,
                from: None,
                status: None,
                kind: None,
                index_uid: None,
                uid: None
            }
        )
    }
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

    pub fn with_uid(self, uid: TaskId) -> Self {
        let mut task_vec = self.uid.unwrap_or_default();
        task_vec.push(uid);
        Self {
            uid: Some(task_vec),
            ..self
        }
    }

    pub fn with_limit(self, limit: u32) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }
}

/// Database const names for the `IndexScheduler`.
mod db_name {
    pub const ALL_TASKS: &str = "all-tasks";
    pub const STATUS: &str = "status";
    pub const KIND: &str = "kind";
    pub const INDEX_TASKS: &str = "index-tasks";
}

/// This module is responsible for two things;
/// 1. Resolve the name of the indexes.
/// 2. Schedule the tasks.
pub struct IndexScheduler {
    /// The list of tasks currently processing and their starting date.
    pub(crate) processing_tasks: Arc<RwLock<(OffsetDateTime, RoaringBitmap)>>,

    pub(crate) file_store: FileStore,

    /// The LMDB environment which the DBs are associated with.
    pub(crate) env: Env,

    // The main database, it contains all the tasks accessible by their Id.
    pub(crate) all_tasks: Database<OwnedType<BEU32>, SerdeJson<Task>>,

    /// All the tasks ids grouped by their status.
    pub(crate) status: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
    /// All the tasks ids grouped by their kind.
    pub(crate) kind: Database<SerdeBincode<Kind>, RoaringBitmapCodec>,
    /// Store the tasks associated to an index.
    pub(crate) index_tasks: Database<Str, RoaringBitmapCodec>,

    /// In charge of creating, opening, storing and returning indexes.
    pub(crate) index_mapper: IndexMapper,

    /// Get a signal when a batch needs to be processed.
    pub(crate) wake_up: Arc<SignalEvent>,

    /// Weither autobatching is enabled or not.
    pub(crate) autobatching_enabled: bool,

    /// The path used to create the dumps.
    pub(crate) dumps_path: PathBuf,

    // ================= test
    /// The next entry is dedicated to the tests.
    /// It provide a way to break in multiple part of the scheduler.
    #[cfg(test)]
    test_breakpoint_sdr: crossbeam::channel::Sender<Breakpoint>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Breakpoint {
    Start,
    BatchCreated,
    BeforeProcessing,
    AfterProcessing,
}

impl IndexScheduler {
    pub fn new(
        tasks_path: PathBuf,
        update_file_path: PathBuf,
        indexes_path: PathBuf,
        dumps_path: PathBuf,
        index_size: usize,
        indexer_config: IndexerConfig,
        autobatching_enabled: bool,
        #[cfg(test)] test_breakpoint_sdr: crossbeam::channel::Sender<Breakpoint>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&tasks_path)?;
        std::fs::create_dir_all(&update_file_path)?;
        std::fs::create_dir_all(&indexes_path)?;
        std::fs::create_dir_all(&dumps_path)?;

        let mut options = heed::EnvOpenOptions::new();
        options.max_dbs(6);

        let env = options.open(tasks_path)?;
        let processing_tasks = (OffsetDateTime::now_utc(), RoaringBitmap::new());
        let file_store = FileStore::new(&update_file_path)?;

        // allow unreachable_code to get rids of the warning in the case of a test build.
        let this = Self {
            // by default there is no processing tasks
            processing_tasks: Arc::new(RwLock::new(processing_tasks)),
            file_store,
            all_tasks: env.create_database(Some(db_name::ALL_TASKS))?,
            status: env.create_database(Some(db_name::STATUS))?,
            kind: env.create_database(Some(db_name::KIND))?,
            index_tasks: env.create_database(Some(db_name::INDEX_TASKS))?,
            index_mapper: IndexMapper::new(&env, indexes_path, index_size, indexer_config)?,
            env,
            // we want to start the loop right away in case meilisearch was ctrl+Ced while processing things
            wake_up: Arc::new(SignalEvent::auto(true)),
            autobatching_enabled,
            dumps_path,

            #[cfg(test)]
            test_breakpoint_sdr,
        };

        this.run();
        Ok(this)
    }

    /// This function will execute in a different thread and must be called only once.
    fn run(&self) {
        let run = Self {
            processing_tasks: self.processing_tasks.clone(),
            file_store: self.file_store.clone(),
            env: self.env.clone(),
            all_tasks: self.all_tasks,
            status: self.status,
            kind: self.kind,
            index_tasks: self.index_tasks,
            index_mapper: self.index_mapper.clone(),
            wake_up: self.wake_up.clone(),
            autobatching_enabled: self.autobatching_enabled,
            dumps_path: self.dumps_path.clone(),

            #[cfg(test)]
            test_breakpoint_sdr: self.test_breakpoint_sdr.clone(),
        };

        std::thread::spawn(move || loop {
            run.wake_up.wait();

            match run.tick() {
                Ok(0) => (),
                Ok(_) => run.wake_up.signal(),
                Err(e) => log::error!("{}", e),
            }
        });
    }

    pub fn indexer_config(&self) -> &IndexerConfig {
        &self.index_mapper.indexer_config
    }

    /// Return the index corresponding to the name. If it wasn't opened before
    /// it'll be opened. But if it doesn't exist on disk it'll throw an
    /// `IndexNotFound` error.
    pub fn index(&self, name: &str) -> Result<Index> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.index(&rtxn, name)
    }

    /// Return and open all the indexes.
    pub fn indexes(&self) -> Result<Vec<(String, Index)>> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.indexes(&rtxn)
    }

    /// Return the task ids corresponding to the query
    pub fn get_task_ids(&self, query: &Query) -> Result<RoaringBitmap> {
        let rtxn = self.env.read_txn()?;

        // This is the list of all the tasks.
        let mut tasks = self.all_task_ids(&rtxn)?;

        if let Some(uids) = &query.uid {
            tasks &= RoaringBitmap::from_iter(uids);
        }

        if let Some(status) = &query.status {
            let mut status_tasks = RoaringBitmap::new();
            for status in status {
                status_tasks |= self.get_status(&rtxn, *status)?;
            }
            tasks &= status_tasks;
        }

        if let Some(kind) = &query.kind {
            let mut kind_tasks = RoaringBitmap::new();
            for kind in kind {
                kind_tasks |= self.get_kind(&rtxn, *kind)?;
            }
            tasks &= kind_tasks;
        }

        if let Some(index) = &query.index_uid {
            let mut index_tasks = RoaringBitmap::new();
            for index in index {
                index_tasks |= self.index_tasks(&rtxn, &index)?;
            }
            tasks &= index_tasks;
        }
        rtxn.commit().unwrap();
        Ok(tasks)
    }

    /// Returns the tasks corresponding to the query.
    pub fn get_tasks(&self, query: Query) -> Result<Vec<Task>> {
        let tasks = self.get_task_ids(&query)?;
        let rtxn = self.env.read_txn()?;

        let tasks = self.get_existing_tasks(
            &rtxn,
            tasks
                .into_iter()
                .rev()
                .take(query.limit.unwrap_or(u32::MAX) as usize),
        )?;

        let (started_at, processing) = self
            .processing_tasks
            .read()
            .map_err(|_| Error::CorruptedTaskQueue)?
            .clone();

        let ret = tasks.into_iter();
        if processing.is_empty() {
            Ok(ret.collect())
        } else {
            Ok(ret
                .map(|task| match processing.contains(task.uid) {
                    true => Task {
                        status: Status::Processing,
                        started_at: Some(started_at),
                        ..task
                    },
                    false => task,
                })
                .collect())
        }
    }

    /// Register a new task in the scheduler. If it fails and data was associated with the task
    /// it tries to delete the file.
    pub fn register(&self, task: KindWithContent) -> Result<Task> {
        let mut wtxn = self.env.write_txn()?;

        let task = Task {
            uid: self.next_task_id(&wtxn)?,
            enqueued_at: time::OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            error: None,
            details: (&task).into(),
            status: Status::Enqueued,
            kind: task,
        };
        self.all_tasks
            .append(&mut wtxn, &BEU32::new(task.uid), &task)?;

        if let Some(indexes) = task.indexes() {
            for index in indexes {
                self.update_index(&mut wtxn, index, |bitmap| {
                    bitmap.insert(task.uid);
                })?;
            }
        }

        self.update_status(&mut wtxn, Status::Enqueued, |bitmap| {
            bitmap.insert(task.uid);
        })?;

        self.update_kind(&mut wtxn, task.kind.as_kind(), |bitmap| {
            (bitmap.insert(task.uid));
        })?;

        match wtxn.commit() {
            Ok(()) => (),
            _e @ Err(_) => {
                self.delete_persisted_task_data(&task)?;
                // _e?;
            }
        }

        // notify the scheduler loop to execute a new tick
        self.wake_up.signal();

        Ok(task)
    }

    /// Register a new task comming from a dump in the scheduler.
    /// By takinig a mutable ref we're pretty sure no one will ever import a dump while actix is running.
    pub fn register_dumped_task(
        &mut self,
        task: TaskDump,
        content_file: Option<Box<UpdateFile>>,
    ) -> Result<Task> {
        // Currently we don't need to access the tasks queue while loading a dump thus I can block everything.
        let mut wtxn = self.env.write_txn()?;

        let content_uuid = match content_file {
            Some(content_file) if task.status == Status::Enqueued => {
                let (uuid, mut file) = self.create_update_file()?;
                let mut builder = DocumentsBatchBuilder::new(file.as_file_mut());
                for doc in content_file {
                    builder.append_json_object(&doc?)?;
                }
                builder.into_inner()?;
                file.persist()?;

                Some(uuid)
            }
            // If the task isn't `Enqueued` then just generate a recognisable `Uuid`
            // in case we try to open it later.
            _ if task.status != Status::Enqueued => Some(Uuid::nil()),
            _ => None,
        };

        let task = Task {
            uid: task.uid,
            enqueued_at: task.enqueued_at,
            started_at: task.started_at,
            finished_at: task.finished_at,
            error: task.error,
            details: task.details,
            status: task.status,
            kind: match task.kind {
                KindDump::DocumentImport {
                    primary_key,
                    method,
                    documents_count,
                    allow_index_creation,
                } => KindWithContent::DocumentImport {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                    method,
                    content_file: content_uuid.ok_or(Error::CorruptedDump)?,
                    documents_count,
                    allow_index_creation,
                },
                KindDump::DocumentDeletion { documents_ids } => KindWithContent::DocumentDeletion {
                    documents_ids,
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::DocumentClear => KindWithContent::DocumentClear {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::Settings {
                    settings,
                    is_deletion,
                    allow_index_creation,
                } => KindWithContent::Settings {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    new_settings: settings,
                    is_deletion,
                    allow_index_creation,
                },
                KindDump::IndexDeletion => KindWithContent::IndexDeletion {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::IndexCreation { primary_key } => KindWithContent::IndexCreation {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                },
                KindDump::IndexUpdate { primary_key } => KindWithContent::IndexUpdate {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                },
                KindDump::IndexSwap { lhs, rhs } => KindWithContent::IndexSwap { lhs, rhs },
                KindDump::CancelTask { tasks } => KindWithContent::CancelTask { tasks },
                KindDump::DeleteTasks { query, tasks } => {
                    KindWithContent::TaskDeletion { query, tasks }
                }
                KindDump::DumpExport {
                    dump_uid,
                    keys,
                    instance_uid,
                } => KindWithContent::DumpExport {
                    dump_uid,
                    keys,
                    instance_uid,
                },
                KindDump::Snapshot => KindWithContent::Snapshot,
            },
        };

        self.all_tasks
            .put(&mut wtxn, &BEU32::new(task.uid), &task)?;

        if let Some(indexes) = task.indexes() {
            for index in indexes {
                self.update_index(&mut wtxn, index, |bitmap| {
                    bitmap.insert(task.uid);
                })?;
            }
        }

        self.update_status(&mut wtxn, task.status, |bitmap| {
            bitmap.insert(task.uid);
        })?;

        self.update_kind(&mut wtxn, task.kind.as_kind(), |bitmap| {
            (bitmap.insert(task.uid));
        })?;

        wtxn.commit()?;
        self.wake_up.signal();

        Ok(task)
    }

    /// Create a new index without any associated task.
    pub fn create_raw_index(&self, name: &str) -> Result<Index> {
        let mut wtxn = self.env.write_txn()?;
        let index = self.index_mapper.create_index(&mut wtxn, name)?;
        wtxn.commit()?;

        Ok(index)
    }

    pub fn create_update_file(&self) -> Result<(Uuid, file_store::File)> {
        Ok(self.file_store.new_update()?)
    }

    #[cfg(test)]
    pub fn create_update_file_with_uuid(&self, uuid: u128) -> Result<(Uuid, file_store::File)> {
        Ok(self.file_store.new_update_with_uuid(uuid)?)
    }

    pub fn delete_update_file(&self, uuid: Uuid) -> Result<()> {
        Ok(self.file_store.delete(uuid)?)
    }

    /// Create and execute and store the result of one batch of registered tasks.
    ///
    /// Returns the number of processed tasks.
    fn tick(&self) -> Result<usize> {
        #[cfg(test)]
        self.test_breakpoint_sdr.send(Breakpoint::Start).unwrap();

        let rtxn = self.env.read_txn()?;
        let batch = match self.create_next_batch(&rtxn)? {
            Some(batch) => batch,
            None => return Ok(0),
        };
        // we don't need this transaction any longer.
        drop(rtxn);

        // 1. store the starting date with the bitmap of processing tasks.
        let mut ids = batch.ids();
        ids.sort_unstable();
        let processed_tasks = ids.len();
        let processing_tasks = RoaringBitmap::from_sorted_iter(ids.iter().copied()).unwrap();
        let started_at = OffsetDateTime::now_utc();
        *self.processing_tasks.write().unwrap() = (started_at, processing_tasks);

        #[cfg(test)]
        {
            self.test_breakpoint_sdr
                .send(Breakpoint::BatchCreated)
                .unwrap();
            self.test_breakpoint_sdr
                .send(Breakpoint::BeforeProcessing)
                .unwrap();
        }

        // 2. Process the tasks
        let res = self.process_batch(batch);
        let mut wtxn = self.env.write_txn()?;
        let finished_at = OffsetDateTime::now_utc();
        match res {
            Ok(tasks) => {
                for mut task in tasks {
                    task.started_at = Some(started_at);
                    task.finished_at = Some(finished_at);
                    self.update_task(&mut wtxn, &task)?;
                    self.delete_persisted_task_data(&task)?;
                }
                log::info!("A batch of tasks was successfully completed.");
            }
            // In case of a failure we must get back and patch all the tasks with the error.
            Err(err) => {
                let error: ResponseError = err.into();
                for id in ids {
                    let mut task = self.get_task(&wtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                    task.started_at = Some(started_at);
                    task.finished_at = Some(finished_at);
                    task.status = Status::Failed;
                    task.error = Some(error.clone());

                    self.update_task(&mut wtxn, &task)?;
                }
            }
        }
        *self.processing_tasks.write().unwrap() = (finished_at, RoaringBitmap::new());
        wtxn.commit()?;

        #[cfg(test)]
        self.test_breakpoint_sdr
            .send(Breakpoint::AfterProcessing)
            .unwrap();

        Ok(processed_tasks)
    }

    pub(crate) fn delete_persisted_task_data(&self, task: &Task) -> Result<()> {
        match &task.kind {
            KindWithContent::DocumentImport { content_file, .. } => {
                self.delete_update_file(*content_file)
            }
            KindWithContent::DocumentDeletion { .. }
            | KindWithContent::DocumentClear { .. }
            | KindWithContent::Settings { .. }
            | KindWithContent::IndexDeletion { .. }
            | KindWithContent::IndexCreation { .. }
            | KindWithContent::IndexUpdate { .. }
            | KindWithContent::IndexSwap { .. }
            | KindWithContent::CancelTask { .. }
            | KindWithContent::TaskDeletion { .. }
            | KindWithContent::DumpExport { .. }
            | KindWithContent::Snapshot => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use file_store::File;
    use meili_snap::snapshot;
    use meilisearch_types::milli::update::IndexDocumentsMethod::ReplaceDocuments;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::snapshot::snapshot_index_scheduler;

    use super::*;

    /// Return a `KindWithContent::IndexCreation` task
    fn index_creation_task(index: &'static str, primary_key: &'static str) -> KindWithContent {
        KindWithContent::IndexCreation {
            index_uid: S(index),
            primary_key: Some(S(primary_key)),
        }
    }
    /// Create a `KindWithContent::DocumentImport` task that imports documents.
    ///
    /// - `index_uid` is given as parameter
    /// - `primary_key` is given as parameter
    /// - `method` is set to `ReplaceDocuments`
    /// - `content_file` is given as parameter
    /// - `documents_count` is given as parameter
    /// - `allow_index_creation` is set to `true`
    fn replace_document_import_task(
        index: &'static str,
        primary_key: Option<&'static str>,
        content_file_uuid: u128,
        documents_count: u64,
    ) -> KindWithContent {
        KindWithContent::DocumentImport {
            index_uid: S(index),
            primary_key: primary_key.map(ToOwned::to_owned),
            method: ReplaceDocuments,
            content_file: Uuid::from_u128(content_file_uuid),
            documents_count: documents_count,
            allow_index_creation: true,
        }
    }

    /// Create an update file with the given file uuid.
    ///
    /// The update file contains just one simple document whose id is given by `document_id`.
    ///
    /// The uuid of the file and its documents count is returned.
    fn sample_documents(
        index_scheduler: &IndexScheduler,
        file_uuid: u128,
        document_id: usize,
    ) -> (File, u64) {
        let content = format!(
            r#"
        {{
            "id" : "{document_id}"
        }}"#
        );

        let (_uuid, mut file) = index_scheduler
            .create_update_file_with_uuid(file_uuid)
            .unwrap();
        let documents_count =
            meilisearch_types::document_formats::read_json(content.as_bytes(), file.as_file_mut())
                .unwrap() as u64;
        (file, documents_count)
    }

    impl IndexScheduler {
        pub fn test(autobatching: bool) -> (Self, IndexSchedulerHandle) {
            let tempdir = TempDir::new().unwrap();
            let (sender, receiver) = crossbeam::channel::bounded(0);

            let index_scheduler = Self::new(
                tempdir.path().join("db_path"),
                tempdir.path().join("file_store"),
                tempdir.path().join("indexes"),
                tempdir.path().join("dumps"),
                1024 * 1024,
                IndexerConfig::default(),
                autobatching, // enable autobatching
                sender,
            )
            .unwrap();

            let index_scheduler_handle = IndexSchedulerHandle {
                _tempdir: tempdir,
                test_breakpoint_rcv: receiver,
            };

            (index_scheduler, index_scheduler_handle)
        }
    }

    pub struct IndexSchedulerHandle {
        _tempdir: TempDir,
        test_breakpoint_rcv: crossbeam::channel::Receiver<Breakpoint>,
    }

    impl IndexSchedulerHandle {
        /// Wait until the provided breakpoint is reached.
        fn wait_till(&self, breakpoint: Breakpoint) {
            self.test_breakpoint_rcv.iter().find(|b| *b == breakpoint);
        }

        #[allow(unused)]
        /// Wait until the provided breakpoint is reached.
        fn next_breakpoint(&self) -> Breakpoint {
            self.test_breakpoint_rcv.recv().unwrap()
        }

        /// The scheduler will not stop on breakpoints anymore.
        fn dont_block(self) {
            std::thread::spawn(move || loop {
                // unroll and ignore all the state the scheduler is going to send us.
                self.test_breakpoint_rcv.iter().last();
            });
        }
    }

    #[test]
    fn register() {
        // In this test, the handle doesn't make any progress, we only check that the tasks are registered
        let (index_scheduler, _handle) = IndexScheduler::test(true);

        let kinds = [
            index_creation_task("catto", "mouse"),
            replace_document_import_task("catto", None, 0, 12),
            KindWithContent::CancelTask { tasks: vec![0, 1] },
            replace_document_import_task("catto", None, 1, 50),
            replace_document_import_task("doggo", Some("bone"), 2, 5000),
        ];
        for (idx, kind) in kinds.into_iter().enumerate() {
            let k = kind.as_kind();
            let task = index_scheduler.register(kind).unwrap();

            assert_eq!(task.uid, idx as u32);
            assert_eq!(task.status, Status::Enqueued);
            assert_eq!(task.kind.as_kind(), k);
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler));
    }

    #[test]
    fn insert_task_while_another_task_is_processing() {
        let (index_scheduler, handle) = IndexScheduler::test(true);

        index_scheduler.register(KindWithContent::Snapshot).unwrap();
        handle.wait_till(Breakpoint::BatchCreated);
        // while the task is processing can we register another task?
        index_scheduler.register(KindWithContent::Snapshot).unwrap();
        index_scheduler
            .register(KindWithContent::IndexDeletion {
                index_uid: S("doggos"),
            })
            .unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler));
    }

    /// We send a lot of tasks but notify the tasks scheduler only once as
    /// we send them very fast, we must make sure that they are all processed.
    #[test]
    fn process_tasks_inserted_without_new_signal() {
        let (index_scheduler, handle) = IndexScheduler::test(true);

        index_scheduler
            .register(KindWithContent::IndexCreation {
                index_uid: S("doggos"),
                primary_key: None,
            })
            .unwrap();
        index_scheduler
            .register(KindWithContent::IndexCreation {
                index_uid: S("cattos"),
                primary_key: None,
            })
            .unwrap();
        index_scheduler
            .register(KindWithContent::IndexDeletion {
                index_uid: S("doggos"),
            })
            .unwrap();

        handle.wait_till(Breakpoint::Start);
        handle.wait_till(Breakpoint::AfterProcessing);
        handle.wait_till(Breakpoint::AfterProcessing);
        handle.wait_till(Breakpoint::AfterProcessing);

        let mut tasks = index_scheduler.get_tasks(Query::default()).unwrap();
        tasks.reverse();
        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].status, Status::Succeeded);
        assert_eq!(tasks[1].status, Status::Succeeded);
        assert_eq!(tasks[2].status, Status::Succeeded);
    }

    #[test]
    fn process_tasks_without_autobatching() {
        let (index_scheduler, handle) = IndexScheduler::test(false);

        index_scheduler
            .register(KindWithContent::IndexCreation {
                index_uid: S("doggos"),
                primary_key: None,
            })
            .unwrap();
        index_scheduler
            .register(KindWithContent::DocumentClear {
                index_uid: S("doggos"),
            })
            .unwrap();
        index_scheduler
            .register(KindWithContent::DocumentClear {
                index_uid: S("doggos"),
            })
            .unwrap();
        index_scheduler
            .register(KindWithContent::DocumentClear {
                index_uid: S("doggos"),
            })
            .unwrap();

        handle.wait_till(Breakpoint::AfterProcessing);
        handle.wait_till(Breakpoint::AfterProcessing);
        handle.wait_till(Breakpoint::AfterProcessing);
        handle.wait_till(Breakpoint::AfterProcessing);

        let mut tasks = index_scheduler.get_tasks(Query::default()).unwrap();
        tasks.reverse();
        assert_eq!(tasks.len(), 4);
        assert_eq!(tasks[0].status, Status::Succeeded);
        assert_eq!(tasks[1].status, Status::Succeeded);
        assert_eq!(tasks[2].status, Status::Succeeded);
        assert_eq!(tasks[3].status, Status::Succeeded);
    }

    #[test]
    fn task_deletion_undeleteable() {
        let (index_scheduler, handle) = IndexScheduler::test(true);

        let to_enqueue = [
            index_creation_task("catto", "mouse"),
            replace_document_import_task("catto", None, 0, 12),
            replace_document_import_task("doggo", Some("bone"), 1, 5000),
        ];
        for task in to_enqueue {
            let _ = index_scheduler.register(task).unwrap();
        }

        // here we have registered all the tasks, but the index scheduler
        // has not progressed at all
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");

        index_scheduler
            .register(KindWithContent::TaskDeletion {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter(&[0, 1]),
            })
            .unwrap();

        // again, no progress made at all, but one more task is registered
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_enqueued");

        // now we create the first batch
        handle.wait_till(Breakpoint::BatchCreated);

        // the task deletion should now be "processing"
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processing");

        handle.wait_till(Breakpoint::AfterProcessing);

        // after the task deletion is processed, no task should actually have been deleted,
        // because the tasks with ids 0 and 1 were still "enqueued", and thus undeleteable
        // the "task deletion" task should be marked as "succeeded" and, in its details, the
        // number of deleted tasks should be 0
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_done");
    }

    #[test]
    fn task_deletion_deleteable() {
        let (index_scheduler, handle) = IndexScheduler::test(true);

        let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
        let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);

        let to_enqueue = [
            replace_document_import_task("catto", None, 0, documents_count0),
            replace_document_import_task("doggo", Some("bone"), 1, documents_count1),
        ];

        for task in to_enqueue {
            let _ = index_scheduler.register(task).unwrap();
        }
        file0.persist().unwrap();
        file1.persist().unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");

        handle.wait_till(Breakpoint::AfterProcessing);
        // first addition of documents should be successful
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_processed");

        // Now we delete the first task
        index_scheduler
            .register(KindWithContent::TaskDeletion {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter(&[0]),
            })
            .unwrap();

        handle.wait_till(Breakpoint::AfterProcessing);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processed");
    }

    #[test]
    fn task_deletion_delete_same_task_twice() {
        let (index_scheduler, handle) = IndexScheduler::test(true);

        let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
        let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);

        let to_enqueue = [
            replace_document_import_task("catto", None, 0, documents_count0),
            replace_document_import_task("doggo", Some("bone"), 1, documents_count1),
        ];

        for task in to_enqueue {
            let _ = index_scheduler.register(task).unwrap();
        }
        file0.persist().unwrap();
        file1.persist().unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");

        handle.wait_till(Breakpoint::AfterProcessing);
        // first addition of documents should be successful
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_processed");

        // Now we delete the first task multiple times in a row
        for _ in 0..2 {
            index_scheduler
                .register(KindWithContent::TaskDeletion {
                    query: "test_query".to_owned(),
                    tasks: RoaringBitmap::from_iter(&[0]),
                })
                .unwrap();
        }
        for _ in 0..2 {
            handle.wait_till(Breakpoint::AfterProcessing);
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processed");
    }

    #[test]
    fn document_addition() {
        let (index_scheduler, handle) = IndexScheduler::test(true);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count =
            meilisearch_types::document_formats::read_json(content.as_bytes(), file.as_file_mut())
                .unwrap() as u64;
        index_scheduler
            .register(KindWithContent::DocumentImport {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            })
            .unwrap();
        file.persist().unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler));

        handle.wait_till(Breakpoint::BatchCreated);

        snapshot!(snapshot_index_scheduler(&index_scheduler));

        handle.wait_till(Breakpoint::AfterProcessing);

        snapshot!(snapshot_index_scheduler(&index_scheduler));
    }

    #[test]
    fn do_not_batch_task_of_different_indexes() {
        let (index_scheduler, handle) = IndexScheduler::test(true);
        let index_names = ["doggos", "cattos", "girafos"];

        for name in index_names {
            index_scheduler
                .register(KindWithContent::IndexCreation {
                    index_uid: name.to_string(),
                    primary_key: None,
                })
                .unwrap();
        }

        for name in index_names {
            index_scheduler
                .register(KindWithContent::DocumentClear {
                    index_uid: name.to_string(),
                })
                .unwrap();
        }

        for _ in 0..(index_names.len() * 2) {
            handle.wait_till(Breakpoint::AfterProcessing);
        }

        let mut tasks = index_scheduler.get_tasks(Query::default()).unwrap();
        tasks.reverse();
        assert_eq!(tasks.len(), 6);
        assert_eq!(tasks[0].status, Status::Succeeded);
        assert_eq!(tasks[1].status, Status::Succeeded);
        assert_eq!(tasks[2].status, Status::Succeeded);
        assert_eq!(tasks[3].status, Status::Succeeded);
        assert_eq!(tasks[4].status, Status::Succeeded);
        assert_eq!(tasks[5].status, Status::Succeeded);
    }

    #[macro_export]
    macro_rules! debug_snapshot {
        ($value:expr, @$snapshot:literal) => {{
            let value = format!("{:?}", $value);
            meili_snap::snapshot!(value, @$snapshot);
        }};
    }

    #[test]
    fn simple_new() {
        crate::IndexScheduler::test(true);
    }
}

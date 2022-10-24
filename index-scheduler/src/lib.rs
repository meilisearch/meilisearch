/*!
This crate defines the index scheduler, which is responsible for:
1. Keeping references to meilisearch's indexes and mapping them to their
user-defined names.
2. Scheduling tasks given by the user and executing them, in batch if possible.

When an `IndexScheduler` is created, a new thread containing a reference to the
scheduler is created. This thread runs the scheduler's run loop, where the
scheduler waits to be woken up to process new tasks. It wakes up when:

1. it is launched for the first time
2. a new task is registered
3. a batch of tasks has been processed

It is only within this thread that the scheduler is allowed to process tasks.
On the other hand, the publicly accessible methods of the scheduler can be
called asynchronously from any thread. These methods can either query the
content of the scheduler or enqueue new tasks.
*/

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

use utils::keep_tasks_within_datetimes;

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, RwLock};

use file_store::FileStore;
use meilisearch_types::error::ResponseError;
use meilisearch_types::heed::types::{OwnedType, SerdeBincode, SerdeJson, Str};
use meilisearch_types::heed::{self, Database, Env};
use meilisearch_types::milli;
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::{CboRoaringBitmapCodec, Index, RoaringBitmapCodec, BEU32};
use roaring::RoaringBitmap;
use synchronoise::SignalEvent;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::index_mapper::IndexMapper;

pub(crate) type BEI128 =
    meilisearch_types::heed::zerocopy::I128<meilisearch_types::heed::byteorder::BE>;

/// Defines a subset of tasks to be retrieved from the [`IndexScheduler`].
///
/// An empty/default query (where each field is set to `None`) matches all tasks.
/// Each non-null field restricts the set of tasks further.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// The maximum number of tasks to be matched
    pub limit: Option<u32>,
    /// The minimum [task id](`meilisearch_types::tasks::Task::uid`) to be matched
    pub from: Option<u32>,
    /// The allowed [statuses](`meilisearch_types::tasks::Task::status`) of the matched tasls
    pub status: Option<Vec<Status>>,
    /// The allowed [kinds](meilisearch_types::tasks::Kind) of the matched tasks.
    ///
    /// The kind of a task is given by:
    /// ```
    /// # use meilisearch_types::tasks::{Task, Kind};
    /// # fn doc_func(task: Task) -> Kind {
    /// task.kind.as_kind()
    /// # }
    /// ```
    pub kind: Option<Vec<Kind>>,
    /// The allowed [index ids](meilisearch_types::tasks::Task::index_uid) of the matched tasks
    pub index_uid: Option<Vec<String>>,
    /// The [task ids](`meilisearch_types::tasks::Task::uid`) to be matched
    pub uid: Option<Vec<TaskId>>,

    /// Exclusive upper bound of the matched tasks' [`enqueued_at`](meilisearch_types::tasks::Task::enqueued_at) field.
    pub before_enqueued_at: Option<OffsetDateTime>,
    /// Exclusive lower bound of the matched tasks' [`enqueued_at`](meilisearch_types::tasks::Task::enqueued_at) field.
    pub after_enqueued_at: Option<OffsetDateTime>,
    /// Exclusive upper bound of the matched tasks' [`started_at`](meilisearch_types::tasks::Task::started_at) field.
    pub before_started_at: Option<OffsetDateTime>,
    /// Exclusive lower bound of the matched tasks' [`started_at`](meilisearch_types::tasks::Task::started_at) field.
    pub after_started_at: Option<OffsetDateTime>,
    /// Exclusive upper bound of the matched tasks' [`finished_at`](meilisearch_types::tasks::Task::finished_at) field.
    pub before_finished_at: Option<OffsetDateTime>,
    /// Exclusive lower bound of the matched tasks' [`finished_at`](meilisearch_types::tasks::Task::finished_at) field.
    pub after_finished_at: Option<OffsetDateTime>,
}

impl Query {
    /// Return `true` iff every field of the query is set to `None`, such that the query
    /// matches all tasks.
    pub fn is_empty(&self) -> bool {
        matches!(
            self,
            Query {
                limit: None,
                from: None,
                status: None,
                kind: None,
                index_uid: None,
                uid: None,
                before_enqueued_at: None,
                after_enqueued_at: None,
                before_started_at: None,
                after_started_at: None,
                before_finished_at: None,
                after_finished_at: None,
            }
        )
    }

    /// Add an [index id](meilisearch_types::tasks::Task::index_uid) to the list of permitted indexes.
    pub fn with_index(self, index_uid: String) -> Self {
        let mut index_vec = self.index_uid.unwrap_or_default();
        index_vec.push(index_uid);
        Self { index_uid: Some(index_vec), ..self }
    }
}

#[derive(Debug, Clone)]
struct ProcessingTasks {
    /// The date and time at which the indexation started.
    started_at: OffsetDateTime,
    /// The list of tasks ids that are currently running.
    processing: RoaringBitmap,
}

impl ProcessingTasks {
    /// Creates an empty `ProcessingAt` struct.
    fn new() -> ProcessingTasks {
        ProcessingTasks { started_at: OffsetDateTime::now_utc(), processing: RoaringBitmap::new() }
    }

    /// Stores the currently processing tasks, and the date time at which it started.
    fn start_processing_at(&mut self, started_at: OffsetDateTime, processing: RoaringBitmap) {
        self.started_at = started_at;
        self.processing = processing;
    }

    /// Set the processing tasks to an empty list.
    fn stop_processing_at(&mut self, stopped_at: OffsetDateTime) {
        self.started_at = stopped_at;
        self.processing = RoaringBitmap::new();
    }

    /// Returns `true` if there, at least, is one task that is currently processing we must stop.
    fn must_cancel_processing_tasks(&self, canceled_tasks: &RoaringBitmap) -> bool {
        !self.processing.is_disjoint(canceled_tasks)
    }
}

#[derive(Default, Clone, Debug)]
struct MustStopProcessing(Arc<AtomicBool>);

impl MustStopProcessing {
    fn get(&self) -> bool {
        self.0.load(Relaxed)
    }

    fn must_stop(&self) {
        self.0.store(true, Relaxed);
    }

    fn reset(&self) {
        self.0.store(false, Relaxed);
    }
}

/// Database const names for the `IndexScheduler`.
mod db_name {
    pub const ALL_TASKS: &str = "all-tasks";
    pub const STATUS: &str = "status";
    pub const KIND: &str = "kind";
    pub const INDEX_TASKS: &str = "index-tasks";
    pub const ENQUEUED_AT: &str = "enqueued-at";
    pub const STARTED_AT: &str = "started-at";
    pub const FINISHED_AT: &str = "finished-at";
}

/// Structure which holds meilisearch's indexes and schedules the tasks
/// to be performed on them.
pub struct IndexScheduler {
    /// The LMDB environment which the DBs are associated with.
    pub(crate) env: Env,

    /// A boolean that can be set to true to stop the currently processing tasks.
    pub(crate) must_stop_processing: MustStopProcessing,

    /// The list of tasks currently processing
    pub(crate) processing_tasks: Arc<RwLock<ProcessingTasks>>,

    /// The list of files referenced by the tasks
    pub(crate) file_store: FileStore,

    // The main database, it contains all the tasks accessible by their Id.
    pub(crate) all_tasks: Database<OwnedType<BEU32>, SerdeJson<Task>>,

    /// All the tasks ids grouped by their status.
    // TODO we should not be able to serialize a `Status::Processing` in this database.
    pub(crate) status: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
    /// All the tasks ids grouped by their kind.
    pub(crate) kind: Database<SerdeBincode<Kind>, RoaringBitmapCodec>,
    /// Store the tasks associated to an index.
    pub(crate) index_tasks: Database<Str, RoaringBitmapCodec>,

    /// Store the task ids of tasks which were enqueued at a specific date
    ///
    /// Note that since we store the date with nanosecond-level precision, it would be
    /// reasonable to assume that there is only one task per key. However, it is not a
    /// theoretical certainty, and we might want to make it possible to enqueue multiple
    /// tasks at a time in the future.
    pub(crate) enqueued_at: Database<OwnedType<BEI128>, CboRoaringBitmapCodec>,

    /// Store the task ids of finished tasks which started being processed at a specific date
    pub(crate) started_at: Database<OwnedType<BEI128>, CboRoaringBitmapCodec>,

    /// Store the task ids of tasks which finished at a specific date
    pub(crate) finished_at: Database<OwnedType<BEI128>, CboRoaringBitmapCodec>,

    /// In charge of creating, opening, storing and returning indexes.
    pub(crate) index_mapper: IndexMapper,

    /// Get a signal when a batch needs to be processed.
    pub(crate) wake_up: Arc<SignalEvent>,

    /// Whether auto-batching is enabled or not.
    pub(crate) autobatching_enabled: bool,

    /// The path used to create the dumps.
    pub(crate) dumps_path: PathBuf,

    // ================= test
    /// The next entry is dedicated to the tests.
    /// It provide a way to break in multiple part of the scheduler.
    #[cfg(test)]
    test_breakpoint_sdr: crossbeam::channel::Sender<(Breakpoint, bool)>,

    #[cfg(test)]
    /// A list of planned failures within the [`tick`](IndexScheduler::tick) method of the index scheduler.
    ///
    /// The first field is the iteration index and the second field identifies a location in the code.
    planned_failures: Vec<(usize, tests::FailureLocation)>,

    #[cfg(test)]
    /// A counter that is incremented before every call to [`tick`](IndexScheduler::tick)
    run_loop_iteration: Arc<RwLock<usize>>,
}
impl IndexScheduler {
    fn private_clone(&self) -> Self {
        Self {
            env: self.env.clone(),
            must_stop_processing: self.must_stop_processing.clone(),
            processing_tasks: self.processing_tasks.clone(),
            file_store: self.file_store.clone(),
            all_tasks: self.all_tasks.clone(),
            status: self.status.clone(),
            kind: self.kind.clone(),
            index_tasks: self.index_tasks.clone(),
            enqueued_at: self.enqueued_at.clone(),
            started_at: self.started_at.clone(),
            finished_at: self.finished_at.clone(),
            index_mapper: self.index_mapper.clone(),
            wake_up: self.wake_up.clone(),
            autobatching_enabled: self.autobatching_enabled.clone(),
            dumps_path: self.dumps_path.clone(),
            #[cfg(test)]
            test_breakpoint_sdr: self.test_breakpoint_sdr.clone(),
            #[cfg(test)]
            planned_failures: self.planned_failures.clone(),
            #[cfg(test)]
            run_loop_iteration: self.run_loop_iteration.clone(),
        }
    }
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
    /// Create an index scheduler and start its run loop.
    ///
    /// ## Arguments
    /// - `tasks_path`: the path to the folder containing the task databases
    /// - `update_file_path`: the path to the file store containing the files associated to the tasks
    /// - `indexes_path`: the path to the folder containing meilisearch's indexes
    /// - `dumps_path`: the path to the folder containing the dumps
    /// - `index_size`: the maximum size, in bytes, of each meilisearch index
    /// - `indexer_config`: configuration used during indexing for each meilisearch index
    /// - `autobatching_enabled`: `true` iff the index scheduler is allowed to automatically batch tasks
    /// together, to process multiple tasks at once.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tasks_path: PathBuf,
        update_file_path: PathBuf,
        indexes_path: PathBuf,
        dumps_path: PathBuf,
        task_db_size: usize,
        index_size: usize,
        indexer_config: IndexerConfig,
        autobatching_enabled: bool,
        #[cfg(test)] test_breakpoint_sdr: crossbeam::channel::Sender<(Breakpoint, bool)>,
        #[cfg(test)] planned_failures: Vec<(usize, tests::FailureLocation)>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&tasks_path)?;
        std::fs::create_dir_all(&update_file_path)?;
        std::fs::create_dir_all(&indexes_path)?;
        std::fs::create_dir_all(&dumps_path)?;

        let mut options = heed::EnvOpenOptions::new();
        options.max_dbs(9);
        options.map_size(task_db_size);

        let env = options.open(tasks_path)?;
        let file_store = FileStore::new(&update_file_path)?;

        // allow unreachable_code to get rids of the warning in the case of a test build.
        let this = Self {
            must_stop_processing: MustStopProcessing::default(),
            processing_tasks: Arc::new(RwLock::new(ProcessingTasks::new())),
            file_store,
            all_tasks: env.create_database(Some(db_name::ALL_TASKS))?,
            status: env.create_database(Some(db_name::STATUS))?,
            kind: env.create_database(Some(db_name::KIND))?,
            index_tasks: env.create_database(Some(db_name::INDEX_TASKS))?,
            enqueued_at: env.create_database(Some(db_name::ENQUEUED_AT))?,
            started_at: env.create_database(Some(db_name::STARTED_AT))?,
            finished_at: env.create_database(Some(db_name::FINISHED_AT))?,
            index_mapper: IndexMapper::new(&env, indexes_path, index_size, indexer_config)?,
            env,
            // we want to start the loop right away in case meilisearch was ctrl+Ced while processing things
            wake_up: Arc::new(SignalEvent::auto(true)),
            autobatching_enabled,
            dumps_path,

            #[cfg(test)]
            test_breakpoint_sdr,
            #[cfg(test)]
            planned_failures,
            #[cfg(test)]
            run_loop_iteration: Arc::new(RwLock::new(0)),
        };

        this.run();
        Ok(this)
    }

    /// Start the run loop for the given index scheduler.
    ///
    /// This function will execute in a different thread and must be called
    /// only once per index scheduler.
    fn run(&self) {
        let run = self.private_clone();

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

    /// Return the index corresponding to the name.
    ///
    /// * If the index wasn't opened before, the index will be opened.
    /// * If the index doesn't exist on disk, the `IndexNotFoundError` is thrown.
    pub fn index(&self, name: &str) -> Result<Index> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.index(&rtxn, name)
    }

    /// Return and open all the indexes.
    pub fn indexes(&self) -> Result<Vec<(String, Index)>> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.indexes(&rtxn)
    }

    /// Return the task ids matched by the given query.
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
                index_tasks |= self.index_tasks(&rtxn, index)?;
            }
            tasks &= index_tasks;
        }

        keep_tasks_within_datetimes(
            &rtxn,
            &mut tasks,
            self.enqueued_at,
            query.after_enqueued_at,
            query.before_enqueued_at,
        )?;

        keep_tasks_within_datetimes(
            &rtxn,
            &mut tasks,
            self.started_at,
            query.after_started_at,
            query.before_started_at,
        )?;

        keep_tasks_within_datetimes(
            &rtxn,
            &mut tasks,
            self.finished_at,
            query.after_finished_at,
            query.before_finished_at,
        )?;

        Ok(tasks)
    }

    /// Returns the tasks matched by the given query.
    pub fn get_tasks(&self, query: Query) -> Result<Vec<Task>> {
        let tasks = self.get_task_ids(&query)?;
        let rtxn = self.env.read_txn()?;

        let tasks = self.get_existing_tasks(
            &rtxn,
            tasks.into_iter().rev().take(query.limit.unwrap_or(u32::MAX) as usize),
        )?;

        let ProcessingTasks { started_at, processing, .. } =
            self.processing_tasks.read().map_err(|_| Error::CorruptedTaskQueue)?.clone();

        let ret = tasks.into_iter();
        if processing.is_empty() {
            Ok(ret.collect())
        } else {
            Ok(ret
                .map(|task| match processing.contains(task.uid) {
                    true => {
                        Task { status: Status::Processing, started_at: Some(started_at), ..task }
                    }
                    false => task,
                })
                .collect())
        }
    }

    /// Register a new task in the scheduler.
    ///
    /// If it fails and data was associated with the task, it tries to delete the associated data.
    pub fn register(&self, kind: KindWithContent) -> Result<Task> {
        let mut wtxn = self.env.write_txn()?;

        let task = Task {
            uid: self.next_task_id(&wtxn)?,
            enqueued_at: time::OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            error: None,
            canceled_by: None,
            details: kind.default_details(),
            status: Status::Enqueued,
            kind: kind.clone(),
        };
        self.all_tasks.append(&mut wtxn, &BEU32::new(task.uid), &task)?;

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

        utils::insert_task_datetime(&mut wtxn, self.enqueued_at, task.enqueued_at, task.uid)?;

        if let Err(e) = wtxn.commit() {
            self.delete_persisted_task_data(&task)?;
            return Err(e.into());
        }

        // If the registered task is a task cancelation
        // we inform the processing tasks to stop (if necessary).
        if let KindWithContent::TaskCancelation { tasks, .. } = kind {
            let tasks_to_cancel = RoaringBitmap::from_iter(tasks);
            if self.processing_tasks.read().unwrap().must_cancel_processing_tasks(&tasks_to_cancel)
            {
                self.must_stop_processing.must_stop();
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
            canceled_by: task.canceled_by,
            details: task.details,
            status: task.status,
            kind: match task.kind {
                KindDump::DocumentImport {
                    primary_key,
                    method,
                    documents_count,
                    allow_index_creation,
                } => KindWithContent::DocumentAdditionOrUpdate {
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
                KindDump::Settings { settings, is_deletion, allow_index_creation } => {
                    KindWithContent::SettingsUpdate {
                        index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                        new_settings: settings,
                        is_deletion,
                        allow_index_creation,
                    }
                }
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
                KindDump::IndexSwap { swaps } => KindWithContent::IndexSwap { swaps },
                KindDump::TaskCancelation { query, tasks } => {
                    KindWithContent::TaskCancelation { query, tasks }
                }
                KindDump::TasksDeletion { query, tasks } => {
                    KindWithContent::TaskDeletion { query, tasks }
                }
                KindDump::DumpCreation { dump_uid, keys, instance_uid } => {
                    KindWithContent::DumpCreation { dump_uid, keys, instance_uid }
                }
                KindDump::Snapshot => KindWithContent::Snapshot,
            },
        };

        self.all_tasks.put(&mut wtxn, &BEU32::new(task.uid), &task)?;

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

    /// Create a file and register it in the index scheduler.
    ///
    /// The returned file and uuid can be used to associate
    /// some data to a task. The file will be kept until
    /// the task has been fully processed.
    pub fn create_update_file(&self) -> Result<(Uuid, file_store::File)> {
        Ok(self.file_store.new_update()?)
    }

    #[cfg(test)]
    pub fn create_update_file_with_uuid(&self, uuid: u128) -> Result<(Uuid, file_store::File)> {
        Ok(self.file_store.new_update_with_uuid(uuid)?)
    }

    /// Delete a file from the index scheduler.
    ///
    /// Counterpart to the [`create_update_file`](IndexScheduler::create_update_file) method.
    pub fn delete_update_file(&self, uuid: Uuid) -> Result<()> {
        Ok(self.file_store.delete(uuid)?)
    }

    /// Perform one iteration of the run loop.
    ///
    /// 1. Find the next batch of tasks to be processed.
    /// 2. Update the information of these tasks following the start of their processing.
    /// 3. Update the in-memory list of processed tasks accordingly.
    /// 4. Process the batch:
    ///    - perform the actions of each batched task
    ///    - update the information of each batched task following the end
    ///      of their processing.
    /// 5. Reset the in-memory list of processed tasks.
    ///
    /// Returns the number of processed tasks.
    fn tick(&self) -> Result<usize> {
        #[cfg(test)]
        {
            *self.run_loop_iteration.write().unwrap() += 1;
            self.breakpoint(Breakpoint::Start);
        }

        let rtxn = self.env.read_txn()?;
        let batch = match self.create_next_batch(&rtxn)? {
            Some(batch) => batch,
            None => return Ok(0),
        };
        drop(rtxn);

        // 1. store the starting date with the bitmap of processing tasks.
        let mut ids = batch.ids();
        ids.sort_unstable();
        let processed_tasks = ids.len();
        let processing_tasks = RoaringBitmap::from_sorted_iter(ids.iter().copied()).unwrap();
        let started_at = OffsetDateTime::now_utc();

        // We reset the must_stop flag to be sure that we don't stop processing tasks
        self.must_stop_processing.reset();
        self.processing_tasks.write().unwrap().start_processing_at(started_at, processing_tasks);

        #[cfg(test)]
        self.breakpoint(Breakpoint::BatchCreated);

        // 2. Process the tasks
        let res = {
            let cloned_index_scheduler = self.private_clone();
            let handle = std::thread::spawn(move || cloned_index_scheduler.process_batch(batch));
            handle.join().unwrap_or_else(|_| Err(Error::MilliPanic))
        };

        #[cfg(test)]
        self.maybe_fail(tests::FailureLocation::AcquiringWtxn)?;

        let mut wtxn = self.env.write_txn()?;

        let finished_at = OffsetDateTime::now_utc();
        match res {
            Ok(tasks) => {
                #[allow(unused_variables)]
                for (i, mut task) in tasks.into_iter().enumerate() {
                    task.started_at = Some(started_at);
                    task.finished_at = Some(finished_at);

                    #[cfg(test)]
                    self.maybe_fail(
                        tests::FailureLocation::UpdatingTaskAfterProcessBatchSuccess {
                            task_uid: i as u32,
                        },
                    )?;

                    self.update_task(&mut wtxn, &task)?;
                    self.delete_persisted_task_data(&task)?;
                }
                log::info!("A batch of tasks was successfully completed.");
            }
            // If we have an abortion error we must stop the tick here and re-schedule tasks.
            Err(Error::Milli(milli::Error::InternalError(
                milli::InternalError::AbortedIndexation,
            ))) => {
                // TODO should we add a breakpoint here?
                wtxn.abort()?;
                return Ok(0);
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

                    #[cfg(test)]
                    self.maybe_fail(tests::FailureLocation::UpdatingTaskAfterProcessBatchFailure)?;

                    self.delete_persisted_task_data(&task)?;
                    self.update_task(&mut wtxn, &task)?;
                }
            }
        }
        self.processing_tasks.write().unwrap().stop_processing_at(finished_at);

        #[cfg(test)]
        self.maybe_fail(tests::FailureLocation::CommittingWtxn)?;

        wtxn.commit()?;

        #[cfg(test)]
        self.breakpoint(Breakpoint::AfterProcessing);

        Ok(processed_tasks)
    }

    pub(crate) fn delete_persisted_task_data(&self, task: &Task) -> Result<()> {
        match task.content_uuid() {
            Some(content_file) => self.delete_update_file(*content_file),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    fn breakpoint(&self, b: Breakpoint) {
        // We send two messages. The first one will sync with the call
        // to `handle.wait_until(b)`. The second one will block until the
        // the next call to `handle.wait_until(..)`.
        self.test_breakpoint_sdr.send((b, false)).unwrap();
        // This one will only be able to be sent if the test handle stays alive.
        // If it fails, then it means that we have exited the test.
        // By crashing with `unwrap`, we kill the run loop.
        self.test_breakpoint_sdr.send((b, true)).unwrap();
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

    use crate::snapshot::{snapshot_bitmap, snapshot_index_scheduler};

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum FailureLocation {
        InsideCreateBatch,
        InsideProcessBatch,
        PanicInsideProcessBatch,
        AcquiringWtxn,
        UpdatingTaskAfterProcessBatchSuccess { task_uid: u32 },
        UpdatingTaskAfterProcessBatchFailure,
        CommittingWtxn,
    }

    impl IndexScheduler {
        pub fn test(
            autobatching: bool,
            planned_failures: Vec<(usize, FailureLocation)>,
        ) -> (Self, IndexSchedulerHandle) {
            let tempdir = TempDir::new().unwrap();
            let (sender, receiver) = crossbeam::channel::bounded(0);

            let index_scheduler = Self::new(
                tempdir.path().join("db_path"),
                tempdir.path().join("file_store"),
                tempdir.path().join("indexes"),
                tempdir.path().join("dumps"),
                1024 * 1024,
                1024 * 1024,
                IndexerConfig::default(),
                autobatching, // enable autobatching
                sender,
                planned_failures,
            )
            .unwrap();

            let index_scheduler_handle =
                IndexSchedulerHandle { _tempdir: tempdir, test_breakpoint_rcv: receiver };

            (index_scheduler, index_scheduler_handle)
        }

        /// Return a [`CorruptedTaskQueue`](Error::CorruptedTaskQueue) error if a failure is planned
        /// for the given location and current run loop iteration.
        pub fn maybe_fail(&self, location: FailureLocation) -> Result<()> {
            if self.planned_failures.contains(&(*self.run_loop_iteration.read().unwrap(), location))
            {
                match location {
                    FailureLocation::PanicInsideProcessBatch => {
                        panic!("simulated panic")
                    }
                    _ => Err(Error::CorruptedTaskQueue),
                }
            } else {
                Ok(())
            }
        }
    }

    /// Return a `KindWithContent::IndexCreation` task
    fn index_creation_task(index: &'static str, primary_key: &'static str) -> KindWithContent {
        KindWithContent::IndexCreation { index_uid: S(index), primary_key: Some(S(primary_key)) }
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
        KindWithContent::DocumentAdditionOrUpdate {
            index_uid: S(index),
            primary_key: primary_key.map(ToOwned::to_owned),
            method: ReplaceDocuments,
            content_file: Uuid::from_u128(content_file_uuid),
            documents_count,
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

        let (_uuid, mut file) = index_scheduler.create_update_file_with_uuid(file_uuid).unwrap();
        let documents_count =
            meilisearch_types::document_formats::read_json(content.as_bytes(), file.as_file_mut())
                .unwrap() as u64;
        (file, documents_count)
    }

    pub struct IndexSchedulerHandle {
        _tempdir: TempDir,
        test_breakpoint_rcv: crossbeam::channel::Receiver<(Breakpoint, bool)>,
    }

    impl IndexSchedulerHandle {
        /// Wait until the provided breakpoint is reached.
        fn wait_till(&self, breakpoint: Breakpoint) {
            self.test_breakpoint_rcv.iter().find(|b| *b == (breakpoint, false));
        }
    }

    #[test]
    fn register() {
        // In this test, the handle doesn't make any progress, we only check that the tasks are registered
        let (index_scheduler, _handle) = IndexScheduler::test(true, vec![]);

        let kinds = [
            index_creation_task("catto", "mouse"),
            replace_document_import_task("catto", None, 0, 12),
            replace_document_import_task("catto", None, 1, 50),
            replace_document_import_task("doggo", Some("bone"), 2, 5000),
        ];
        let (_, file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        file.persist().unwrap();
        let (_, file) = index_scheduler.create_update_file_with_uuid(1).unwrap();
        file.persist().unwrap();
        let (_, file) = index_scheduler.create_update_file_with_uuid(2).unwrap();
        file.persist().unwrap();

        for (idx, kind) in kinds.into_iter().enumerate() {
            let k = kind.as_kind();
            let task = index_scheduler.register(kind).unwrap();
            index_scheduler.assert_internally_consistent();

            assert_eq!(task.uid, idx as u32);
            assert_eq!(task.status, Status::Enqueued);
            assert_eq!(task.kind.as_kind(), k);
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler));
    }

    #[test]
    fn insert_task_while_another_task_is_processing() {
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);

        index_scheduler.register(index_creation_task("index_a", "id")).unwrap();
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::BatchCreated);
        index_scheduler.assert_internally_consistent();

        // while the task is processing can we register another task?
        index_scheduler.register(index_creation_task("index_b", "id")).unwrap();
        index_scheduler.assert_internally_consistent();

        index_scheduler
            .register(KindWithContent::IndexDeletion { index_uid: S("index_a") })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        snapshot!(snapshot_index_scheduler(&index_scheduler));
    }

    /// We send a lot of tasks but notify the tasks scheduler only once as
    /// we send them very fast, we must make sure that they are all processed.
    #[test]
    fn process_tasks_inserted_without_new_signal() {
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);

        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("cattos"), primary_key: None })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        index_scheduler
            .register(KindWithContent::IndexDeletion { index_uid: S("doggos") })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::Start);
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        let mut tasks = index_scheduler.get_tasks(Query::default()).unwrap();
        tasks.reverse();
        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].status, Status::Succeeded);
        assert_eq!(tasks[1].status, Status::Succeeded);
        assert_eq!(tasks[2].status, Status::Succeeded);
    }

    #[test]
    fn process_tasks_without_autobatching() {
        let (index_scheduler, handle) = IndexScheduler::test(false, vec![]);

        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        index_scheduler
            .register(KindWithContent::DocumentClear { index_uid: S("doggos") })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        index_scheduler
            .register(KindWithContent::DocumentClear { index_uid: S("doggos") })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        index_scheduler
            .register(KindWithContent::DocumentClear { index_uid: S("doggos") })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

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
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);

        let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
        let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);
        file0.persist().unwrap();
        file1.persist().unwrap();

        let to_enqueue = [
            index_creation_task("catto", "mouse"),
            replace_document_import_task("catto", None, 0, documents_count0),
            replace_document_import_task("doggo", Some("bone"), 1, documents_count1),
        ];

        for task in to_enqueue {
            let _ = index_scheduler.register(task).unwrap();
            index_scheduler.assert_internally_consistent();
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
        index_scheduler.assert_internally_consistent();

        // again, no progress made at all, but one more task is registered
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_enqueued");

        // now we create the first batch
        handle.wait_till(Breakpoint::BatchCreated);
        index_scheduler.assert_internally_consistent();

        // the task deletion should now be "processing"
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processing");

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        // after the task deletion is processed, no task should actually have been deleted,
        // because the tasks with ids 0 and 1 were still "enqueued", and thus undeleteable
        // the "task deletion" task should be marked as "succeeded" and, in its details, the
        // number of deleted tasks should be 0
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_done");
    }

    #[test]
    fn task_deletion_deleteable() {
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);

        let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
        let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);
        file0.persist().unwrap();
        file1.persist().unwrap();

        let to_enqueue = [
            replace_document_import_task("catto", None, 0, documents_count0),
            replace_document_import_task("doggo", Some("bone"), 1, documents_count1),
        ];

        for task in to_enqueue {
            let _ = index_scheduler.register(task).unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        // first addition of documents should be successful
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_processed");

        // Now we delete the first task
        index_scheduler
            .register(KindWithContent::TaskDeletion {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter(&[0]),
            })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processed");
    }

    #[test]
    fn task_deletion_delete_same_task_twice() {
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);

        let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
        let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);
        file0.persist().unwrap();
        file1.persist().unwrap();

        let to_enqueue = [
            replace_document_import_task("catto", None, 0, documents_count0),
            replace_document_import_task("doggo", Some("bone"), 1, documents_count1),
        ];

        for task in to_enqueue {
            let _ = index_scheduler.register(task).unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

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
            index_scheduler.assert_internally_consistent();
        }
        for _ in 0..2 {
            handle.wait_till(Breakpoint::AfterProcessing);
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processed");
    }

    #[test]
    fn document_addition() {
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count =
            meilisearch_types::document_formats::read_json(content.as_bytes(), file.as_file_mut())
                .unwrap() as u64;
        file.persist().unwrap();
        index_scheduler
            .register(KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            })
            .unwrap();

        index_scheduler.assert_internally_consistent();

        snapshot!(snapshot_index_scheduler(&index_scheduler));

        handle.wait_till(Breakpoint::BatchCreated);
        index_scheduler.assert_internally_consistent();

        snapshot!(snapshot_index_scheduler(&index_scheduler));

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        snapshot!(snapshot_index_scheduler(&index_scheduler));
    }

    #[test]
    fn document_addition_and_index_deletion() {
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count =
            meilisearch_types::document_formats::read_json(content.as_bytes(), file.as_file_mut())
                .unwrap() as u64;
        file.persist().unwrap();
        index_scheduler
            .register(KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        index_scheduler
            .register(KindWithContent::IndexDeletion { index_uid: S("doggos") })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        snapshot!(snapshot_index_scheduler(&index_scheduler));

        handle.wait_till(Breakpoint::Start); // The index creation.
        handle.wait_till(Breakpoint::Start); // before anything happens.
        handle.wait_till(Breakpoint::Start); // after the execution of the two tasks in a single batch.

        snapshot!(snapshot_index_scheduler(&index_scheduler));
    }

    #[test]
    fn do_not_batch_task_of_different_indexes() {
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);
        let index_names = ["doggos", "cattos", "girafos"];

        for name in index_names {
            index_scheduler
                .register(KindWithContent::IndexCreation {
                    index_uid: name.to_string(),
                    primary_key: None,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        for name in index_names {
            index_scheduler
                .register(KindWithContent::DocumentClear { index_uid: name.to_string() })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        for _ in 0..(index_names.len() * 2) {
            handle.wait_till(Breakpoint::AfterProcessing);
            index_scheduler.assert_internally_consistent();
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

    #[test]
    fn swap_indexes() {
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);

        let to_enqueue = [
            index_creation_task("a", "id"),
            index_creation_task("b", "id"),
            index_creation_task("c", "id"),
            index_creation_task("d", "id"),
        ];

        for task in to_enqueue {
            let _ = index_scheduler.register(task).unwrap();
            index_scheduler.assert_internally_consistent();
        }

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_processed");

        index_scheduler
            .register(KindWithContent::IndexSwap {
                swaps: vec![("a".to_owned(), "b".to_owned()), ("c".to_owned(), "d".to_owned())],
            })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_swap_processed");

        index_scheduler
            .register(KindWithContent::IndexSwap { swaps: vec![("a".to_owned(), "c".to_owned())] })
            .unwrap();
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_swap_processed");
    }

    #[test]
    fn document_addition_and_index_deletion_on_unexisting_index() {
        let (index_scheduler, handle) = IndexScheduler::test(true, vec![]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count =
            meilisearch_types::document_formats::read_json(content.as_bytes(), file.as_file_mut())
                .unwrap() as u64;
        file.persist().unwrap();
        index_scheduler
            .register(KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            })
            .unwrap();
        index_scheduler
            .register(KindWithContent::IndexDeletion { index_uid: S("doggos") })
            .unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler));

        handle.wait_till(Breakpoint::Start); // before anything happens.
        handle.wait_till(Breakpoint::Start); // after the execution of the two tasks in a single batch.

        snapshot!(snapshot_index_scheduler(&index_scheduler));
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
        crate::IndexScheduler::test(true, vec![]);
    }

    #[test]
    fn query_processing_tasks() {
        let (index_scheduler, handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::InsideCreateBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _task = index_scheduler.register(kind).unwrap();

        handle.wait_till(Breakpoint::BatchCreated);
        let query = Query { status: Some(vec![Status::Processing]), ..Default::default() };
        let processing_tasks = index_scheduler.get_task_ids(&query).unwrap();
        snapshot!(snapshot_bitmap(&processing_tasks), @"[0,]");
    }

    #[test]
    fn fail_in_create_batch_for_index_creation() {
        let (index_scheduler, handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::InsideCreateBatch)]);

        let kinds = [index_creation_task("catto", "mouse")];

        for kind in kinds {
            let _task = index_scheduler.register(kind).unwrap();
            index_scheduler.assert_internally_consistent();
        }
        handle.wait_till(Breakpoint::BatchCreated);

        // We skipped an iteration of `tick` to reach BatchCreated
        assert_eq!(*index_scheduler.run_loop_iteration.read().unwrap(), 2);
        // Otherwise nothing weird happened
        index_scheduler.assert_internally_consistent();
        snapshot!(snapshot_index_scheduler(&index_scheduler));
    }

    #[test]
    fn fail_in_process_batch_for_index_creation() {
        let (index_scheduler, handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");

        let _task = index_scheduler.register(kind).unwrap();
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);

        // Still in the first iteration
        assert_eq!(*index_scheduler.run_loop_iteration.read().unwrap(), 1);
        // No matter what happens in process_batch, the index_scheduler should be internally consistent
        index_scheduler.assert_internally_consistent();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "index_creation_failed");
    }

    #[test]
    fn fail_in_process_batch_for_document_addition() {
        let (index_scheduler, handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::InsideProcessBatch)]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count =
            meilisearch_types::document_formats::read_json(content.as_bytes(), file.as_file_mut())
                .unwrap() as u64;
        file.persist().unwrap();
        index_scheduler
            .register(KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            })
            .unwrap();
        index_scheduler.assert_internally_consistent();
        handle.wait_till(Breakpoint::BatchCreated);

        snapshot!(
            snapshot_index_scheduler(&index_scheduler),
            name: "document_addition_batch_created"
        );

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "document_addition_failed");
    }

    #[test]
    fn fail_in_update_task_after_process_batch_success_for_document_addition() {
        // TODO: this is not the correct behaviour, because we process the
        // same tasks twice.
        //
        // I wonder whether the run loop should maybe be paused if we encounter a failure
        // at that point. Alternatively, maybe we should preemptively mark the tasks
        // as failed at the beginning of `IndexScheduler::tick`.

        let (index_scheduler, handle) = IndexScheduler::test(
            true,
            vec![(1, FailureLocation::UpdatingTaskAfterProcessBatchSuccess { task_uid: 0 })],
        );

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count =
            meilisearch_types::document_formats::read_json(content.as_bytes(), file.as_file_mut())
                .unwrap() as u64;
        file.persist().unwrap();
        index_scheduler
            .register(KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            })
            .unwrap();
        index_scheduler.assert_internally_consistent();
        handle.wait_till(Breakpoint::Start);

        index_scheduler.assert_internally_consistent();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "document_addition_succeeded_but_index_scheduler_not_updated");

        handle.wait_till(Breakpoint::AfterProcessing);
        index_scheduler.assert_internally_consistent();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_iteratino");
    }

    #[test]
    fn panic_in_process_batch_for_index_creation() {
        let (index_scheduler, handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::PanicInsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");

        let _task = index_scheduler.register(kind).unwrap();
        index_scheduler.assert_internally_consistent();

        handle.wait_till(Breakpoint::AfterProcessing);

        // Still in the first iteration
        assert_eq!(*index_scheduler.run_loop_iteration.read().unwrap(), 1);
        // No matter what happens in process_batch, the index_scheduler should be internally consistent
        index_scheduler.assert_internally_consistent();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "index_creation_failed");
    }
}

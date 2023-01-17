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
mod insta_snapshot;
mod lru;
mod utils;
mod uuid_codec;

pub type Result<T> = std::result::Result<T, Error>;
pub type TaskId = u32;

use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dump::{KindDump, TaskDump, UpdateFile};
pub use error::Error;
use file_store::FileStore;
use meilisearch_types::error::ResponseError;
use meilisearch_types::heed::types::{OwnedType, SerdeBincode, SerdeJson, Str};
use meilisearch_types::heed::{self, Database, Env, RoTxn};
use meilisearch_types::milli;
use meilisearch_types::milli::documents::DocumentsBatchBuilder;
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::{CboRoaringBitmapCodec, Index, RoaringBitmapCodec, BEU32};
use meilisearch_types::tasks::{Kind, KindWithContent, Status, Task};
use roaring::RoaringBitmap;
use synchronoise::SignalEvent;
use time::OffsetDateTime;
use utils::{filter_out_references_to_newer_tasks, keep_tasks_within_datetimes, map_bound};
use uuid::Uuid;

use crate::index_mapper::IndexMapper;
use crate::utils::{check_index_swap_validity, clamp_to_page_size};

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
    pub statuses: Option<Vec<Status>>,
    /// The allowed [kinds](meilisearch_types::tasks::Kind) of the matched tasks.
    ///
    /// The kind of a task is given by:
    /// ```
    /// # use meilisearch_types::tasks::{Task, Kind};
    /// # fn doc_func(task: Task) -> Kind {
    /// task.kind.as_kind()
    /// # }
    /// ```
    pub types: Option<Vec<Kind>>,
    /// The allowed [index ids](meilisearch_types::tasks::Task::index_uid) of the matched tasks
    pub index_uids: Option<Vec<String>>,
    /// The [task ids](`meilisearch_types::tasks::Task::uid`) to be matched
    pub uids: Option<Vec<TaskId>>,
    /// The [task ids](`meilisearch_types::tasks::Task::uid`) of the [`TaskCancelation`](meilisearch_types::tasks::Task::Kind::TaskCancelation) tasks
    /// that canceled the matched tasks.
    pub canceled_by: Option<Vec<TaskId>>,
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
    /// Return `true` if every field of the query is set to `None`, such that the query
    /// matches all tasks.
    pub fn is_empty(&self) -> bool {
        matches!(
            self,
            Query {
                limit: None,
                from: None,
                statuses: None,
                types: None,
                index_uids: None,
                uids: None,
                canceled_by: None,
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
        let mut index_vec = self.index_uids.unwrap_or_default();
        index_vec.push(index_uid);
        Self { index_uids: Some(index_vec), ..self }
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

    /// Set the processing tasks to an empty list
    fn stop_processing(&mut self) {
        self.processing = RoaringBitmap::new();
    }

    /// Returns `true` if there, at least, is one task that is currently processing that we must stop.
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
    pub const CANCELED_BY: &str = "canceled_by";
    pub const ENQUEUED_AT: &str = "enqueued-at";
    pub const STARTED_AT: &str = "started-at";
    pub const FINISHED_AT: &str = "finished-at";
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Breakpoint {
    // this state is only encountered while creating the scheduler in the test suite.
    Init,

    Start,
    BatchCreated,
    BeforeProcessing,
    AfterProcessing,
    AbortedIndexation,
    ProcessBatchSucceeded,
    ProcessBatchFailed,
    InsideProcessBatch,
}

#[derive(Debug)]
pub struct IndexSchedulerOptions {
    /// The path to the version file of Meilisearch.
    pub version_file_path: PathBuf,
    /// The path to the folder containing the auth LMDB env.
    pub auth_path: PathBuf,
    /// The path to the folder containing the task databases.
    pub tasks_path: PathBuf,
    /// The path to the file store containing the files associated to the tasks.
    pub update_file_path: PathBuf,
    /// The path to the folder containing meilisearch's indexes.
    pub indexes_path: PathBuf,
    /// The path to the folder containing the snapshots.
    pub snapshots_path: PathBuf,
    /// The path to the folder containing the dumps.
    pub dumps_path: PathBuf,
    /// The maximum size, in bytes, of the task index.
    pub task_db_size: usize,
    /// The size, in bytes, with which a meilisearch index is opened the first time of each meilisearch index.
    pub index_base_map_size: usize,
    /// The size, in bytes, by which the map size of an index is increased when it resized due to being full.
    pub index_growth_amount: usize,
    /// The number of indexes that can be concurrently opened in memory.
    pub index_count: usize,
    /// Configuration used during indexing for each meilisearch index.
    pub indexer_config: IndexerConfig,
    /// Set to `true` iff the index scheduler is allowed to automatically
    /// batch tasks together, to process multiple tasks at once.
    pub autobatching_enabled: bool,
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

    /// Store the tasks that were canceled by a task uid
    pub(crate) canceled_by: Database<OwnedType<BEU32>, RoaringBitmapCodec>,

    /// Store the task ids of tasks which were enqueued at a specific date
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

    /// The path used to create the snapshots.
    pub(crate) snapshots_path: PathBuf,

    /// The path to the folder containing the auth LMDB env.
    pub(crate) auth_path: PathBuf,

    /// The path to the version file of Meilisearch.
    pub(crate) version_file_path: PathBuf,

    // ================= test
    // The next entry is dedicated to the tests.
    /// Provide a way to set a breakpoint in multiple part of the scheduler.
    ///
    /// See [self.breakpoint()](`IndexScheduler::breakpoint`) for an explanation.
    #[cfg(test)]
    test_breakpoint_sdr: crossbeam::channel::Sender<(Breakpoint, bool)>,

    /// A list of planned failures within the [`tick`](IndexScheduler::tick) method of the index scheduler.
    ///
    /// The first field is the iteration index and the second field identifies a location in the code.
    #[cfg(test)]
    planned_failures: Vec<(usize, tests::FailureLocation)>,

    /// A counter that is incremented before every call to [`tick`](IndexScheduler::tick)
    #[cfg(test)]
    run_loop_iteration: Arc<RwLock<usize>>,
}

impl IndexScheduler {
    fn private_clone(&self) -> IndexScheduler {
        IndexScheduler {
            env: self.env.clone(),
            must_stop_processing: self.must_stop_processing.clone(),
            processing_tasks: self.processing_tasks.clone(),
            file_store: self.file_store.clone(),
            all_tasks: self.all_tasks,
            status: self.status,
            kind: self.kind,
            index_tasks: self.index_tasks,
            canceled_by: self.canceled_by,
            enqueued_at: self.enqueued_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            index_mapper: self.index_mapper.clone(),
            wake_up: self.wake_up.clone(),
            autobatching_enabled: self.autobatching_enabled,
            snapshots_path: self.snapshots_path.clone(),
            dumps_path: self.dumps_path.clone(),
            auth_path: self.auth_path.clone(),
            version_file_path: self.version_file_path.clone(),
            #[cfg(test)]
            test_breakpoint_sdr: self.test_breakpoint_sdr.clone(),
            #[cfg(test)]
            planned_failures: self.planned_failures.clone(),
            #[cfg(test)]
            run_loop_iteration: self.run_loop_iteration.clone(),
        }
    }
}

impl IndexScheduler {
    /// Create an index scheduler and start its run loop.
    pub fn new(
        options: IndexSchedulerOptions,
        #[cfg(test)] test_breakpoint_sdr: crossbeam::channel::Sender<(Breakpoint, bool)>,
        #[cfg(test)] planned_failures: Vec<(usize, tests::FailureLocation)>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&options.tasks_path)?;
        std::fs::create_dir_all(&options.update_file_path)?;
        std::fs::create_dir_all(&options.indexes_path)?;
        std::fs::create_dir_all(&options.dumps_path)?;

        let env = heed::EnvOpenOptions::new()
            .max_dbs(10)
            .map_size(clamp_to_page_size(options.task_db_size))
            .open(options.tasks_path)?;
        let file_store = FileStore::new(&options.update_file_path)?;

        // allow unreachable_code to get rids of the warning in the case of a test build.
        let this = Self {
            must_stop_processing: MustStopProcessing::default(),
            processing_tasks: Arc::new(RwLock::new(ProcessingTasks::new())),
            file_store,
            all_tasks: env.create_database(Some(db_name::ALL_TASKS))?,
            status: env.create_database(Some(db_name::STATUS))?,
            kind: env.create_database(Some(db_name::KIND))?,
            index_tasks: env.create_database(Some(db_name::INDEX_TASKS))?,
            canceled_by: env.create_database(Some(db_name::CANCELED_BY))?,
            enqueued_at: env.create_database(Some(db_name::ENQUEUED_AT))?,
            started_at: env.create_database(Some(db_name::STARTED_AT))?,
            finished_at: env.create_database(Some(db_name::FINISHED_AT))?,
            index_mapper: IndexMapper::new(
                &env,
                options.indexes_path,
                options.index_base_map_size,
                options.index_growth_amount,
                options.index_count,
                options.indexer_config,
            )?,
            env,
            // we want to start the loop right away in case meilisearch was ctrl+Ced while processing things
            wake_up: Arc::new(SignalEvent::auto(true)),
            autobatching_enabled: options.autobatching_enabled,
            dumps_path: options.dumps_path,
            snapshots_path: options.snapshots_path,
            auth_path: options.auth_path,
            version_file_path: options.version_file_path,

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

    pub fn read_txn(&self) -> Result<RoTxn> {
        self.env.read_txn().map_err(|e| e.into())
    }

    /// Start the run loop for the given index scheduler.
    ///
    /// This function will execute in a different thread and must be called
    /// only once per index scheduler.
    fn run(&self) {
        let run = self.private_clone();
        std::thread::Builder::new()
            .name(String::from("scheduler"))
            .spawn(move || {
                #[cfg(test)]
                run.breakpoint(Breakpoint::Init);

                run.wake_up.wait();

                loop {
                    match run.tick() {
                        Ok(TickOutcome::TickAgain(_)) => (),
                        Ok(TickOutcome::WaitForSignal) => run.wake_up.wait(),
                        Err(e) => {
                            log::error!("{}", e);
                            // Wait one second when an irrecoverable error occurs.
                            if matches!(
                                e,
                                Error::CorruptedTaskQueue
                                    | Error::TaskDatabaseUpdate(_)
                                    | Error::HeedTransaction(_)
                                    | Error::CreateBatch(_)
                            ) {
                                std::thread::sleep(Duration::from_secs(1));
                            }
                        }
                    }
                }
            })
            .unwrap();
    }

    pub fn indexer_config(&self) -> &IndexerConfig {
        &self.index_mapper.indexer_config
    }

    pub fn size(&self) -> Result<u64> {
        Ok(self.env.real_disk_size()?)
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

    /// Return the task ids matched by the given query from the index scheduler's point of view.
    pub(crate) fn get_task_ids(&self, rtxn: &RoTxn, query: &Query) -> Result<RoaringBitmap> {
        let ProcessingTasks {
            started_at: started_at_processing, processing: processing_tasks, ..
        } = self.processing_tasks.read().unwrap().clone();

        let mut tasks = self.all_task_ids(rtxn)?;

        if let Some(from) = &query.from {
            tasks.remove_range(from.saturating_add(1)..);
        }

        if let Some(status) = &query.statuses {
            let mut status_tasks = RoaringBitmap::new();
            for status in status {
                match status {
                    // special case for Processing tasks
                    Status::Processing => {
                        status_tasks |= &processing_tasks;
                    }
                    status => status_tasks |= &self.get_status(rtxn, *status)?,
                };
            }
            if !status.contains(&Status::Processing) {
                tasks -= &processing_tasks;
            }
            tasks &= status_tasks;
        }

        if let Some(uids) = &query.uids {
            let uids = RoaringBitmap::from_iter(uids);
            tasks &= &uids;
        }

        if let Some(canceled_by) = &query.canceled_by {
            let mut all_canceled_tasks = RoaringBitmap::new();
            for cancel_task_uid in canceled_by {
                if let Some(canceled_by_uid) =
                    self.canceled_by.get(rtxn, &BEU32::new(*cancel_task_uid))?
                {
                    all_canceled_tasks |= canceled_by_uid;
                }
            }

            // if the canceled_by has been specified but no task
            // matches then we prefer matching zero than all tasks.
            if all_canceled_tasks.is_empty() {
                return Ok(RoaringBitmap::new());
            } else {
                tasks &= all_canceled_tasks;
            }
        }

        if let Some(kind) = &query.types {
            let mut kind_tasks = RoaringBitmap::new();
            for kind in kind {
                kind_tasks |= self.get_kind(rtxn, *kind)?;
            }
            tasks &= &kind_tasks;
        }

        if let Some(index) = &query.index_uids {
            let mut index_tasks = RoaringBitmap::new();
            for index in index {
                index_tasks |= self.index_tasks(rtxn, index)?;
            }
            tasks &= &index_tasks;
        }

        // For the started_at filter, we need to treat the part of the tasks that are processing from the part of the
        // tasks that are not processing. The non-processing ones are filtered normally while the processing ones
        // are entirely removed unless the in-memory startedAt variable falls within the date filter.
        // Once we have filtered the two subsets, we put them back together and assign it back to `tasks`.
        tasks = {
            let (mut filtered_non_processing_tasks, mut filtered_processing_tasks) =
                (&tasks - &processing_tasks, &tasks & &processing_tasks);

            // special case for Processing tasks
            // A closure that clears the filtered_processing_tasks if their started_at date falls outside the given bounds
            let mut clear_filtered_processing_tasks =
                |start: Bound<OffsetDateTime>, end: Bound<OffsetDateTime>| {
                    let start = map_bound(start, |b| b.unix_timestamp_nanos());
                    let end = map_bound(end, |b| b.unix_timestamp_nanos());
                    let is_within_dates = RangeBounds::contains(
                        &(start, end),
                        &started_at_processing.unix_timestamp_nanos(),
                    );
                    if !is_within_dates {
                        filtered_processing_tasks.clear();
                    }
                };
            match (query.after_started_at, query.before_started_at) {
                (None, None) => (),
                (None, Some(before)) => {
                    clear_filtered_processing_tasks(Bound::Unbounded, Bound::Excluded(before))
                }
                (Some(after), None) => {
                    clear_filtered_processing_tasks(Bound::Excluded(after), Bound::Unbounded)
                }
                (Some(after), Some(before)) => {
                    clear_filtered_processing_tasks(Bound::Excluded(after), Bound::Excluded(before))
                }
            };

            keep_tasks_within_datetimes(
                rtxn,
                &mut filtered_non_processing_tasks,
                self.started_at,
                query.after_started_at,
                query.before_started_at,
            )?;
            filtered_non_processing_tasks | filtered_processing_tasks
        };

        keep_tasks_within_datetimes(
            rtxn,
            &mut tasks,
            self.enqueued_at,
            query.after_enqueued_at,
            query.before_enqueued_at,
        )?;

        keep_tasks_within_datetimes(
            rtxn,
            &mut tasks,
            self.finished_at,
            query.after_finished_at,
            query.before_finished_at,
        )?;

        if let Some(limit) = query.limit {
            tasks = tasks.into_iter().rev().take(limit as usize).collect();
        }

        Ok(tasks)
    }

    /// Return true iff there is at least one task associated with this index
    /// that is processing.
    pub fn is_index_processing(&self, index: &str) -> Result<bool> {
        let rtxn = self.env.read_txn()?;
        let processing_tasks = self.processing_tasks.read().unwrap().processing.clone();
        let index_tasks = self.index_tasks(&rtxn, index)?;
        let nbr_index_processing_tasks = processing_tasks.intersection_len(&index_tasks);
        Ok(nbr_index_processing_tasks > 0)
    }

    /// Return the task ids matching the query from the user's point of view.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    /// with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_task_ids_from_authorized_indexes(
        &self,
        rtxn: &RoTxn,
        query: &Query,
        authorized_indexes: &Option<Vec<String>>,
    ) -> Result<RoaringBitmap> {
        let mut tasks = self.get_task_ids(rtxn, query)?;

        // If the query contains a list of index uid or there is a finite list of authorized indexes,
        // then we must exclude all the kinds that aren't associated to one and only one index.
        if query.index_uids.is_some() || authorized_indexes.is_some() {
            for kind in enum_iterator::all::<Kind>().filter(|kind| !kind.related_to_one_index()) {
                tasks -= self.get_kind(rtxn, kind)?;
            }
        }

        // Any task that is internally associated with a non-authorized index
        // must be discarded.
        if let Some(authorized_indexes) = authorized_indexes {
            let all_indexes_iter = self.index_tasks.iter(rtxn)?;
            for result in all_indexes_iter {
                let (index, index_tasks) = result?;
                if !authorized_indexes.contains(&index.to_owned()) {
                    tasks -= index_tasks;
                }
            }
        }

        Ok(tasks)
    }

    /// Return the tasks matching the query from the user's point of view.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    /// with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_tasks_from_authorized_indexes(
        &self,
        query: Query,
        authorized_indexes: Option<Vec<String>>,
    ) -> Result<Vec<Task>> {
        let rtxn = self.env.read_txn()?;

        let tasks =
            self.get_task_ids_from_authorized_indexes(&rtxn, &query, &authorized_indexes)?;

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

        let mut task = Task {
            uid: self.next_task_id(&wtxn)?,
            enqueued_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            error: None,
            canceled_by: None,
            details: kind.default_details(),
            status: Status::Enqueued,
            kind: kind.clone(),
        };
        // For deletion and cancelation tasks, we want to make extra sure that they
        // don't attempt to delete/cancel tasks that are newer than themselves.
        filter_out_references_to_newer_tasks(&mut task);
        // If the register task is an index swap task, verify that it is well-formed
        // (that it does not contain duplicate indexes).
        check_index_swap_validity(&task)?;

        // Get rid of the mutability.
        let task = task;

        self.all_tasks.append(&mut wtxn, &BEU32::new(task.uid), &task)?;

        for index in task.indexes() {
            self.update_index(&mut wtxn, index, |bitmap| {
                bitmap.insert(task.uid);
            })?;
        }

        self.update_status(&mut wtxn, Status::Enqueued, |bitmap| {
            bitmap.insert(task.uid);
        })?;

        self.update_kind(&mut wtxn, task.kind.as_kind(), |bitmap| {
            bitmap.insert(task.uid);
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

    /// Register a new task coming from a dump in the scheduler.
    /// By taking a mutable ref we're pretty sure no one will ever import a dump while actix is running.
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
                KindDump::DumpCreation { keys, instance_uid } => {
                    KindWithContent::DumpCreation { keys, instance_uid }
                }
                KindDump::SnapshotCreation => KindWithContent::SnapshotCreation,
            },
        };

        self.all_tasks.put(&mut wtxn, &BEU32::new(task.uid), &task)?;

        for index in task.indexes() {
            self.update_index(&mut wtxn, index, |bitmap| {
                bitmap.insert(task.uid);
            })?;
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
    pub fn create_raw_index(
        &self,
        name: &str,
        date: Option<(OffsetDateTime, OffsetDateTime)>,
    ) -> Result<Index> {
        let wtxn = self.env.write_txn()?;
        let index = self.index_mapper.create_index(wtxn, name, date)?;
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

    /// The size on disk taken by all the updates files contained in the `IndexScheduler`, in bytes.
    pub fn compute_update_file_size(&self) -> Result<u64> {
        Ok(self.file_store.compute_total_size()?)
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
    fn tick(&self) -> Result<TickOutcome> {
        #[cfg(test)]
        {
            *self.run_loop_iteration.write().unwrap() += 1;
            self.breakpoint(Breakpoint::Start);
        }

        let rtxn = self.env.read_txn().map_err(Error::HeedTransaction)?;
        let batch =
            match self.create_next_batch(&rtxn).map_err(|e| Error::CreateBatch(Box::new(e)))? {
                Some(batch) => batch,
                None => return Ok(TickOutcome::WaitForSignal),
            };
        let index_uid = batch.index_uid().map(ToOwned::to_owned);
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
            let handle = std::thread::Builder::new()
                .name(String::from("batch-operation"))
                .spawn(move || cloned_index_scheduler.process_batch(batch))
                .unwrap();
            handle.join().unwrap_or(Err(Error::ProcessBatchPanicked))
        };

        #[cfg(test)]
        self.maybe_fail(tests::FailureLocation::AcquiringWtxn)?;

        let mut wtxn = self.env.write_txn().map_err(Error::HeedTransaction)?;

        let finished_at = OffsetDateTime::now_utc();
        match res {
            Ok(tasks) => {
                #[cfg(test)]
                self.breakpoint(Breakpoint::ProcessBatchSucceeded);

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

                    self.update_task(&mut wtxn, &task)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?;
                    if let Err(e) = self.delete_persisted_task_data(&task) {
                        log::error!("Failure to delete the content files associated with task {}. Error: {e}", task.uid);
                    }
                }
                log::info!("A batch of tasks was successfully completed.");
            }
            // If we have an abortion error we must stop the tick here and re-schedule tasks.
            Err(Error::Milli(milli::Error::InternalError(
                milli::InternalError::AbortedIndexation,
            ))) => {
                #[cfg(test)]
                self.breakpoint(Breakpoint::AbortedIndexation);
                wtxn.abort().map_err(Error::HeedTransaction)?;

                // We make sure that we don't call `stop_processing` on the `processing_tasks`,
                // this is because we want to let the next tick call `create_next_batch` and keep
                // the `started_at` date times and `processings` of the current processing tasks.
                // This date time is used by the task cancelation to store the right `started_at`
                // date in the task on disk.
                return Ok(TickOutcome::TickAgain(0));
            }
            // If an index said it was full, we need to:
            // 1. identify which index is full
            // 2. close the associated environment
            // 3. resize it
            // 4. re-schedule tasks
            Err(Error::Milli(milli::Error::UserError(
                milli::UserError::MaxDatabaseSizeReached,
            ))) if index_uid.is_some() => {
                // fixme: add index_uid to match to avoid the unwrap
                let index_uid = index_uid.unwrap();
                // fixme: handle error more gracefully? not sure when this could happen
                self.index_mapper.resize_index(&wtxn, &index_uid)?;
                wtxn.abort().map_err(Error::HeedTransaction)?;

                return Ok(TickOutcome::TickAgain(0));
            }
            // In case of a failure we must get back and patch all the tasks with the error.
            Err(err) => {
                #[cfg(test)]
                self.breakpoint(Breakpoint::ProcessBatchFailed);
                let error: ResponseError = err.into();
                for id in ids {
                    let mut task = self
                        .get_task(&wtxn, id)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?
                        .ok_or(Error::CorruptedTaskQueue)?;
                    task.started_at = Some(started_at);
                    task.finished_at = Some(finished_at);
                    task.status = Status::Failed;
                    task.error = Some(error.clone());
                    task.details = task.details.map(|d| d.to_failed());

                    #[cfg(test)]
                    self.maybe_fail(tests::FailureLocation::UpdatingTaskAfterProcessBatchFailure)?;

                    if let Err(e) = self.delete_persisted_task_data(&task) {
                        log::error!("Failure to delete the content files associated with task {}. Error: {e}", task.uid);
                    }
                    self.update_task(&mut wtxn, &task)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?;
                }
            }
        }

        self.processing_tasks.write().unwrap().stop_processing();

        #[cfg(test)]
        self.maybe_fail(tests::FailureLocation::CommittingWtxn)?;

        wtxn.commit().map_err(Error::HeedTransaction)?;

        #[cfg(test)]
        self.breakpoint(Breakpoint::AfterProcessing);

        Ok(TickOutcome::TickAgain(processed_tasks))
    }

    pub(crate) fn delete_persisted_task_data(&self, task: &Task) -> Result<()> {
        match task.content_uuid() {
            Some(content_file) => self.delete_update_file(content_file),
            None => Ok(()),
        }
    }

    /// Blocks the thread until the test handle asks to progress to/through this breakpoint.
    ///
    /// Two messages are sent through the channel for each breakpoint.
    /// The first message is `(b, false)` and the second message is `(b, true)`.
    ///
    /// Since the channel has a capacity of zero, the `send` and `recv` calls wait for each other.
    /// So when the index scheduler calls `test_breakpoint_sdr.send(b, false)`, it blocks
    /// the thread until the test catches up by calling `test_breakpoint_rcv.recv()` enough.
    /// From the test side, we call `recv()` repeatedly until we find the message `(breakpoint, false)`.
    /// As soon as we find it, the index scheduler is unblocked but then wait again on the call to
    /// `test_breakpoint_sdr.send(b, true)`. This message will only be able to send once the
    /// test asks to progress to the next `(b2, false)`.
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

/// The outcome of calling the [`IndexScheduler::tick`] function.
pub enum TickOutcome {
    /// The scheduler should immediately attempt another `tick`.
    ///
    /// The `usize` field contains the number of processed tasks.
    TickAgain(usize),
    /// The scheduler should wait for an external signal before attempting another `tick`.
    WaitForSignal,
}

#[cfg(test)]
mod tests {
    use std::io::{BufWriter, Seek, Write};
    use std::time::Instant;

    use big_s::S;
    use crossbeam::channel::RecvTimeoutError;
    use file_store::File;
    use meili_snap::snapshot;
    use meilisearch_types::document_formats::DocumentFormatError;
    use meilisearch_types::milli::obkv_to_json;
    use meilisearch_types::milli::update::IndexDocumentsMethod::{
        ReplaceDocuments, UpdateDocuments,
    };
    use meilisearch_types::tasks::IndexSwap;
    use meilisearch_types::VERSION_FILE_NAME;
    use tempfile::{NamedTempFile, TempDir};
    use time::Duration;
    use uuid::Uuid;
    use Breakpoint::*;

    use super::*;
    use crate::insta_snapshot::{snapshot_bitmap, snapshot_index_scheduler};

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
            autobatching_enabled: bool,
            planned_failures: Vec<(usize, FailureLocation)>,
        ) -> (Self, IndexSchedulerHandle) {
            let tempdir = TempDir::new().unwrap();
            let (sender, receiver) = crossbeam::channel::bounded(0);

            let options = IndexSchedulerOptions {
                version_file_path: tempdir.path().join(VERSION_FILE_NAME),
                auth_path: tempdir.path().join("auth"),
                tasks_path: tempdir.path().join("db_path"),
                update_file_path: tempdir.path().join("file_store"),
                indexes_path: tempdir.path().join("indexes"),
                snapshots_path: tempdir.path().join("snapshots"),
                dumps_path: tempdir.path().join("dumps"),
                task_db_size: 1000 * 1000, // 1 MB, we don't use MiB on purpose.
                index_base_map_size: 1000 * 1000, // 1 MB, we don't use MiB on purpose.
                index_growth_amount: 1000 * 1000, // 1 MB
                index_count: 5,
                indexer_config: IndexerConfig::default(),
                autobatching_enabled,
            };

            let index_scheduler = Self::new(options, sender, planned_failures).unwrap();

            // To be 100% consistent between all test we're going to start the scheduler right now
            // and ensure it's in the expected starting state.
            let breakpoint = match receiver.recv_timeout(std::time::Duration::from_secs(1)) {
                Ok(b) => b,
                Err(RecvTimeoutError::Timeout) => {
                    panic!("The scheduler seems to be waiting for a new task while your test is waiting for a breakpoint.")
                }
                Err(RecvTimeoutError::Disconnected) => panic!("The scheduler crashed."),
            };
            assert_eq!(breakpoint, (Init, false));
            let index_scheduler_handle = IndexSchedulerHandle {
                _tempdir: tempdir,
                test_breakpoint_rcv: receiver,
                last_breakpoint: breakpoint.0,
            };

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

    /// Adapting to the new json reading interface
    pub fn read_json(
        bytes: &[u8],
        write: impl Write + Seek,
    ) -> std::result::Result<u64, DocumentFormatError> {
        let temp_file = NamedTempFile::new().unwrap();
        let mut buffer = BufWriter::new(temp_file.reopen().unwrap());
        buffer.write_all(bytes).unwrap();
        buffer.flush().unwrap();
        meilisearch_types::document_formats::read_json(temp_file.as_file(), write)
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
        let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
        (file, documents_count)
    }

    pub struct IndexSchedulerHandle {
        _tempdir: TempDir,
        test_breakpoint_rcv: crossbeam::channel::Receiver<(Breakpoint, bool)>,
        last_breakpoint: Breakpoint,
    }

    impl IndexSchedulerHandle {
        /// Advance the scheduler to the next tick.
        /// Panic
        /// * If the scheduler is waiting for a task to be registered.
        /// * If the breakpoint queue is in a bad state.
        #[track_caller]
        fn advance(&mut self) -> Breakpoint {
            let (breakpoint_1, b) = match self
                .test_breakpoint_rcv
                .recv_timeout(std::time::Duration::from_secs(5))
            {
                Ok(b) => b,
                Err(RecvTimeoutError::Timeout) => {
                    panic!("The scheduler seems to be waiting for a new task while your test is waiting for a breakpoint.")
                }
                Err(RecvTimeoutError::Disconnected) => panic!("The scheduler crashed."),
            };
            // if we've already encountered a breakpoint we're supposed to be stuck on the false
            // and we expect the same variant with the true to come now.
            assert_eq!(
                (breakpoint_1, b),
                (self.last_breakpoint, true),
                "Internal error in the test suite. In the previous iteration I got `({:?}, false)` and now I got `({:?}, {:?})`.",
                self.last_breakpoint,
                breakpoint_1,
                b,
            );

            let (breakpoint_2, b) = match self
                .test_breakpoint_rcv
                .recv_timeout(std::time::Duration::from_secs(5))
            {
                Ok(b) => b,
                Err(RecvTimeoutError::Timeout) => {
                    panic!("The scheduler seems to be waiting for a new task while your test is waiting for a breakpoint.")
                }
                Err(RecvTimeoutError::Disconnected) => panic!("The scheduler crashed."),
            };
            assert!(!b, "Found the breakpoint handle in a bad state. Check your test suite");

            self.last_breakpoint = breakpoint_2;

            breakpoint_2
        }

        /// Advance the scheduler until all the provided breakpoints are reached in order.
        #[track_caller]
        fn advance_till(&mut self, breakpoints: impl IntoIterator<Item = Breakpoint>) {
            for breakpoint in breakpoints {
                let b = self.advance();
                assert_eq!(
                    b, breakpoint,
                    "Was expecting the breakpoint `{:?}` but instead got `{:?}`.",
                    breakpoint, b
                );
            }
        }

        /// Wait for `n` successful batches.
        #[track_caller]
        fn advance_n_successful_batches(&mut self, n: usize) {
            for _ in 0..n {
                self.advance_one_successful_batch();
            }
        }

        /// Wait for `n` failed batches.
        #[track_caller]
        fn advance_n_failed_batches(&mut self, n: usize) {
            for _ in 0..n {
                self.advance_one_failed_batch();
            }
        }

        // Wait for one successful batch.
        #[track_caller]
        fn advance_one_successful_batch(&mut self) {
            self.advance_till([Start, BatchCreated]);
            loop {
                match self.advance() {
                    // the process_batch function can call itself recursively, thus we need to
                    // accept as may InsideProcessBatch as possible before moving to the next state.
                    InsideProcessBatch => (),
                    // the batch went successfully, we can stop the loop and go on with the next states.
                    ProcessBatchSucceeded => break,
                    AbortedIndexation => panic!("The batch was aborted."),
                    ProcessBatchFailed => panic!("The batch failed."),
                    breakpoint => panic!("Encountered an impossible breakpoint `{:?}`, this is probably an issue with the test suite.", breakpoint),
                }
            }

            self.advance_till([AfterProcessing]);
        }

        // Wait for one failed batch.
        #[track_caller]
        fn advance_one_failed_batch(&mut self) {
            self.advance_till([Start, BatchCreated]);
            loop {
                match self.advance() {
                    // the process_batch function can call itself recursively, thus we need to
                    // accept as may InsideProcessBatch as possible before moving to the next state.
                    InsideProcessBatch => (),
                    // the batch went failed, we can stop the loop and go on with the next states.
                    ProcessBatchFailed => break,
                    ProcessBatchSucceeded => panic!("The batch succeeded. (and it wasn't supposed to sorry)"),
                    AbortedIndexation => panic!("The batch was aborted."),
                    breakpoint => panic!("Encountered an impossible breakpoint `{:?}`, this is probably an issue with the test suite.", breakpoint),
                }
            }
            self.advance_till([AfterProcessing]);
        }
    }

    #[test]
    fn register() {
        // In this test, the handle doesn't make any progress, we only check that the tasks are registered
        let (index_scheduler, mut _handle) = IndexScheduler::test(true, vec![]);

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

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "everything_is_succesfully_registered");
    }

    #[test]
    fn insert_task_while_another_task_is_processing() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        index_scheduler.register(index_creation_task("index_a", "id")).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        handle.advance_till([Start, BatchCreated]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_batch_creation");

        // while the task is processing can we register another task?
        index_scheduler.register(index_creation_task("index_b", "id")).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

        index_scheduler
            .register(KindWithContent::IndexDeletion { index_uid: S("index_a") })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");
    }

    /// We send a lot of tasks but notify the tasks scheduler only once as
    /// we send them very fast, we must make sure that they are all processed.
    #[test]
    fn process_tasks_inserted_without_new_signal() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("cattos"), primary_key: None })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

        index_scheduler
            .register(KindWithContent::IndexDeletion { index_uid: S("doggos") })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_second_task");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_third_task");
    }

    #[test]
    fn process_tasks_without_autobatching() {
        let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        index_scheduler
            .register(KindWithContent::DocumentClear { index_uid: S("doggos") })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

        index_scheduler
            .register(KindWithContent::DocumentClear { index_uid: S("doggos") })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");

        index_scheduler
            .register(KindWithContent::DocumentClear { index_uid: S("doggos") })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_fourth_task");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "fourth");
    }

    #[test]
    fn task_deletion_undeleteable() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

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
                tasks: RoaringBitmap::from_iter([0, 1]),
            })
            .unwrap();
        // again, no progress made at all, but one more task is registered
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_enqueued");

        // now we create the first batch
        handle.advance_till([Start, BatchCreated]);

        // the task deletion should now be "processing"
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processing");

        handle.advance_till([InsideProcessBatch, ProcessBatchSucceeded, AfterProcessing]);
        // after the task deletion is processed, no task should actually have been deleted,
        // because the tasks with ids 0 and 1 were still "enqueued", and thus undeleteable
        // the "task deletion" task should be marked as "succeeded" and, in its details, the
        // number of deleted tasks should be 0
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_done");
    }

    #[test]
    fn task_deletion_deleteable() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

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

        handle.advance_one_successful_batch();
        // first addition of documents should be successful
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_processed");

        // Now we delete the first task
        index_scheduler
            .register(KindWithContent::TaskDeletion {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0]),
            })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_task_deletion");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processed");
    }

    #[test]
    fn task_deletion_delete_same_task_twice() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

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

        handle.advance_one_successful_batch();
        // first addition of documents should be successful
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_processed");

        // Now we delete the first task multiple times in a row
        for _ in 0..2 {
            index_scheduler
                .register(KindWithContent::TaskDeletion {
                    query: "test_query".to_owned(),
                    tasks: RoaringBitmap::from_iter([0]),
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        for _ in 0..2 {
            handle.advance_one_successful_batch();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processed");
    }

    #[test]
    fn document_addition() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
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
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_register");

        handle.advance_till([Start, BatchCreated]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_the_batch_creation");

        handle.advance_till([InsideProcessBatch, ProcessBatchSucceeded, AfterProcessing]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "once_everything_is_processed");
    }

    #[test]
    fn document_addition_and_index_deletion() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
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
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

        index_scheduler
            .register(KindWithContent::IndexDeletion { index_uid: S("doggos") })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");

        handle.advance_one_successful_batch(); // The index creation.
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "before_index_creation");
        handle.advance_one_successful_batch(); // // after the execution of the two tasks in a single batch.
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "both_task_succeeded");
    }

    #[test]
    fn do_not_batch_task_of_different_indexes() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);
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
            handle.advance_one_successful_batch();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");
    }

    #[test]
    fn swap_indexes() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

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

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "create_a");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "create_b");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "create_c");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "create_d");

        index_scheduler
            .register(KindWithContent::IndexSwap {
                swaps: vec![
                    IndexSwap { indexes: ("a".to_owned(), "b".to_owned()) },
                    IndexSwap { indexes: ("c".to_owned(), "d".to_owned()) },
                ],
            })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_swap_registered");
        index_scheduler
            .register(KindWithContent::IndexSwap {
                swaps: vec![IndexSwap { indexes: ("a".to_owned(), "c".to_owned()) }],
            })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "two_swaps_registered");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_swap_processed");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_swap_processed");

        index_scheduler.register(KindWithContent::IndexSwap { swaps: vec![] }).unwrap();
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_empty_swap_processed");
    }

    #[test]
    fn swap_indexes_errors() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

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
        handle.advance_n_successful_batches(4);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_the_index_creation");

        let first_snap = snapshot_index_scheduler(&index_scheduler);
        snapshot!(first_snap, name: "initial_tasks_processed");

        let err = index_scheduler
            .register(KindWithContent::IndexSwap {
                swaps: vec![
                    IndexSwap { indexes: ("a".to_owned(), "b".to_owned()) },
                    IndexSwap { indexes: ("b".to_owned(), "a".to_owned()) },
                ],
            })
            .unwrap_err();
        snapshot!(format!("{err}"), @"Indexes must be declared only once during a swap. `a`, `b` were specified several times.");

        let second_snap = snapshot_index_scheduler(&index_scheduler);
        assert_eq!(first_snap, second_snap);

        // Index `e` does not exist, but we don't check its existence yet
        index_scheduler
            .register(KindWithContent::IndexSwap {
                swaps: vec![
                    IndexSwap { indexes: ("a".to_owned(), "b".to_owned()) },
                    IndexSwap { indexes: ("c".to_owned(), "e".to_owned()) },
                    IndexSwap { indexes: ("d".to_owned(), "f".to_owned()) },
                ],
            })
            .unwrap();
        handle.advance_one_failed_batch();
        // Now the first swap should have an error message saying `e` and `f` do not exist
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_swap_failed");
    }

    #[test]
    fn document_addition_and_index_deletion_on_unexisting_index() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
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

        handle.advance_n_successful_batches(1);

        snapshot!(snapshot_index_scheduler(&index_scheduler));
    }

    #[test]
    fn cancel_enqueued_task() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
        file0.persist().unwrap();

        let to_enqueue = [
            replace_document_import_task("catto", None, 0, documents_count0),
            KindWithContent::TaskCancelation {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0]),
            },
        ];
        for task in to_enqueue {
            let _ = index_scheduler.register(task).unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_processed");
    }

    #[test]
    fn cancel_succeeded_task() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
        file0.persist().unwrap();

        let _ = index_scheduler
            .register(replace_document_import_task("catto", None, 0, documents_count0))
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_task_processed");

        index_scheduler
            .register(KindWithContent::TaskCancelation {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0]),
            })
            .unwrap();

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_processed");
    }

    #[test]
    fn cancel_processing_task() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
        file0.persist().unwrap();

        let _ = index_scheduler
            .register(replace_document_import_task("catto", None, 0, documents_count0))
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        handle.advance_till([Start, BatchCreated, InsideProcessBatch]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_task_processing");

        index_scheduler
            .register(KindWithContent::TaskCancelation {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0]),
            })
            .unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_task_registered");
        // Now we check that we can reach the AbortedIndexation error handling
        handle.advance_till([AbortedIndexation]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "aborted_indexation");

        // handle.advance_till([Start, BatchCreated, BeforeProcessing, AfterProcessing]);
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_processed");
    }

    #[test]
    fn cancel_mix_of_tasks() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
        file0.persist().unwrap();
        let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);
        file1.persist().unwrap();
        let (file2, documents_count2) = sample_documents(&index_scheduler, 2, 2);
        file2.persist().unwrap();

        let to_enqueue = [
            replace_document_import_task("catto", None, 0, documents_count0),
            replace_document_import_task("beavero", None, 1, documents_count1),
            replace_document_import_task("wolfo", None, 2, documents_count2),
        ];
        for task in to_enqueue {
            let _ = index_scheduler.register(task).unwrap();
            index_scheduler.assert_internally_consistent();
        }
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_task_processed");

        handle.advance_till([Start, BatchCreated, InsideProcessBatch]);
        index_scheduler
            .register(KindWithContent::TaskCancelation {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0, 1, 2]),
            })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processing_second_task_cancel_enqueued");

        handle.advance_till([AbortedIndexation]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "aborted_indexation");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_processed");
    }

    #[test]
    fn test_document_replace() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
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
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler));

        // everything should be batched together.
        handle.advance_n_successful_batches(1);
        snapshot!(snapshot_index_scheduler(&index_scheduler));

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_update() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: UpdateDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler));

        // everything should be batched together.
        handle.advance_n_successful_batches(1);
        snapshot!(snapshot_index_scheduler(&index_scheduler));

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_mixed_document_addition() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let method = if i % 2 == 0 { UpdateDocuments } else { ReplaceDocuments };

            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Only half of the task should've been processed since we can't autobatch replace and update together.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_replace_without_autobatching() {
        let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
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
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Nothing should be batched thus half of the tasks are processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        // Everything is processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_update_without_autobatching() {
        let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: UpdateDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Nothing should be batched thus half of the tasks are processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        // Everything is processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
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
    fn query_tasks_from_and_limit() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let kind = index_creation_task("doggo", "bone");
        let _task = index_scheduler.register(kind).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        let kind = index_creation_task("whalo", "plankton");
        let _task = index_scheduler.register(kind).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");
        let kind = index_creation_task("catto", "his_own_vomit");
        let _task = index_scheduler.register(kind).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");

        handle.advance_n_successful_batches(3);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_all_tasks");

        let rtxn = index_scheduler.env.read_txn().unwrap();
        let query = Query { limit: Some(0), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query { limit: Some(1), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        let query = Query { limit: Some(2), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[1,2,]");

        let query = Query { from: Some(1), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,]");

        let query = Query { from: Some(2), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,2,]");

        let query = Query { from: Some(1), limit: Some(1), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[1,]");

        let query = Query { from: Some(1), limit: Some(2), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,]");
    }

    #[test]
    fn query_tasks_simple() {
        let start_time = OffsetDateTime::now_utc();

        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(3, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _task = index_scheduler.register(kind).unwrap();
        let kind = index_creation_task("doggo", "sheep");
        let _task = index_scheduler.register(kind).unwrap();
        let kind = index_creation_task("whalo", "fish");
        let _task = index_scheduler.register(kind).unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "start");

        handle.advance_till([Start, BatchCreated]);

        let rtxn = index_scheduler.env.read_txn().unwrap();

        let query = Query { statuses: Some(vec![Status::Processing]), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,]"); // only the processing tasks in the first tick

        let query = Query { statuses: Some(vec![Status::Enqueued]), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[1,2,]"); // only the enqueued tasks in the first tick

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,2,]"); // both enqueued and processing tasks in the first tick

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            after_started_at: Some(start_time),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // both enqueued and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the test, which should excludes the enqueued tasks
        snapshot!(snapshot_bitmap(&tasks), @"[0,]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            before_started_at: Some(start_time),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // both enqueued and processing tasks in the first tick, but limited to those with a started_at
        // that comes before the start of the test, which should excludes all of them
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            after_started_at: Some(start_time),
            before_started_at: Some(start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // both enqueued and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the test and before one minute after the start of the test,
        // which should exclude the enqueued tasks and include the only processing task
        snapshot!(snapshot_bitmap(&tasks), @"[0,]");

        handle.advance_till([
            InsideProcessBatch,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
            Start,
            BatchCreated,
        ]);

        let rtxn = index_scheduler.env.read_txn().unwrap();

        let second_start_time = OffsetDateTime::now_utc();

        let query = Query {
            statuses: Some(vec![Status::Succeeded, Status::Processing]),
            after_started_at: Some(start_time),
            before_started_at: Some(start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // both succeeded and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the test and before one minute after the start of the test,
        // which should include all tasks
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,]");

        let query = Query {
            statuses: Some(vec![Status::Succeeded, Status::Processing]),
            before_started_at: Some(start_time),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // both succeeded and processing tasks in the first tick, but limited to those with a started_at
        // that comes before the start of the test, which should exclude all tasks
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Succeeded, Status::Processing]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // both succeeded and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the second part of the test and before one minute after the
        // second start of the test, which should exclude all tasks
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        // now we make one more batch, the started_at field of the new tasks will be past `second_start_time`
        handle.advance_till([
            InsideProcessBatch,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
            Start,
            BatchCreated,
        ]);

        let rtxn = index_scheduler.env.read_txn().unwrap();

        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // we run the same query to verify that, and indeed find that the last task is matched
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Succeeded, Status::Processing]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // enqueued, succeeded, or processing tasks started after the second part of the test, should
        // again only return the last task
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        handle.advance_till([ProcessBatchFailed, AfterProcessing]);
        let rtxn = index_scheduler.read_txn().unwrap();

        // now the last task should have failed
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "end");
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // so running the last query should return nothing
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // but the same query on failed tasks should return the last task
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // but the same query on failed tasks should return the last task
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            uids: Some(vec![1]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // same query but with an invalid uid
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            uids: Some(vec![2]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // same query but with a valid uid
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");
    }

    #[test]
    fn query_tasks_special_rules() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(3, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _task = index_scheduler.register(kind).unwrap();
        let kind = index_creation_task("doggo", "sheep");
        let _task = index_scheduler.register(kind).unwrap();
        let kind = KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: ("catto".to_owned(), "doggo".to_owned()) }],
        };
        let _task = index_scheduler.register(kind).unwrap();
        let kind = KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: ("catto".to_owned(), "whalo".to_owned()) }],
        };
        let _task = index_scheduler.register(kind).unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "start");

        handle.advance_till([Start, BatchCreated]);

        let rtxn = index_scheduler.env.read_txn().unwrap();

        let query = Query { index_uids: Some(vec!["catto".to_owned()]), ..Default::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // only the first task associated with catto is returned, the indexSwap tasks are excluded!
        snapshot!(snapshot_bitmap(&tasks), @"[0,]");

        let query = Query { index_uids: Some(vec!["catto".to_owned()]), ..Default::default() };
        let tasks = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &Some(vec!["doggo".to_owned()]))
            .unwrap();
        // we have asked for only the tasks associated with catto, but are only authorized to retrieve the tasks
        // associated with doggo -> empty result
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query::default();
        let tasks = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &Some(vec!["doggo".to_owned()]))
            .unwrap();
        // we asked for all the tasks, but we are only authorized to retrieve the doggo tasks
        // -> only the index creation of doggo should be returned
        snapshot!(snapshot_bitmap(&tasks), @"[1,]");

        let query = Query::default();
        let tasks = index_scheduler
            .get_task_ids_from_authorized_indexes(
                &rtxn,
                &query,
                &Some(vec!["catto".to_owned(), "doggo".to_owned()]),
            )
            .unwrap();
        // we asked for all the tasks, but we are only authorized to retrieve the doggo and catto tasks
        // -> all tasks except the swap of catto with whalo are returned
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,]");

        let query = Query::default();
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // we asked for all the tasks with all index authorized -> all tasks returned
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,2,3,]");
    }

    #[test]
    fn query_tasks_canceled_by() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(3, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _ = index_scheduler.register(kind).unwrap();
        let kind = index_creation_task("doggo", "sheep");
        let _ = index_scheduler.register(kind).unwrap();
        let kind = KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: ("catto".to_owned(), "doggo".to_owned()) }],
        };
        let _task = index_scheduler.register(kind).unwrap();

        handle.advance_n_successful_batches(1);
        let kind = KindWithContent::TaskCancelation {
            query: "test_query".to_string(),
            tasks: [0, 1, 2, 3].into_iter().collect(),
        };
        let task_cancelation = index_scheduler.register(kind).unwrap();
        handle.advance_n_successful_batches(1);

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "start");

        let rtxn = index_scheduler.read_txn().unwrap();
        let query = Query { canceled_by: Some(vec![task_cancelation.uid]), ..Query::default() };
        let tasks =
            index_scheduler.get_task_ids_from_authorized_indexes(&rtxn, &query, &None).unwrap();
        // 0 is not returned because it was not canceled, 3 is not returned because it is the uid of the
        // taskCancelation itself
        snapshot!(snapshot_bitmap(&tasks), @"[1,2,]");

        let query = Query { canceled_by: Some(vec![task_cancelation.uid]), ..Query::default() };
        let tasks = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &Some(vec!["doggo".to_string()]))
            .unwrap();
        // Return only 1 because the user is not authorized to see task 2
        snapshot!(snapshot_bitmap(&tasks), @"[1,]");
    }

    #[test]
    fn fail_in_process_batch_for_index_creation() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");

        let _task = index_scheduler.register(kind).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_register");

        handle.advance_one_failed_batch();

        // Still in the first iteration
        assert_eq!(*index_scheduler.run_loop_iteration.read().unwrap(), 1);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "index_creation_failed");
    }

    #[test]
    fn fail_in_process_batch_for_document_addition() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::InsideProcessBatch)]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
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
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        handle.advance_till([Start, BatchCreated]);

        snapshot!(
            snapshot_index_scheduler(&index_scheduler),
            name: "document_addition_batch_created"
        );

        handle.advance_till([ProcessBatchFailed, AfterProcessing]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "document_addition_failed");
    }

    #[test]
    fn fail_in_update_task_after_process_batch_success_for_document_addition() {
        let (index_scheduler, mut handle) = IndexScheduler::test(
            true,
            vec![(1, FailureLocation::UpdatingTaskAfterProcessBatchSuccess { task_uid: 0 })],
        );

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
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
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        handle.advance_till([Start]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "document_addition_succeeded_but_index_scheduler_not_updated");

        handle.advance_till([BatchCreated, InsideProcessBatch, ProcessBatchSucceeded]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_batch_succeeded");

        // At this point the next time the scheduler will try to progress it should encounter
        // a critical failure and have to wait for 1s before retrying anything.

        let before_failure = Instant::now();
        handle.advance_till([Start]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_failing_to_commit");
        let failure_duration = before_failure.elapsed();
        assert!(failure_duration.as_millis() >= 1000);

        handle.advance_till([
            BatchCreated,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
        ]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_successfully_processed");
    }

    #[test]
    fn test_document_addition_cant_create_index_without_index() {
        // We're going to autobatch multiple document addition that don't have
        // the right to create an index while there is no index currently.
        // Thus, everything should be batched together and a IndexDoesNotExists
        // error should be throwed.
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: false,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Everything should be batched together.
        handle.advance_till([
            Start,
            BatchCreated,
            InsideProcessBatch,
            ProcessBatchFailed,
            AfterProcessing,
        ]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_processing_the_10_tasks");

        // The index should not exists.
        snapshot!(format!("{}", index_scheduler.index("doggos").map(|_| ()).unwrap_err()), @"Index `doggos` not found.");
    }

    #[test]
    fn test_document_addition_cant_create_index_without_index_without_autobatching() {
        // We're going to execute multiple document addition that don't have
        // the right to create an index while there is no index currently.
        // Since the autobatching is disabled, every tasks should be processed
        // sequentially and throw an IndexDoesNotExists.
        let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: false,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Nothing should be batched thus half of the tasks are processed.
        handle.advance_n_failed_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        // Everything is processed.
        handle.advance_n_failed_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // The index should not exists.
        snapshot!(format!("{}", index_scheduler.index("doggos").map(|_| ()).unwrap_err()), @"Index `doggos` not found.");
    }

    #[test]
    fn test_document_addition_cant_create_index_with_index() {
        // We're going to autobatch multiple document addition that don't have
        // the right to create an index while there is already an index.
        // Thus, everything should be batched together and no error should be
        // throwed.
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        // Create the index.
        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: false,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Everything should be batched together.
        handle.advance_n_successful_batches(1);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_processing_the_10_tasks");

        // Has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_cant_create_index_with_index_without_autobatching() {
        // We're going to execute multiple document addition that don't have
        // the right to create an index while there is no index currently.
        // Since the autobatching is disabled, every tasks should be processed
        // sequentially and throw an IndexDoesNotExists.
        let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

        // Create the index.
        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: false,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Nothing should be batched thus half of the tasks are processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        // Everything is processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // Has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_mixed_rights_with_index() {
        // We're going to autobatch multiple document addition.
        // - The index already exists
        // - The first document addition don't have the right to create an index
        //   can it batch with the other one?
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        // Create the index.
        index_scheduler
            .register(KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None })
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );
            let allow_index_creation = i % 2 != 0;

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Everything should be batched together.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // Has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_mixed_right_without_index_starts_with_cant_create() {
        // We're going to autobatch multiple document addition.
        // - The index does not exists
        // - The first document addition don't have the right to create an index
        // - The second do. They should not batch together.
        // - The second should batch with everything else as it's going to create an index.
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );
            let allow_index_creation = i % 2 != 0;

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // A first batch should be processed with only the first documentAddition that's going to fail.
        handle.advance_one_failed_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "only_first_task_failed");

        // Everything else should be batched together.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // Has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_multiple_primary_key() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in ["id", "bork", "bloup"].iter().enumerate() {
            let content = format!(
                r#"{{
                    "id": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S(primary_key)),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_3_tasks");

        // A first batch should be processed with only the first documentAddition.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "only_first_task_succeed");

        // The second batch should fail.
        handle.advance_one_failed_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_task_fails");

        // The second batch should fail.
        handle.advance_one_failed_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_fails");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"id");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_multiple_primary_key_batch_wrong_key() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in ["id", "bork", "bork"].iter().enumerate() {
            let content = format!(
                r#"{{
                    "id": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S(primary_key)),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_3_tasks");

        // A first batch should be processed with only the first documentAddition.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "only_first_task_succeed");

        // The second batch should fail and contains two tasks.
        handle.advance_one_failed_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_and_third_tasks_fails");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"id");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_bad_primary_key() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in ["bork", "bork", "id", "bork", "id"].iter().enumerate() {
            let content = format!(
                r#"{{
                    "id": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S(primary_key)),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_5_tasks");

        // A first batch should be processed with only the first two documentAddition.
        // it should fails because the documents don't contains any `bork` field.
        // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_and_second_task_fails");

        // The primary key should be set to none since we failed the batch.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap();
        snapshot!(primary_key.is_none(), @"true");

        // The second batch should succeed and only contains one task.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_succeeds");

        // The primary key should be set to `id` since this batch succeeded.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"id");

        // We're trying to `bork` again, but now there is already a primary key set for this index.
        handle.advance_one_failed_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "fourth_task_fails");

        // Finally the last task should succeed since its primary key is the same as the valid one.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "fifth_task_succeeds");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"id");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_set_and_null_primary_key() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in
            [None, Some("bork"), Some("paw"), None, None, Some("paw")].into_iter().enumerate()
        {
            let content = format!(
                r#"{{
                    "paw": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: primary_key.map(|pk| pk.to_string()),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_6_tasks");

        // A first batch should contains only one task that fails because we can't infer the primary key.
        // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_task_fails");

        // The second batch should contains only one task that fails because we bork is not a valid primary key.
        // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_task_fails");

        // No primary key should be set at this point.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap();
        snapshot!(primary_key.is_none(), @"true");

        // The third batch should succeed and only contains one task.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_succeeds");

        // The primary key should be set to `id` since this batch succeeded.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"paw");

        // We should be able to batch together the next two tasks that don't specify any primary key
        // + the last task that matches the current primary-key. Everything should succeed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_other_tasks_succeeds");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"paw");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_set_and_null_primary_key_inference_works() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in [None, Some("bork"), Some("doggoid"), None, None, Some("doggoid")]
            .into_iter()
            .enumerate()
        {
            let content = format!(
                r#"{{
                    "doggoid": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), file.as_file_mut()).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: primary_key.map(|pk| pk.to_string()),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                })
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_6_tasks");

        // A first batch should contains only one task that succeed and sets the primary key to `doggoid`.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_task_succeed");

        // Checking the primary key.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap();
        snapshot!(primary_key.is_none(), @"false");

        // The second batch should contains only one task that fails because it tries to update the primary key to `bork`.
        handle.advance_one_failed_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_task_fails");

        // The third batch should succeed and only contains one task.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_succeeds");

        // We should be able to batch together the next two tasks that don't specify any primary key
        // + the last task that matches the current primary-key. Everything should succeed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_other_tasks_succeeds");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"doggoid");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn panic_in_process_batch_for_index_creation() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::PanicInsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");

        let _task = index_scheduler.register(kind).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        handle.advance_till([Start, BatchCreated, ProcessBatchFailed, AfterProcessing]);

        // Still in the first iteration
        assert_eq!(*index_scheduler.run_loop_iteration.read().unwrap(), 1);
        // No matter what happens in process_batch, the index_scheduler should be internally consistent
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "index_creation_failed");
    }
}

use std::collections::{BTreeSet, HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering;

use meilisearch_types::batches::BatchId;
use meilisearch_types::heed::{Database, RoTxn, RwTxn};
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::milli::{self, CboRoaringBitmapCodec, ChannelCongestion};
use meilisearch_types::tasks::{Details, IndexSwap, Kind, KindWithContent, Status, Task};
use meilisearch_types::versioning::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use milli::update::Settings as MilliSettings;
use roaring::{MultiOps, RoaringBitmap};

use super::create_batch::Batch;
use crate::processing::{
    AtomicBatchStep, AtomicTaskStep, CreateIndexProgress, DeleteIndexProgress, FinalizingIndexStep,
    InnerSwappingTwoIndexes, SwappingTheIndexes, TaskCancelationProgress, TaskDeletionProgress,
    UpdateIndexProgress,
};
use crate::utils::{consecutive_ranges, swap_index_uid_in_task, ProcessingBatch};
use crate::{Error, IndexScheduler, Result, TaskId, BEI128};

#[derive(Debug, Default)]
pub struct ProcessBatchInfo {
    /// The write channel congestion. None when unavailable: settings update.
    pub congestion: Option<ChannelCongestion>,
    /// The sizes of the different databases before starting the indexation.
    pub pre_commit_dabases_sizes: indexmap::IndexMap<&'static str, usize>,
    /// The sizes of the different databases after commiting the indexation.
    pub post_commit_dabases_sizes: indexmap::IndexMap<&'static str, usize>,
}

impl IndexScheduler {
    /// Apply the operation associated with the given batch.
    ///
    /// ## Return
    /// The list of tasks that were processed. The metadata of each task in the returned
    /// list is updated accordingly, with the exception of the its date fields
    /// [`finished_at`](meilisearch_types::tasks::Task::finished_at) and [`started_at`](meilisearch_types::tasks::Task::started_at).
    #[tracing::instrument(level = "trace", skip(self, batch, progress), target = "indexing::scheduler", fields(batch=batch.to_string()))]
    pub(crate) fn process_batch(
        &self,
        batch: Batch,
        current_batch: &mut ProcessingBatch,
        progress: Progress,
    ) -> Result<(Vec<Task>, ProcessBatchInfo)> {
        #[cfg(test)]
        {
            self.maybe_fail(crate::test_utils::FailureLocation::InsideProcessBatch)?;
            self.maybe_fail(crate::test_utils::FailureLocation::PanicInsideProcessBatch)?;
            self.breakpoint(crate::test_utils::Breakpoint::InsideProcessBatch);
        }

        match batch {
            Batch::TaskCancelation { mut task } => {
                // 1. Retrieve the tasks that matched the query at enqueue-time.
                let matched_tasks =
                    if let KindWithContent::TaskCancelation { tasks, query: _ } = &task.kind {
                        tasks
                    } else {
                        unreachable!()
                    };

                let rtxn = self.env.read_txn()?;
                let mut canceled_tasks = self.cancel_matched_tasks(
                    &rtxn,
                    task.uid,
                    current_batch,
                    matched_tasks,
                    &progress,
                )?;

                task.status = Status::Succeeded;
                match &mut task.details {
                    Some(Details::TaskCancelation {
                        matched_tasks: _,
                        canceled_tasks: canceled_tasks_details,
                        original_filter: _,
                    }) => {
                        *canceled_tasks_details = Some(canceled_tasks.len() as u64);
                    }
                    _ => unreachable!(),
                }

                canceled_tasks.push(task);

                Ok((canceled_tasks, ProcessBatchInfo::default()))
            }
            Batch::TaskDeletions(mut tasks) => {
                // 1. Retrieve the tasks that matched the query at enqueue-time.
                let mut matched_tasks = RoaringBitmap::new();

                for task in tasks.iter() {
                    if let KindWithContent::TaskDeletion { tasks, query: _ } = &task.kind {
                        matched_tasks |= tasks;
                    } else {
                        unreachable!()
                    }
                }

                let mut deleted_tasks = self.delete_matched_tasks(&matched_tasks, &progress)?;

                for task in tasks.iter_mut() {
                    task.status = Status::Succeeded;
                    let KindWithContent::TaskDeletion { tasks, query: _ } = &task.kind else {
                        unreachable!()
                    };

                    let deleted_tasks_count = deleted_tasks.intersection_len(tasks);
                    deleted_tasks -= tasks;

                    match &mut task.details {
                        Some(Details::TaskDeletion {
                            matched_tasks: _,
                            deleted_tasks,
                            original_filter: _,
                        }) => {
                            *deleted_tasks = Some(deleted_tasks_count);
                        }
                        _ => unreachable!(),
                    }
                }

                debug_assert!(
                    deleted_tasks.is_empty(),
                    "There should be no tasks left to delete after processing the batch"
                );

                Ok((tasks, ProcessBatchInfo::default()))
            }
            Batch::SnapshotCreation(tasks) => self
                .process_snapshot(progress, tasks)
                .map(|tasks| (tasks, ProcessBatchInfo::default())),
            Batch::Dump(task) => self
                .process_dump_creation(progress, task)
                .map(|tasks| (tasks, ProcessBatchInfo::default())),
            Batch::IndexOperation { op, must_create_index } => {
                let index_uid = op.index_uid().to_string();
                let index = if must_create_index {
                    // create the index if it doesn't already exist
                    let wtxn = self.env.write_txn()?;
                    self.index_mapper.create_index(wtxn, &index_uid, None)?
                } else {
                    let rtxn = self.env.read_txn()?;
                    self.index_mapper.index(&rtxn, &index_uid)?
                };

                let mut index_wtxn = index.write_txn()?;

                let index_version = index.get_version(&index_wtxn)?.unwrap_or((1, 12, 0));
                let package_version = (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);
                if index_version != package_version {
                    return Err(Error::IndexVersionMismatch {
                        index: index_uid,
                        index_version,
                        package_version,
                    });
                }

                // the index operation can take a long time, so save this handle to make it available to the search for the duration of the tick
                self.index_mapper
                    .set_currently_updating_index(Some((index_uid.clone(), index.clone())));

                let pre_commit_dabases_sizes = index.database_sizes(&index_wtxn)?;
                let (tasks, congestion) = self.apply_index_operation(
                    &mut index_wtxn,
                    &index,
                    op,
                    &progress,
                    current_batch.embedder_stats.clone(),
                )?;

                {
                    progress.update_progress(FinalizingIndexStep::Committing);
                    let span = tracing::trace_span!(target: "indexing::scheduler", "commit");
                    let _entered = span.enter();

                    index_wtxn.commit()?;
                }

                // if the update processed successfully, we're going to store the new
                // stats of the index. Since the tasks have already been processed and
                // this is a non-critical operation. If it fails, we should not fail
                // the entire batch.
                let mut post_commit_dabases_sizes = None;
                let res = || -> Result<()> {
                    progress.update_progress(FinalizingIndexStep::ComputingStats);
                    let index_rtxn = index.read_txn()?;
                    let stats = crate::index_mapper::IndexStats::new(&index, &index_rtxn)
                        .map_err(|e| Error::from_milli(e, Some(index_uid.to_string())))?;
                    let mut wtxn = self.env.write_txn()?;
                    self.index_mapper.store_stats_of(&mut wtxn, &index_uid, &stats)?;
                    post_commit_dabases_sizes = Some(index.database_sizes(&index_rtxn)?);
                    wtxn.commit()?;
                    Ok(())
                }();

                match res {
                    Ok(_) => (),
                    Err(e) => tracing::error!(
                        error = &e as &dyn std::error::Error,
                        "Could not write the stats of the index"
                    ),
                }

                let info = ProcessBatchInfo {
                    congestion,
                    // In case we fail to the get post-commit sizes we decide
                    // that nothing changed and use the pre-commit sizes.
                    post_commit_dabases_sizes: post_commit_dabases_sizes
                        .unwrap_or_else(|| pre_commit_dabases_sizes.clone()),
                    pre_commit_dabases_sizes,
                };

                Ok((tasks, info))
            }
            Batch::IndexCreation { index_uid, primary_key, task } => {
                progress.update_progress(CreateIndexProgress::CreatingTheIndex);

                let wtxn = self.env.write_txn()?;
                if self.index_mapper.exists(&wtxn, &index_uid)? {
                    return Err(Error::IndexAlreadyExists(index_uid));
                }
                self.index_mapper.create_index(wtxn, &index_uid, None)?;

                self.process_batch(
                    Batch::IndexUpdate { index_uid, primary_key, task },
                    current_batch,
                    progress,
                )
            }
            Batch::IndexUpdate { index_uid, primary_key, mut task } => {
                progress.update_progress(UpdateIndexProgress::UpdatingTheIndex);
                let rtxn = self.env.read_txn()?;
                let index = self.index_mapper.index(&rtxn, &index_uid)?;

                if let Some(primary_key) = primary_key.clone() {
                    let mut index_wtxn = index.write_txn()?;
                    let mut builder = MilliSettings::new(
                        &mut index_wtxn,
                        &index,
                        self.index_mapper.indexer_config(),
                    );
                    builder.set_primary_key(primary_key);
                    let must_stop_processing = self.scheduler.must_stop_processing.clone();

                    builder
                        .execute(
                            &|| must_stop_processing.get(),
                            &progress,
                            current_batch.embedder_stats.clone(),
                        )
                        .map_err(|e| Error::from_milli(e, Some(index_uid.to_string())))?;
                    index_wtxn.commit()?;
                }

                // drop rtxn before starting a new wtxn on the same db
                rtxn.commit()?;

                task.status = Status::Succeeded;
                task.details = Some(Details::IndexInfo { primary_key });

                // if the update processed successfully, we're going to store the new
                // stats of the index. Since the tasks have already been processed and
                // this is a non-critical operation. If it fails, we should not fail
                // the entire batch.
                let res = || -> Result<()> {
                    let mut wtxn = self.env.write_txn()?;
                    let index_rtxn = index.read_txn()?;
                    let stats = crate::index_mapper::IndexStats::new(&index, &index_rtxn)
                        .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?;
                    self.index_mapper.store_stats_of(&mut wtxn, &index_uid, &stats)?;
                    wtxn.commit()?;
                    Ok(())
                }();

                match res {
                    Ok(_) => (),
                    Err(e) => tracing::error!(
                        error = &e as &dyn std::error::Error,
                        "Could not write the stats of the index"
                    ),
                }

                Ok((vec![task], ProcessBatchInfo::default()))
            }
            Batch::IndexDeletion { index_uid, index_has_been_created, mut tasks } => {
                progress.update_progress(DeleteIndexProgress::DeletingTheIndex);
                let wtxn = self.env.write_txn()?;

                // it's possible that the index doesn't exist
                let number_of_documents = || -> Result<u64> {
                    let index = self.index_mapper.index(&wtxn, &index_uid)?;
                    let index_rtxn = index.read_txn()?;
                    index
                        .number_of_documents(&index_rtxn)
                        .map_err(|e| Error::from_milli(e, Some(index_uid.to_string())))
                }()
                .unwrap_or_default();

                // The write transaction is directly owned and committed inside.
                match self.index_mapper.delete_index(wtxn, &index_uid) {
                    Ok(()) => (),
                    Err(Error::IndexNotFound(_)) if index_has_been_created => (),
                    Err(e) => return Err(e),
                }

                // We set all the tasks details to the default value.
                for task in &mut tasks {
                    task.status = Status::Succeeded;
                    task.details = match &task.kind {
                        KindWithContent::IndexDeletion { .. } => {
                            Some(Details::ClearAll { deleted_documents: Some(number_of_documents) })
                        }
                        otherwise => otherwise.default_finished_details(),
                    };
                }

                // Here we could also show that all the internal database sizes goes to 0
                // but it would mean opening the index and that's costly.
                Ok((tasks, ProcessBatchInfo::default()))
            }
            Batch::IndexSwap { mut task } => {
                progress.update_progress(SwappingTheIndexes::EnsuringCorrectnessOfTheSwap);

                let mut wtxn = self.env.write_txn()?;
                let swaps = if let KindWithContent::IndexSwap { swaps } = &task.kind {
                    swaps
                } else {
                    unreachable!()
                };
                let mut not_found_indexes = BTreeSet::new();
                for IndexSwap { indexes: (lhs, rhs) } in swaps {
                    for index in [lhs, rhs] {
                        let index_exists = self.index_mapper.index_exists(&wtxn, index)?;
                        if !index_exists {
                            not_found_indexes.insert(index);
                        }
                    }
                }
                if !not_found_indexes.is_empty() {
                    if not_found_indexes.len() == 1 {
                        return Err(Error::SwapIndexNotFound(
                            not_found_indexes.into_iter().next().unwrap().clone(),
                        ));
                    } else {
                        return Err(Error::SwapIndexesNotFound(
                            not_found_indexes.into_iter().cloned().collect(),
                        ));
                    }
                }
                progress.update_progress(SwappingTheIndexes::SwappingTheIndexes);
                for (step, swap) in swaps.iter().enumerate() {
                    progress.update_progress(VariableNameStep::<SwappingTheIndexes>::new(
                        format!("swapping index {} and {}", swap.indexes.0, swap.indexes.1),
                        step as u32,
                        swaps.len() as u32,
                    ));
                    self.apply_index_swap(
                        &mut wtxn,
                        &progress,
                        task.uid,
                        &swap.indexes.0,
                        &swap.indexes.1,
                    )?;
                }
                wtxn.commit()?;
                task.status = Status::Succeeded;
                Ok((vec![task], ProcessBatchInfo::default()))
            }
            Batch::Export { mut task } => {
                let KindWithContent::Export { url, api_key, payload_size, indexes } = &task.kind
                else {
                    unreachable!()
                };

                let ret = catch_unwind(AssertUnwindSafe(|| {
                    self.process_export(
                        url,
                        api_key.as_deref(),
                        payload_size.as_ref(),
                        indexes,
                        progress,
                    )
                }));

                let stats = match ret {
                    Ok(Ok(stats)) => stats,
                    Ok(Err(Error::AbortedTask)) => return Err(Error::AbortedTask),
                    Ok(Err(e)) => return Err(Error::Export(Box::new(e))),
                    Err(e) => {
                        let msg = match e.downcast_ref::<&'static str>() {
                            Some(s) => *s,
                            None => match e.downcast_ref::<String>() {
                                Some(s) => &s[..],
                                None => "Box<dyn Any>",
                            },
                        };
                        return Err(Error::Export(Box::new(Error::ProcessBatchPanicked(
                            msg.to_string(),
                        ))));
                    }
                };

                task.status = Status::Succeeded;
                if let Some(Details::Export { indexes, .. }) = task.details.as_mut() {
                    *indexes = stats;
                }
                Ok((vec![task], ProcessBatchInfo::default()))
            }
            Batch::UpgradeDatabase { mut tasks } => {
                let KindWithContent::UpgradeDatabase { from } = tasks.last().unwrap().kind else {
                    unreachable!();
                };

                let ret = catch_unwind(AssertUnwindSafe(|| self.process_upgrade(from, progress)));
                match ret {
                    Ok(Ok(())) => (),
                    Ok(Err(Error::AbortedTask)) => return Err(Error::AbortedTask),
                    Ok(Err(e)) => return Err(Error::DatabaseUpgrade(Box::new(e))),
                    Err(e) => {
                        let msg = match e.downcast_ref::<&'static str>() {
                            Some(s) => *s,
                            None => match e.downcast_ref::<String>() {
                                Some(s) => &s[..],
                                None => "Box<dyn Any>",
                            },
                        };
                        return Err(Error::DatabaseUpgrade(Box::new(Error::ProcessBatchPanicked(
                            msg.to_string(),
                        ))));
                    }
                }

                for task in tasks.iter_mut() {
                    task.status = Status::Succeeded;
                    // Since this task can be retried we must reset its error status
                    task.error = None;
                }

                Ok((tasks, ProcessBatchInfo::default()))
            }
        }
    }

    /// Swap the index `lhs` with the index `rhs`.
    fn apply_index_swap(
        &self,
        wtxn: &mut RwTxn,
        progress: &Progress,
        task_id: u32,
        lhs: &str,
        rhs: &str,
    ) -> Result<()> {
        progress.update_progress(InnerSwappingTwoIndexes::RetrieveTheTasks);
        // 1. Verify that both lhs and rhs are existing indexes
        let index_lhs_exists = self.index_mapper.index_exists(wtxn, lhs)?;
        if !index_lhs_exists {
            return Err(Error::IndexNotFound(lhs.to_owned()));
        }
        let index_rhs_exists = self.index_mapper.index_exists(wtxn, rhs)?;
        if !index_rhs_exists {
            return Err(Error::IndexNotFound(rhs.to_owned()));
        }

        // 2. Get the task set for index = name that appeared before the index swap task
        let mut index_lhs_task_ids = self.queue.tasks.index_tasks(wtxn, lhs)?;
        index_lhs_task_ids.remove_range(task_id..);
        let mut index_rhs_task_ids = self.queue.tasks.index_tasks(wtxn, rhs)?;
        index_rhs_task_ids.remove_range(task_id..);

        // 3. before_name -> new_name in the task's KindWithContent
        progress.update_progress(InnerSwappingTwoIndexes::UpdateTheTasks);
        let tasks_to_update = &index_lhs_task_ids | &index_rhs_task_ids;
        let (atomic, task_progress) = AtomicTaskStep::new(tasks_to_update.len() as u32);
        progress.update_progress(task_progress);

        for task_id in tasks_to_update {
            let mut task =
                self.queue.tasks.get_task(wtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;
            swap_index_uid_in_task(&mut task, (lhs, rhs));
            self.queue.tasks.all_tasks.put(wtxn, &task_id, &task)?;
            atomic.fetch_add(1, Ordering::Relaxed);
        }

        // 4. remove the task from indexuid = before_name
        // 5. add the task to indexuid = after_name
        progress.update_progress(InnerSwappingTwoIndexes::UpdateTheIndexesMetadata);
        self.queue.tasks.update_index(wtxn, lhs, |lhs_tasks| {
            *lhs_tasks -= &index_lhs_task_ids;
            *lhs_tasks |= &index_rhs_task_ids;
        })?;
        self.queue.tasks.update_index(wtxn, rhs, |rhs_tasks| {
            *rhs_tasks -= &index_rhs_task_ids;
            *rhs_tasks |= &index_lhs_task_ids;
        })?;

        // 6. Swap in the index mapper
        self.index_mapper.swap(wtxn, lhs, rhs)?;

        Ok(())
    }

    /// Delete each given task from all the databases (if it is deleteable).
    ///
    /// Return the number of tasks that were actually deleted.
    #[allow(clippy::reversed_empty_ranges)]
    fn delete_matched_tasks(
        &self,
        matched_tasks: &RoaringBitmap,
        progress: &Progress,
    ) -> Result<RoaringBitmap> {
        fn remove_task_datetimes(
            wtxn: &mut RwTxn<'_>,
            mut to_remove: HashMap<i128, RoaringBitmap>,
            db: Database<BEI128, CboRoaringBitmapCodec>,
        ) -> Result<()> {
            if to_remove.is_empty() {
                return Ok(());
            }

            let min = to_remove.keys().min().cloned().unwrap(); // to_remove isn't empty so this is ok
            let max = to_remove.keys().max().cloned().unwrap();
            let range = min..=max;

            // We iterate over the time database to see which ranges of timestamps need to be removed
            let lazy_db = db.lazily_decode_data();
            let iter = lazy_db.range(wtxn, &range)?;
            let mut delete_range_start = None;
            let mut delete_ranges = Vec::new();
            let mut to_put: HashMap<i128, RoaringBitmap> = HashMap::new();
            for i in iter {
                let (timestamp, data) = i?;

                if let Some(to_remove) = to_remove.remove(&timestamp) {
                    let mut current =
                        data.decode().map_err(|e| Error::Anyhow(anyhow::anyhow!(e)))?;
                    current -= &to_remove;

                    if current.is_empty() {
                        if delete_range_start.is_none() {
                            delete_range_start = Some(timestamp);
                        }
                    } else {
                        // We could close the deletion range but it's not necessary because the new value will get reinserted anyway
                        to_put.insert(timestamp, current);
                    }
                } else if let Some(delete_range_start) = delete_range_start.take() {
                    // Current one must not be deleted so we need to skip it
                    delete_ranges.push(delete_range_start..timestamp);
                }
            }
            if let Some(delete_range_start) = delete_range_start.take() {
                delete_ranges.push(delete_range_start..(max + 1));
            }

            for range in delete_ranges {
                db.delete_range(wtxn, &range)?;
            }

            for (timestamp, data) in to_put {
                db.put(wtxn, &timestamp, &data)?;
            }

            Ok(())
        }

        fn remove_batch_datetimes(
            wtxn: &mut RwTxn<'_>,
            to_remove: &RoaringBitmap,
            db: Database<BEI128, CboRoaringBitmapCodec>,
        ) -> Result<()> {
            if to_remove.is_empty() {
                return Ok(());
            }

            // We iterate over the time database to see which ranges of timestamps need to be removed
            let iter = db.iter(wtxn)?;
            let mut delete_range_start = None;
            let mut delete_ranges = Vec::new();
            let mut to_put: HashMap<i128, RoaringBitmap> = HashMap::new();
            for i in iter {
                let (timestamp, mut current) = i?;

                if !current.is_disjoint(to_remove) {
                    current -= to_remove;

                    if current.is_empty() {
                        if delete_range_start.is_none() {
                            delete_range_start = Some(timestamp);
                        }
                    } else {
                        // We could close the deletion range but it's not necessary because the new value will get reinserted anyway
                        to_put.insert(timestamp, current);
                    }
                } else if let Some(delete_range_start) = delete_range_start.take() {
                    // Current one must not be deleted so we need to skip it
                    delete_ranges.push(delete_range_start..timestamp);
                }
            }
            if let Some(delete_range_start) = delete_range_start.take() {
                delete_ranges.push(delete_range_start..i128::MAX);
            }

            for range in delete_ranges {
                db.delete_range(wtxn, &range)?;
            }

            for (timestamp, data) in to_put {
                db.put(wtxn, &timestamp, &data)?;
            }

            Ok(())
        }

        progress.update_progress(TaskDeletionProgress::RetrievingTasks);

        let rtxn = self.env.read_txn()?;

        // 1. Remove from this list the tasks that we are not allowed to delete
        let processing_tasks = &self.processing_tasks.read().unwrap().processing.clone();
        let mut status_tasks = HashMap::new();
        for status in enum_iterator::all::<Status>() {
            status_tasks.insert(status, self.queue.tasks.get_status(&rtxn, status)?);
        }
        let enqueued_tasks = status_tasks.get(&Status::Enqueued).unwrap(); // We added all statuses
        let all_task_ids = status_tasks.values().union();
        let mut to_remove_from_statuses = HashMap::new();
        let mut to_delete_tasks = all_task_ids.clone() & matched_tasks;
        to_delete_tasks -= &**processing_tasks;
        to_delete_tasks -= enqueued_tasks;

        // 2. We now have a list of tasks to delete. Read their metadata to list what needs to be updated.
        let mut affected_indexes = HashSet::new();
        let mut affected_statuses = HashSet::new();
        let mut affected_kinds = HashSet::new();
        let mut affected_canceled_by = RoaringBitmap::new();
        let mut affected_batches: HashMap<BatchId, RoaringBitmap> = HashMap::new(); // The tasks that have been removed *per batches*.
        let mut tasks_enqueued_to_remove: HashMap<i128, RoaringBitmap> = HashMap::new();
        let mut tasks_started_to_remove: HashMap<i128, RoaringBitmap> = HashMap::new();
        let mut tasks_finished_to_remove: HashMap<i128, RoaringBitmap> = HashMap::new();
        let (atomic_progress, task_progress) = AtomicTaskStep::new(to_delete_tasks.len() as u32);
        progress.update_progress(task_progress);
        for range in consecutive_ranges(to_delete_tasks.iter()) {
            let iter = self.queue.tasks.all_tasks.range(&rtxn, &range)?;
            for task in iter {
                let (task_id, task) = task?;

                affected_indexes.extend(task.indexes().into_iter().map(|x| x.to_owned()));
                affected_statuses.insert(task.status);
                affected_kinds.insert(task.kind.as_kind());

                let enqueued_at = task.enqueued_at.unix_timestamp_nanos();
                tasks_enqueued_to_remove.entry(enqueued_at).or_default().insert(task_id);

                if let Some(started_at) = task.started_at {
                    tasks_started_to_remove
                        .entry(started_at.unix_timestamp_nanos())
                        .or_default()
                        .insert(task_id);
                }

                if let Some(finished_at) = task.finished_at {
                    tasks_finished_to_remove
                        .entry(finished_at.unix_timestamp_nanos())
                        .or_default()
                        .insert(task_id);
                }

                if let Some(canceled_by) = task.canceled_by {
                    affected_canceled_by.insert(canceled_by);
                }
                if let Some(batch_uid) = task.batch_uid {
                    affected_batches.entry(batch_uid).or_default().insert(task_id);
                }
                atomic_progress.fetch_add(1, Ordering::Relaxed);
            }
        }

        // 3. Read the tasks by indexes, statuses and kinds
        let mut affected_indexes_tasks = HashMap::new();
        for index in &affected_indexes {
            affected_indexes_tasks
                .insert(index.as_str(), self.queue.tasks.index_tasks(&rtxn, index)?);
        }
        let mut to_remove_from_indexes = HashMap::new();

        let mut affected_kinds_tasks = HashMap::new();
        for kind in &affected_kinds {
            affected_kinds_tasks.insert(*kind, self.queue.tasks.get_kind(&rtxn, *kind)?);
        }
        let mut to_remove_from_kinds = HashMap::new();

        // 4. Read affected batches' tasks
        let mut to_delete_batches = RoaringBitmap::new();
        let affected_batches_bitmap = RoaringBitmap::from_iter(affected_batches.keys());
        progress.update_progress(TaskDeletionProgress::RetrievingBatchTasks);
        let (atomic_progress, task_progress) =
            AtomicBatchStep::new(affected_batches_bitmap.len() as u32);
        progress.update_progress(task_progress);
        for range in consecutive_ranges(affected_batches_bitmap.iter()) {
            let iter = self.queue.batch_to_tasks_mapping.range(&rtxn, &range)?;
            for batch in iter {
                let (batch_id, mut tasks) = batch?;
                let to_delete_tasks = affected_batches.remove(&batch_id).unwrap_or_default();
                tasks -= &to_delete_tasks;

                // Note: we never delete tasks from the mapping. It's error-prone but intentional (perf)
                // We make sure to filter the tasks from the mapping when we read them.
                tasks &= &all_task_ids;

                // We must remove the batch entirely
                if tasks.is_empty() {
                    to_delete_batches.insert(batch_id);
                }

                // We must remove the batch from all the reverse indexes it no longer has tasks for.

                for (index, index_tasks) in affected_indexes_tasks.iter() {
                    if index_tasks.is_disjoint(&tasks) {
                        to_remove_from_indexes
                            .entry(index)
                            .or_insert_with(RoaringBitmap::new)
                            .insert(batch_id);
                    }
                }

                for (status, status_tasks) in status_tasks.iter() {
                    if status_tasks.is_disjoint(&tasks) {
                        to_remove_from_statuses
                            .entry(*status)
                            .or_insert_with(RoaringBitmap::new)
                            .insert(batch_id);
                    }
                }

                for (kind, kind_tasks) in affected_kinds_tasks.iter() {
                    if kind_tasks.is_disjoint(&tasks) {
                        to_remove_from_kinds
                            .entry(*kind)
                            .or_insert_with(RoaringBitmap::new)
                            .insert(batch_id);
                    }
                }

                // Note: no need to delete the persisted task data since
                // we can only delete succeeded, failed, and canceled tasks.
                // In each of those cases, the persisted data is supposed to
                // have been deleted already.

                atomic_progress.fetch_add(1, Ordering::Relaxed);
            }
        }

        drop(rtxn);
        let mut owned_wtxn = self.env.write_txn()?;
        let wtxn = &mut owned_wtxn;

        // 7. Remove task datetimes
        progress.update_progress(TaskDeletionProgress::DeletingTasksDateTime);
        remove_task_datetimes(wtxn, tasks_enqueued_to_remove, self.queue.tasks.enqueued_at)?;
        remove_task_datetimes(wtxn, tasks_started_to_remove, self.queue.tasks.started_at)?;
        remove_task_datetimes(wtxn, tasks_finished_to_remove, self.queue.tasks.finished_at)?;

        // 8. Delete batches datetimes
        progress.update_progress(TaskDeletionProgress::DeletingBatchesDateTime);
        remove_batch_datetimes(wtxn, &to_delete_batches, self.queue.batches.enqueued_at)?;
        remove_batch_datetimes(wtxn, &to_delete_batches, self.queue.batches.started_at)?;
        remove_batch_datetimes(wtxn, &to_delete_batches, self.queue.batches.finished_at)?;

        // 9. Remove batches metadata from indexes, statuses, and kinds
        progress.update_progress(TaskDeletionProgress::DeletingBatchesMetadata);

        for (index, batches) in to_remove_from_indexes {
            self.queue.batches.update_index(wtxn, index, |b| {
                *b -= &batches;
            })?;
        }

        for (status, batches) in to_remove_from_statuses {
            self.queue.batches.update_status(wtxn, status, |b| {
                *b -= &batches;
            })?;
        }

        for (kind, batches) in to_remove_from_kinds {
            self.queue.batches.update_kind(wtxn, kind, |b| {
                *b -= &batches;
            })?;
        }

        // 10. Remove tasks from indexes, statuses, and kinds
        progress.update_progress(TaskDeletionProgress::DeletingTasksMetadata);
        let (atomic_progress, task_progress) = AtomicTaskStep::new(
            (affected_indexes.len() + affected_statuses.len() + affected_kinds.len()) as u32,
        );
        progress.update_progress(task_progress);

        for (index, mut tasks) in affected_indexes_tasks.into_iter() {
            tasks -= &to_delete_tasks;
            if tasks.is_empty() {
                self.queue.tasks.index_tasks.delete(wtxn, index)?;
            } else {
                self.queue.tasks.index_tasks.put(wtxn, index, &tasks)?;
            }
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }

        for status in affected_statuses.into_iter() {
            let mut tasks = status_tasks.remove(&status).unwrap(); // we inserted all statuses above
            tasks -= &to_delete_tasks;
            if tasks.is_empty() {
                self.queue.tasks.status.delete(wtxn, &status)?;
            } else {
                self.queue.tasks.status.put(wtxn, &status, &tasks)?;
            }
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }

        for (kind, mut tasks) in affected_kinds_tasks.into_iter() {
            tasks -= &to_delete_tasks;
            if tasks.is_empty() {
                self.queue.tasks.kind.delete(wtxn, &kind)?;
            } else {
                self.queue.tasks.kind.put(wtxn, &kind, &tasks)?;
            }
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }

        // 11. Delete tasks
        progress.update_progress(TaskDeletionProgress::DeletingTasks);
        let (atomic_progress, task_progress) =
            AtomicTaskStep::new((to_delete_tasks.len() + affected_canceled_by.len()) as u32);
        progress.update_progress(task_progress);
        for range in consecutive_ranges(to_delete_tasks.iter()) {
            self.queue.tasks.all_tasks.delete_range(wtxn, &range)?;
            atomic_progress.fetch_add(range.size_hint().0 as u32, Ordering::Relaxed);
        }

        for canceled_by in affected_canceled_by {
            if let Some(mut tasks) = self.queue.tasks.canceled_by.get(wtxn, &canceled_by)? {
                tasks -= &to_delete_tasks;
                if tasks.is_empty() {
                    self.queue.tasks.canceled_by.delete(wtxn, &canceled_by)?;
                } else {
                    self.queue.tasks.canceled_by.put(wtxn, &canceled_by, &tasks)?;
                }
            }
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }

        // 12. Delete batches
        progress.update_progress(TaskDeletionProgress::DeletingBatches);
        let (atomic_progress, task_progress) = AtomicTaskStep::new(to_delete_batches.len() as u32);
        progress.update_progress(task_progress);
        for range in consecutive_ranges(to_delete_batches.iter()) {
            self.queue.batches.all_batches.delete_range(wtxn, &range)?;
            self.queue.batch_to_tasks_mapping.delete_range(wtxn, &range)?;
            atomic_progress.fetch_add(range.size_hint().0 as u32, Ordering::Relaxed);
        }

        owned_wtxn.commit()?;

        Ok(to_delete_tasks)
    }

    /// Cancel each given task from all the databases (if it is cancelable).
    ///
    /// Returns the list of tasks that matched the filter and must be written in the database.
    fn cancel_matched_tasks(
        &self,
        rtxn: &RoTxn,
        cancel_task_id: TaskId,
        current_batch: &mut ProcessingBatch,
        matched_tasks: &RoaringBitmap,
        progress: &Progress,
    ) -> Result<Vec<Task>> {
        progress.update_progress(TaskCancelationProgress::RetrievingTasks);
        let mut tasks_to_cancel = RoaringBitmap::new();

        let enqueued_tasks = &self.queue.tasks.get_status(rtxn, Status::Enqueued)?;

        // 0. Check if any upgrade task was matched.
        //    If so, we cancel all the failed or enqueued upgrade tasks.
        let upgrade_tasks = &self.queue.tasks.get_kind(rtxn, Kind::UpgradeDatabase)?;
        let is_canceling_upgrade = !matched_tasks.is_disjoint(upgrade_tasks);
        if is_canceling_upgrade {
            let failed_tasks = self.queue.tasks.get_status(rtxn, Status::Failed)?;
            tasks_to_cancel |= upgrade_tasks & (enqueued_tasks | failed_tasks);
        }
        // 1. Remove from this list the tasks that we are not allowed to cancel
        //    Notice that only the _enqueued_ ones are cancelable and we should
        //    have already aborted the indexation of the _processing_ ones
        tasks_to_cancel |= enqueued_tasks & matched_tasks;

        // 2. If we're canceling an upgrade, attempt the rollback
        if let Some(latest_upgrade_task) = (&tasks_to_cancel & upgrade_tasks).max() {
            progress.update_progress(TaskCancelationProgress::CancelingUpgrade);

            let task = self.queue.tasks.get_task(rtxn, latest_upgrade_task)?.unwrap();
            let Some(Details::UpgradeDatabase { from, to }) = task.details else {
                unreachable!("wrong details for upgrade task {latest_upgrade_task}")
            };

            // check that we are rollbacking an upgrade to the current Meilisearch
            let bin_major: u32 = meilisearch_types::versioning::VERSION_MAJOR;
            let bin_minor: u32 = meilisearch_types::versioning::VERSION_MINOR;
            let bin_patch: u32 = meilisearch_types::versioning::VERSION_PATCH;

            if to == (bin_major, bin_minor, bin_patch) {
                tracing::warn!(
                    "Rollbacking from v{}.{}.{} to v{}.{}.{}",
                    to.0,
                    to.1,
                    to.2,
                    from.0,
                    from.1,
                    from.2
                );
                let ret = catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.process_rollback(from, progress)
                }));

                match ret {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => return Err(Error::DatabaseUpgrade(Box::new(err))),
                    Err(e) => {
                        let msg = match e.downcast_ref::<&'static str>() {
                            Some(s) => *s,
                            None => match e.downcast_ref::<String>() {
                                Some(s) => &s[..],
                                None => "Box<dyn Any>",
                            },
                        };
                        return Err(Error::DatabaseUpgrade(Box::new(Error::ProcessBatchPanicked(
                            msg.to_string(),
                        ))));
                    }
                }
            } else {
                tracing::debug!(
                    "Not rollbacking an upgrade targetting the earlier version v{}.{}.{}",
                    bin_major,
                    bin_minor,
                    bin_patch
                )
            }
        }

        // 3. We now have a list of tasks to cancel, cancel them
        let (task_progress, progress_obj) = AtomicTaskStep::new(tasks_to_cancel.len() as u32);
        progress.update_progress(progress_obj);

        let mut tasks = self.queue.tasks.get_existing_tasks(
            rtxn,
            tasks_to_cancel.iter().inspect(|_| {
                task_progress.fetch_add(1, Ordering::Relaxed);
            }),
        )?;

        progress.update_progress(TaskCancelationProgress::UpdatingTasks);
        let (task_progress, progress_obj) = AtomicTaskStep::new(tasks_to_cancel.len() as u32);
        progress.update_progress(progress_obj);
        for task in tasks.iter_mut() {
            task.status = Status::Canceled;
            task.canceled_by = Some(cancel_task_id);
            task.details = task.details.as_ref().map(|d| d.to_failed());
            current_batch.processing(Some(task));
            task_progress.fetch_add(1, Ordering::Relaxed);
        }

        Ok(tasks)
    }
}

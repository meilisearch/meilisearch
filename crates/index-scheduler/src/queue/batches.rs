use std::collections::HashSet;
use std::ops::{Bound, RangeBounds};

use meilisearch_types::batches::{Batch, BatchId};
use meilisearch_types::heed::types::{DecodeIgnore, SerdeBincode, SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RoTxn, RwTxn};
use meilisearch_types::milli::{CboRoaringBitmapCodec, RoaringBitmapCodec, BEU32};
use meilisearch_types::tasks::{Kind, Status};
use roaring::{MultiOps, RoaringBitmap};
use time::OffsetDateTime;

use super::{Query, Queue};
use crate::processing::ProcessingTasks;
use crate::utils::{
    insert_task_datetime, keep_ids_within_datetimes, map_bound,
    remove_n_tasks_datetime_earlier_than, remove_task_datetime, ProcessingBatch,
};
use crate::{Error, Result, BEI128};

/// The number of database used by the batch queue
const NUMBER_OF_DATABASES: u32 = 7;
/// Database const names for the `IndexScheduler`.
mod db_name {
    pub const ALL_BATCHES: &str = "all-batches";

    pub const BATCH_STATUS: &str = "batch-status";
    pub const BATCH_KIND: &str = "batch-kind";
    pub const BATCH_INDEX_TASKS: &str = "batch-index-tasks";
    pub const BATCH_ENQUEUED_AT: &str = "batch-enqueued-at";
    pub const BATCH_STARTED_AT: &str = "batch-started-at";
    pub const BATCH_FINISHED_AT: &str = "batch-finished-at";
}

pub struct BatchQueue {
    /// Contains all the batches accessible by their Id.
    pub(crate) all_batches: Database<BEU32, SerdeJson<Batch>>,

    /// All the batches containing a task matching the selected status.
    pub(crate) status: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
    /// All the batches ids grouped by the kind of their task.
    pub(crate) kind: Database<SerdeBincode<Kind>, RoaringBitmapCodec>,
    /// Store the batches associated to an index.
    pub(crate) index_tasks: Database<Str, RoaringBitmapCodec>,
    /// Store the batches containing tasks which were enqueued at a specific date
    pub(crate) enqueued_at: Database<BEI128, CboRoaringBitmapCodec>,
    /// Store the batches containing finished tasks started at a specific date
    pub(crate) started_at: Database<BEI128, CboRoaringBitmapCodec>,
    /// Store the batches containing tasks finished at a specific date
    pub(crate) finished_at: Database<BEI128, CboRoaringBitmapCodec>,
}

impl BatchQueue {
    pub(crate) fn private_clone(&self) -> BatchQueue {
        BatchQueue {
            all_batches: self.all_batches,
            status: self.status,
            kind: self.kind,
            index_tasks: self.index_tasks,
            enqueued_at: self.enqueued_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        }
    }

    pub(crate) const fn nb_db() -> u32 {
        NUMBER_OF_DATABASES
    }

    pub(super) fn new(env: &Env, wtxn: &mut RwTxn) -> Result<Self> {
        Ok(Self {
            all_batches: env.create_database(wtxn, Some(db_name::ALL_BATCHES))?,
            status: env.create_database(wtxn, Some(db_name::BATCH_STATUS))?,
            kind: env.create_database(wtxn, Some(db_name::BATCH_KIND))?,
            index_tasks: env.create_database(wtxn, Some(db_name::BATCH_INDEX_TASKS))?,
            enqueued_at: env.create_database(wtxn, Some(db_name::BATCH_ENQUEUED_AT))?,
            started_at: env.create_database(wtxn, Some(db_name::BATCH_STARTED_AT))?,
            finished_at: env.create_database(wtxn, Some(db_name::BATCH_FINISHED_AT))?,
        })
    }

    pub(crate) fn all_batch_ids(&self, rtxn: &RoTxn) -> Result<RoaringBitmap> {
        enum_iterator::all().map(|s| self.get_status(rtxn, s)).union()
    }

    pub(crate) fn next_batch_id(&self, rtxn: &RoTxn) -> Result<BatchId> {
        Ok(self
            .all_batches
            .remap_data_type::<DecodeIgnore>()
            .last(rtxn)?
            .map(|(k, _)| k + 1)
            .unwrap_or_default())
    }

    pub(crate) fn get_batch(&self, rtxn: &RoTxn, batch_id: BatchId) -> Result<Option<Batch>> {
        Ok(self.all_batches.get(rtxn, &batch_id)?)
    }

    /// Returns the whole set of batches that belongs to this index.
    pub(crate) fn index_batches(&self, rtxn: &RoTxn, index: &str) -> Result<RoaringBitmap> {
        Ok(self.index_tasks.get(rtxn, index)?.unwrap_or_default())
    }

    pub(crate) fn update_index(
        &self,
        wtxn: &mut RwTxn,
        index: &str,
        f: impl Fn(&mut RoaringBitmap),
    ) -> Result<()> {
        let mut batches = self.index_batches(wtxn, index)?;
        f(&mut batches);
        if batches.is_empty() {
            self.index_tasks.delete(wtxn, index)?;
        } else {
            self.index_tasks.put(wtxn, index, &batches)?;
        }

        Ok(())
    }

    pub(crate) fn get_status(&self, rtxn: &RoTxn, status: Status) -> Result<RoaringBitmap> {
        Ok(self.status.get(rtxn, &status)?.unwrap_or_default())
    }

    pub(crate) fn put_status(
        &self,
        wtxn: &mut RwTxn,
        status: Status,
        bitmap: &RoaringBitmap,
    ) -> Result<()> {
        Ok(self.status.put(wtxn, &status, bitmap)?)
    }

    pub(crate) fn update_status(
        &self,
        wtxn: &mut RwTxn,
        status: Status,
        f: impl Fn(&mut RoaringBitmap),
    ) -> Result<()> {
        let mut tasks = self.get_status(wtxn, status)?;
        f(&mut tasks);
        self.put_status(wtxn, status, &tasks)?;

        Ok(())
    }

    pub(crate) fn get_kind(&self, rtxn: &RoTxn, kind: Kind) -> Result<RoaringBitmap> {
        Ok(self.kind.get(rtxn, &kind)?.unwrap_or_default())
    }

    pub(crate) fn put_kind(
        &self,
        wtxn: &mut RwTxn,
        kind: Kind,
        bitmap: &RoaringBitmap,
    ) -> Result<()> {
        Ok(self.kind.put(wtxn, &kind, bitmap)?)
    }

    pub(crate) fn update_kind(
        &self,
        wtxn: &mut RwTxn,
        kind: Kind,
        f: impl Fn(&mut RoaringBitmap),
    ) -> Result<()> {
        let mut tasks = self.get_kind(wtxn, kind)?;
        f(&mut tasks);
        self.put_kind(wtxn, kind, &tasks)?;
        Ok(())
    }

    pub(crate) fn write_batch(&self, wtxn: &mut RwTxn, batch: ProcessingBatch) -> Result<()> {
        let old_batch = self.all_batches.get(wtxn, &batch.uid)?;

        self.all_batches.put(
            wtxn,
            &batch.uid,
            &Batch {
                uid: batch.uid,
                progress: None,
                details: batch.details,
                stats: batch.stats,
                started_at: batch.started_at,
                finished_at: batch.finished_at,
                enqueued_at: batch.enqueued_at,
            },
        )?;

        // Update the statuses
        if let Some(ref old_batch) = old_batch {
            for status in old_batch.stats.status.keys() {
                self.update_status(wtxn, *status, |bitmap| {
                    bitmap.remove(batch.uid);
                })?;
            }
        }
        for status in batch.statuses {
            self.update_status(wtxn, status, |bitmap| {
                bitmap.insert(batch.uid);
            })?;
        }

        // Update the kinds / types
        if let Some(ref old_batch) = old_batch {
            let kinds: HashSet<_> = old_batch.stats.types.keys().cloned().collect();
            for kind in kinds.difference(&batch.kinds) {
                self.update_kind(wtxn, *kind, |bitmap| {
                    bitmap.remove(batch.uid);
                })?;
            }
        }
        for kind in batch.kinds {
            self.update_kind(wtxn, kind, |bitmap| {
                bitmap.insert(batch.uid);
            })?;
        }

        // Update the indexes
        if let Some(ref old_batch) = old_batch {
            let indexes: HashSet<_> = old_batch.stats.index_uids.keys().cloned().collect();
            for index in indexes.difference(&batch.indexes) {
                self.update_index(wtxn, index, |bitmap| {
                    bitmap.remove(batch.uid);
                })?;
            }
        }
        for index in batch.indexes {
            self.update_index(wtxn, &index, |bitmap| {
                bitmap.insert(batch.uid);
            })?;
        }

        // Update the enqueued_at: we cannot retrieve the previous enqueued at from the previous batch, and
        // must instead go through the db looking for it. We cannot look at the task contained in this batch either
        // because they may have been removed.
        // What we know, though, is that the task date is from before the enqueued_at, and max two timestamps have been written
        // to the DB per batches.
        if let Some(ref old_batch) = old_batch {
            if let Some(enqueued_at) = old_batch.enqueued_at {
                remove_task_datetime(wtxn, self.enqueued_at, enqueued_at.earliest, old_batch.uid)?;
                remove_task_datetime(wtxn, self.enqueued_at, enqueued_at.oldest, old_batch.uid)?;
            } else {
                // If we don't have the enqueued at in the batch it means the database comes from the v1.12
                // and we still need to find the date by scrolling the database
                remove_n_tasks_datetime_earlier_than(
                    wtxn,
                    self.enqueued_at,
                    old_batch.started_at,
                    old_batch.stats.total_nb_tasks.clamp(1, 2) as usize,
                    old_batch.uid,
                )?;
            }
        }
        // A finished batch MUST contains at least one task and have an enqueued_at
        let enqueued_at = batch.enqueued_at.as_ref().unwrap();
        insert_task_datetime(wtxn, self.enqueued_at, enqueued_at.earliest, batch.uid)?;
        insert_task_datetime(wtxn, self.enqueued_at, enqueued_at.oldest, batch.uid)?;

        // Update the started at and finished at
        if let Some(ref old_batch) = old_batch {
            remove_task_datetime(wtxn, self.started_at, old_batch.started_at, old_batch.uid)?;
            if let Some(finished_at) = old_batch.finished_at {
                remove_task_datetime(wtxn, self.finished_at, finished_at, old_batch.uid)?;
            }
        }
        insert_task_datetime(wtxn, self.started_at, batch.started_at, batch.uid)?;
        insert_task_datetime(wtxn, self.finished_at, batch.finished_at.unwrap(), batch.uid)?;

        Ok(())
    }

    /// Convert an iterator to a `Vec` of batches. The batches MUST exist or a
    /// `CorruptedTaskQueue` error will be thrown.
    pub(crate) fn get_existing_batches(
        &self,
        rtxn: &RoTxn,
        tasks: impl IntoIterator<Item = BatchId>,
        processing: &ProcessingTasks,
    ) -> Result<Vec<Batch>> {
        tasks
            .into_iter()
            .map(|batch_id| {
                if Some(batch_id) == processing.batch.as_ref().map(|batch| batch.uid) {
                    let mut batch = processing.batch.as_ref().unwrap().to_batch();
                    batch.progress = processing.get_progress_view();
                    Ok(batch)
                } else {
                    self.get_batch(rtxn, batch_id)
                        .and_then(|task| task.ok_or(Error::CorruptedTaskQueue))
                }
            })
            .collect::<Result<_>>()
    }
}

impl Queue {
    /// Return the batch ids matched by the given query from the index scheduler's point of view.
    pub(crate) fn get_batch_ids(
        &self,
        rtxn: &RoTxn,
        query: &Query,
        processing: &ProcessingTasks,
    ) -> Result<RoaringBitmap> {
        let Query {
            limit,
            from,
            reverse,
            uids,
            batch_uids,
            statuses,
            types,
            index_uids,
            canceled_by,
            before_enqueued_at,
            after_enqueued_at,
            before_started_at,
            after_started_at,
            before_finished_at,
            after_finished_at,
        } = query;

        let mut batches = self.batches.all_batch_ids(rtxn)?;
        if let Some(batch_id) = processing.batch.as_ref().map(|batch| batch.uid) {
            batches.insert(batch_id);
        }

        if let Some(from) = from {
            let range = if reverse.unwrap_or_default() {
                u32::MIN..*from
            } else {
                from.saturating_add(1)..u32::MAX
            };
            batches.remove_range(range);
        }

        if let Some(batch_uids) = &batch_uids {
            let batches_uids = RoaringBitmap::from_iter(batch_uids);
            batches &= batches_uids;
        }

        if let Some(status) = &statuses {
            let mut status_batches = RoaringBitmap::new();
            for status in status {
                match status {
                    // special case for Processing batches
                    Status::Processing => {
                        if let Some(batch_id) = processing.batch.as_ref().map(|batch| batch.uid) {
                            status_batches.insert(batch_id);
                        }
                    }
                    // Enqueued tasks are not stored in batches
                    Status::Enqueued => (),
                    status => status_batches |= &self.batches.get_status(rtxn, *status)?,
                };
            }
            if !status.contains(&Status::Processing) {
                if let Some(ref batch) = processing.batch {
                    batches.remove(batch.uid);
                }
            }
            batches &= status_batches;
        }

        if let Some(task_uids) = &uids {
            let mut batches_by_task_uids = RoaringBitmap::new();
            for task_uid in task_uids {
                if let Some(task) = self.tasks.get_task(rtxn, *task_uid)? {
                    if let Some(batch_uid) = task.batch_uid {
                        batches_by_task_uids.insert(batch_uid);
                    }
                }
            }
            batches &= batches_by_task_uids;
        }

        // There is no database for this query, we must retrieve the task queried by the client and ensure it's valid
        if let Some(canceled_by) = &canceled_by {
            let mut all_canceled_batches = RoaringBitmap::new();
            for cancel_uid in canceled_by {
                if let Some(task) = self.tasks.get_task(rtxn, *cancel_uid)? {
                    if task.kind.as_kind() == Kind::TaskCancelation
                        && task.status == Status::Succeeded
                    {
                        if let Some(batch_uid) = task.batch_uid {
                            all_canceled_batches.insert(batch_uid);
                        }
                    }
                }
            }

            // if the canceled_by has been specified but no batch
            // matches then we prefer matching zero than all batches.
            if all_canceled_batches.is_empty() {
                return Ok(RoaringBitmap::new());
            } else {
                batches &= all_canceled_batches;
            }
        }

        if let Some(kind) = &types {
            let mut kind_batches = RoaringBitmap::new();
            for kind in kind {
                kind_batches |= self.batches.get_kind(rtxn, *kind)?;
                if let Some(uid) = processing
                    .batch
                    .as_ref()
                    .and_then(|batch| batch.kinds.contains(kind).then_some(batch.uid))
                {
                    kind_batches.insert(uid);
                }
            }
            batches &= &kind_batches;
        }

        if let Some(index) = &index_uids {
            let mut index_batches = RoaringBitmap::new();
            for index in index {
                index_batches |= self.batches.index_batches(rtxn, index)?;
                if let Some(uid) = processing
                    .batch
                    .as_ref()
                    .and_then(|batch| batch.indexes.contains(index).then_some(batch.uid))
                {
                    index_batches.insert(uid);
                }
            }
            batches &= &index_batches;
        }

        // For the started_at filter, we need to treat the part of the batches that are processing from the part of the
        // batches that are not processing. The non-processing ones are filtered normally while the processing ones
        // are entirely removed unless the in-memory startedAt variable falls within the date filter.
        // Once we have filtered the two subsets, we put them back together and assign it back to `batches`.
        batches = {
            let (mut filtered_non_processing_batches, mut filtered_processing_batches) =
                (&batches - &*processing.processing, &batches & &*processing.processing);

            // special case for Processing batches
            // A closure that clears the filtered_processing_batches if their started_at date falls outside the given bounds
            let mut clear_filtered_processing_batches =
                |start: Bound<OffsetDateTime>, end: Bound<OffsetDateTime>| {
                    let start = map_bound(start, |b| b.unix_timestamp_nanos());
                    let end = map_bound(end, |b| b.unix_timestamp_nanos());
                    let is_within_dates = RangeBounds::contains(
                        &(start, end),
                        &processing
                            .batch
                            .as_ref()
                            .map_or_else(OffsetDateTime::now_utc, |batch| batch.started_at)
                            .unix_timestamp_nanos(),
                    );
                    if !is_within_dates {
                        filtered_processing_batches.clear();
                    }
                };
            match (after_started_at, before_started_at) {
                (None, None) => (),
                (None, Some(before)) => {
                    clear_filtered_processing_batches(Bound::Unbounded, Bound::Excluded(*before))
                }
                (Some(after), None) => {
                    clear_filtered_processing_batches(Bound::Excluded(*after), Bound::Unbounded)
                }
                (Some(after), Some(before)) => clear_filtered_processing_batches(
                    Bound::Excluded(*after),
                    Bound::Excluded(*before),
                ),
            };

            keep_ids_within_datetimes(
                rtxn,
                &mut filtered_non_processing_batches,
                self.batches.started_at,
                *after_started_at,
                *before_started_at,
            )?;
            filtered_non_processing_batches | filtered_processing_batches
        };

        keep_ids_within_datetimes(
            rtxn,
            &mut batches,
            self.batches.enqueued_at,
            *after_enqueued_at,
            *before_enqueued_at,
        )?;

        keep_ids_within_datetimes(
            rtxn,
            &mut batches,
            self.batches.finished_at,
            *after_finished_at,
            *before_finished_at,
        )?;

        if let Some(limit) = limit {
            batches = if query.reverse.unwrap_or_default() {
                batches.into_iter().take(*limit as usize).collect()
            } else {
                batches.into_iter().rev().take(*limit as usize).collect()
            };
        }

        Ok(batches)
    }

    /// Return the batch ids matching the query along with the total number of batches
    /// by ignoring the from and limit parameters from the user's point of view.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    ///    with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub(crate) fn get_batch_ids_from_authorized_indexes(
        &self,
        rtxn: &RoTxn,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
        processing: &ProcessingTasks,
    ) -> Result<(RoaringBitmap, u64)> {
        // compute all batches matching the filter by ignoring the limits, to find the number of batches matching
        // the filter.
        // As this causes us to compute the filter twice it is slightly inefficient, but doing it this way spares
        // us from modifying the underlying implementation, and the performance remains sufficient.
        // Should this change, we would modify `get_batch_ids` to directly return the number of matching batches.
        let total_batches =
            self.get_batch_ids(rtxn, &query.clone().without_limits(), processing)?;
        let mut batches = self.get_batch_ids(rtxn, query, processing)?;

        // If the query contains a list of index uid or there is a finite list of authorized indexes,
        // then we must exclude all the batches that only contains tasks associated to multiple indexes.
        // This works because we don't autobatch tasks associated to multiple indexes with tasks associated
        // to a single index. e.g: IndexSwap cannot be batched with IndexCreation.
        if query.index_uids.is_some() || !filters.all_indexes_authorized() {
            for kind in enum_iterator::all::<Kind>().filter(|kind| !kind.related_to_one_index()) {
                batches -= self.tasks.get_kind(rtxn, kind)?;
                if let Some(batch) = processing.batch.as_ref() {
                    if batch.kinds.contains(&kind) {
                        batches.remove(batch.uid);
                    }
                }
            }
        }

        // Any batch that is internally associated with at least one authorized index
        // must be returned.
        if !filters.all_indexes_authorized() {
            let mut valid_indexes = RoaringBitmap::new();
            let mut forbidden_indexes = RoaringBitmap::new();

            let all_indexes_iter = self.batches.index_tasks.iter(rtxn)?;
            for result in all_indexes_iter {
                let (index, index_tasks) = result?;
                if filters.is_index_authorized(index) {
                    valid_indexes |= index_tasks;
                } else {
                    forbidden_indexes |= index_tasks;
                }
            }
            if let Some(batch) = processing.batch.as_ref() {
                for index in &batch.indexes {
                    if filters.is_index_authorized(index) {
                        valid_indexes.insert(batch.uid);
                    } else {
                        forbidden_indexes.insert(batch.uid);
                    }
                }
            }

            // If a batch had ONE valid task then it should be returned
            let invalid_batches = forbidden_indexes - valid_indexes;

            batches -= invalid_batches;
        }

        Ok((batches, total_batches.len()))
    }

    pub(crate) fn get_batches_from_authorized_indexes(
        &self,
        rtxn: &RoTxn,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
        processing: &ProcessingTasks,
    ) -> Result<(Vec<Batch>, u64)> {
        let (batches, total) =
            self.get_batch_ids_from_authorized_indexes(rtxn, query, filters, processing)?;
        let batches = if query.reverse.unwrap_or_default() {
            Box::new(batches.into_iter()) as Box<dyn Iterator<Item = u32>>
        } else {
            Box::new(batches.into_iter().rev()) as Box<dyn Iterator<Item = u32>>
        };

        let batches = self.batches.get_existing_batches(
            rtxn,
            batches.take(query.limit.unwrap_or(u32::MAX) as usize),
            processing,
        )?;

        Ok((batches, total))
    }
}

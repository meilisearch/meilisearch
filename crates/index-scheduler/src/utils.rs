//! Utility functions on the DBs. Mainly getter and setters.

use std::collections::{BTreeSet, HashSet};
use std::ops::Bound;

use meilisearch_types::heed::types::DecodeIgnore;
use meilisearch_types::heed::{Database, RoTxn, RwTxn};
use meilisearch_types::milli::CboRoaringBitmapCodec;
use meilisearch_types::tasks::{Details, IndexSwap, Kind, KindWithContent, Status};
use roaring::{MultiOps, RoaringBitmap};
use time::OffsetDateTime;

use crate::{Error, IndexScheduler, Result, Task, TaskId, BEI128};

impl IndexScheduler {
    pub(crate) fn all_task_ids(&self, rtxn: &RoTxn) -> Result<RoaringBitmap> {
        enum_iterator::all().map(|s| self.get_status(rtxn, s)).union()
    }

    pub(crate) fn last_task_id(&self, rtxn: &RoTxn) -> Result<Option<TaskId>> {
        Ok(self.all_tasks.remap_data_type::<DecodeIgnore>().last(rtxn)?.map(|(k, _)| k + 1))
    }

    pub(crate) fn next_task_id(&self, rtxn: &RoTxn) -> Result<TaskId> {
        Ok(self.last_task_id(rtxn)?.unwrap_or_default())
    }

    pub(crate) fn get_task(&self, rtxn: &RoTxn, task_id: TaskId) -> Result<Option<Task>> {
        Ok(self.all_tasks.get(rtxn, &task_id)?)
    }

    /// Convert an iterator to a `Vec` of tasks. The tasks MUST exist or a
    /// `CorruptedTaskQueue` error will be throwed.
    pub(crate) fn get_existing_tasks(
        &self,
        rtxn: &RoTxn,
        tasks: impl IntoIterator<Item = TaskId>,
    ) -> Result<Vec<Task>> {
        tasks
            .into_iter()
            .map(|task_id| {
                self.get_task(rtxn, task_id).and_then(|task| task.ok_or(Error::CorruptedTaskQueue))
            })
            .collect::<Result<_>>()
    }

    pub(crate) fn update_task(&self, wtxn: &mut RwTxn, task: &Task) -> Result<()> {
        let old_task = self.get_task(wtxn, task.uid)?.ok_or(Error::CorruptedTaskQueue)?;

        debug_assert_eq!(old_task.uid, task.uid);

        if old_task == *task {
            return Ok(());
        }

        if old_task.status != task.status {
            self.update_status(wtxn, old_task.status, |bitmap| {
                bitmap.remove(task.uid);
            })?;
            self.update_status(wtxn, task.status, |bitmap| {
                bitmap.insert(task.uid);
            })?;
        }

        if old_task.kind.as_kind() != task.kind.as_kind() {
            self.update_kind(wtxn, old_task.kind.as_kind(), |bitmap| {
                bitmap.remove(task.uid);
            })?;
            self.update_kind(wtxn, task.kind.as_kind(), |bitmap| {
                bitmap.insert(task.uid);
            })?;
        }

        assert_eq!(
            old_task.enqueued_at, task.enqueued_at,
            "Cannot update a task's enqueued_at time"
        );
        if old_task.started_at != task.started_at {
            assert!(old_task.started_at.is_none(), "Cannot update a task's started_at time");
            if let Some(started_at) = task.started_at {
                insert_task_datetime(wtxn, self.started_at, started_at, task.uid)?;
            }
        }
        if old_task.finished_at != task.finished_at {
            assert!(old_task.finished_at.is_none(), "Cannot update a task's finished_at time");
            if let Some(finished_at) = task.finished_at {
                insert_task_datetime(wtxn, self.finished_at, finished_at, task.uid)?;
            }
        }

        self.all_tasks.put(wtxn, &task.uid, task)?;
        Ok(())
    }

    /// Returns the whole set of tasks that belongs to this index.
    pub(crate) fn index_tasks(&self, rtxn: &RoTxn, index: &str) -> Result<RoaringBitmap> {
        Ok(self.index_tasks.get(rtxn, index)?.unwrap_or_default())
    }

    pub(crate) fn update_index(
        &self,
        wtxn: &mut RwTxn,
        index: &str,
        f: impl Fn(&mut RoaringBitmap),
    ) -> Result<()> {
        let mut tasks = self.index_tasks(wtxn, index)?;
        f(&mut tasks);
        if tasks.is_empty() {
            self.index_tasks.delete(wtxn, index)?;
        } else {
            self.index_tasks.put(wtxn, index, &tasks)?;
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
}

pub(crate) fn insert_task_datetime(
    wtxn: &mut RwTxn,
    database: Database<BEI128, CboRoaringBitmapCodec>,
    time: OffsetDateTime,
    task_id: TaskId,
) -> Result<()> {
    let timestamp = time.unix_timestamp_nanos();
    let mut task_ids = database.get(wtxn, &timestamp)?.unwrap_or_default();
    task_ids.insert(task_id);
    database.put(wtxn, &timestamp, &RoaringBitmap::from_iter(task_ids))?;
    Ok(())
}

pub(crate) fn remove_task_datetime(
    wtxn: &mut RwTxn,
    database: Database<BEI128, CboRoaringBitmapCodec>,
    time: OffsetDateTime,
    task_id: TaskId,
) -> Result<()> {
    let timestamp = time.unix_timestamp_nanos();
    if let Some(mut existing) = database.get(wtxn, &timestamp)? {
        existing.remove(task_id);
        if existing.is_empty() {
            database.delete(wtxn, &timestamp)?;
        } else {
            database.put(wtxn, &timestamp, &RoaringBitmap::from_iter(existing))?;
        }
    }

    Ok(())
}

pub(crate) fn keep_tasks_within_datetimes(
    rtxn: &RoTxn,
    tasks: &mut RoaringBitmap,
    database: Database<BEI128, CboRoaringBitmapCodec>,
    after: Option<OffsetDateTime>,
    before: Option<OffsetDateTime>,
) -> Result<()> {
    let (start, end) = match (&after, &before) {
        (None, None) => return Ok(()),
        (None, Some(before)) => (Bound::Unbounded, Bound::Excluded(*before)),
        (Some(after), None) => (Bound::Excluded(*after), Bound::Unbounded),
        (Some(after), Some(before)) => (Bound::Excluded(*after), Bound::Excluded(*before)),
    };
    let mut collected_task_ids = RoaringBitmap::new();
    let start = map_bound(start, |b| b.unix_timestamp_nanos());
    let end = map_bound(end, |b| b.unix_timestamp_nanos());
    let iter = database.range(rtxn, &(start, end))?;
    for r in iter {
        let (_timestamp, task_ids) = r?;
        collected_task_ids |= task_ids;
    }
    *tasks &= collected_task_ids;
    Ok(())
}

// TODO: remove when Bound::map ( https://github.com/rust-lang/rust/issues/86026 ) is available on stable
pub(crate) fn map_bound<T, U>(bound: Bound<T>, map: impl FnOnce(T) -> U) -> Bound<U> {
    match bound {
        Bound::Included(x) => Bound::Included(map(x)),
        Bound::Excluded(x) => Bound::Excluded(map(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub fn swap_index_uid_in_task(task: &mut Task, swap: (&str, &str)) {
    use KindWithContent as K;
    let mut index_uids = vec![];
    match &mut task.kind {
        K::DocumentAdditionOrUpdate { index_uid, .. } => index_uids.push(index_uid),
        K::DocumentEdition { index_uid, .. } => index_uids.push(index_uid),
        K::DocumentDeletion { index_uid, .. } => index_uids.push(index_uid),
        K::DocumentDeletionByFilter { index_uid, .. } => index_uids.push(index_uid),
        K::DocumentClear { index_uid } => index_uids.push(index_uid),
        K::SettingsUpdate { index_uid, .. } => index_uids.push(index_uid),
        K::IndexDeletion { index_uid } => index_uids.push(index_uid),
        K::IndexCreation { index_uid, .. } => index_uids.push(index_uid),
        K::IndexUpdate { index_uid, .. } => index_uids.push(index_uid),
        K::IndexSwap { swaps } => {
            for IndexSwap { indexes: (lhs, rhs) } in swaps.iter_mut() {
                if lhs == swap.0 || lhs == swap.1 {
                    index_uids.push(lhs);
                }
                if rhs == swap.0 || rhs == swap.1 {
                    index_uids.push(rhs);
                }
            }
        }
        K::TaskCancelation { .. }
        | K::TaskDeletion { .. }
        | K::DumpCreation { .. }
        | K::SnapshotCreation => (),
    };
    if let Some(Details::IndexSwap { swaps }) = &mut task.details {
        for IndexSwap { indexes: (lhs, rhs) } in swaps.iter_mut() {
            if lhs == swap.0 || lhs == swap.1 {
                index_uids.push(lhs);
            }
            if rhs == swap.0 || rhs == swap.1 {
                index_uids.push(rhs);
            }
        }
    }
    for index_uid in index_uids {
        if index_uid == swap.0 {
            swap.1.clone_into(index_uid);
        } else if index_uid == swap.1 {
            swap.0.clone_into(index_uid);
        }
    }
}

/// Remove references to task ids that are greater than the id of the given task.
pub(crate) fn filter_out_references_to_newer_tasks(task: &mut Task) {
    let new_nbr_of_matched_tasks = match &mut task.kind {
        KindWithContent::TaskCancelation { tasks, .. }
        | KindWithContent::TaskDeletion { tasks, .. } => {
            tasks.remove_range(task.uid..);
            tasks.len()
        }
        _ => return,
    };
    if let Some(
        Details::TaskCancelation { matched_tasks, .. }
        | Details::TaskDeletion { matched_tasks, .. },
    ) = &mut task.details
    {
        *matched_tasks = new_nbr_of_matched_tasks;
    }
}

pub(crate) fn check_index_swap_validity(task: &Task) -> Result<()> {
    let swaps =
        if let KindWithContent::IndexSwap { swaps } = &task.kind { swaps } else { return Ok(()) };
    let mut all_indexes = HashSet::new();
    let mut duplicate_indexes = BTreeSet::new();
    for IndexSwap { indexes: (lhs, rhs) } in swaps {
        for name in [lhs, rhs] {
            let is_new = all_indexes.insert(name);
            if !is_new {
                duplicate_indexes.insert(name);
            }
        }
    }
    if !duplicate_indexes.is_empty() {
        if duplicate_indexes.len() == 1 {
            return Err(Error::SwapDuplicateIndexFound(
                duplicate_indexes.into_iter().next().unwrap().clone(),
            ));
        } else {
            return Err(Error::SwapDuplicateIndexesFound(
                duplicate_indexes.into_iter().cloned().collect(),
            ));
        }
    }
    Ok(())
}

/// Clamp the provided value to be a multiple of system page size.
pub fn clamp_to_page_size(size: usize) -> usize {
    size / page_size::get() * page_size::get()
}

#[cfg(test)]
impl IndexScheduler {
    /// Asserts that the index scheduler's content is internally consistent.
    pub fn assert_internally_consistent(&self) {
        let rtxn = self.env.read_txn().unwrap();
        for task in self.all_tasks.iter(&rtxn).unwrap() {
            let (task_id, task) = task.unwrap();
            let task_index_uid = task.index_uid().map(ToOwned::to_owned);

            let Task {
                uid,
                enqueued_at,
                started_at,
                finished_at,
                error: _,
                canceled_by,
                details,
                status,
                kind,
            } = task;
            assert_eq!(uid, task.uid);
            if let Some(task_index_uid) = &task_index_uid {
                assert!(self
                    .index_tasks
                    .get(&rtxn, task_index_uid.as_str())
                    .unwrap()
                    .unwrap()
                    .contains(task.uid));
            }
            let db_enqueued_at =
                self.enqueued_at.get(&rtxn, &enqueued_at.unix_timestamp_nanos()).unwrap().unwrap();
            assert!(db_enqueued_at.contains(task_id));
            if let Some(started_at) = started_at {
                let db_started_at = self
                    .started_at
                    .get(&rtxn, &started_at.unix_timestamp_nanos())
                    .unwrap()
                    .unwrap();
                assert!(db_started_at.contains(task_id));
            }
            if let Some(finished_at) = finished_at {
                let db_finished_at = self
                    .finished_at
                    .get(&rtxn, &finished_at.unix_timestamp_nanos())
                    .unwrap()
                    .unwrap();
                assert!(db_finished_at.contains(task_id));
            }
            if let Some(canceled_by) = canceled_by {
                let db_canceled_tasks = self.get_status(&rtxn, Status::Canceled).unwrap();
                assert!(db_canceled_tasks.contains(uid));
                let db_canceling_task = self.get_task(&rtxn, canceled_by).unwrap().unwrap();
                assert_eq!(db_canceling_task.status, Status::Succeeded);
                match db_canceling_task.kind {
                    KindWithContent::TaskCancelation { query: _, tasks } => {
                        assert!(tasks.contains(uid));
                    }
                    _ => panic!(),
                }
            }
            if let Some(details) = details {
                match details {
                    Details::IndexSwap { swaps: sw1 } => {
                        if let KindWithContent::IndexSwap { swaps: sw2 } = &kind {
                            assert_eq!(&sw1, sw2);
                        }
                    }
                    Details::DocumentAdditionOrUpdate { received_documents, indexed_documents } => {
                        assert_eq!(kind.as_kind(), Kind::DocumentAdditionOrUpdate);
                        match indexed_documents {
                            Some(indexed_documents) => {
                                assert!(matches!(
                                    status,
                                    Status::Succeeded | Status::Failed | Status::Canceled
                                ));
                                match status {
                                    Status::Succeeded => assert!(indexed_documents <= received_documents),
                                    Status::Failed | Status::Canceled => assert_eq!(indexed_documents, 0),
                                    status => panic!("DocumentAddition can't have an indexed_documents set if it's {}", status),
                                }
                            }
                            None => {
                                assert!(matches!(status, Status::Enqueued | Status::Processing))
                            }
                        }
                    }
                    Details::DocumentEdition { edited_documents, .. } => {
                        assert_eq!(kind.as_kind(), Kind::DocumentEdition);
                        match edited_documents {
                            Some(edited_documents) => {
                                assert!(matches!(
                                    status,
                                    Status::Succeeded | Status::Failed | Status::Canceled
                                ));
                                match status {
                                    Status::Succeeded => (),
                                    Status::Failed | Status::Canceled => assert_eq!(edited_documents, 0),
                                    status => panic!("DocumentEdition can't have an edited_documents set if it's {}", status),
                                }
                            }
                            None => {
                                assert!(matches!(status, Status::Enqueued | Status::Processing))
                            }
                        }
                    }
                    Details::SettingsUpdate { settings: _ } => {
                        assert_eq!(kind.as_kind(), Kind::SettingsUpdate);
                    }
                    Details::IndexInfo { primary_key: pk1 } => match &kind {
                        KindWithContent::IndexCreation { index_uid, primary_key: pk2 }
                        | KindWithContent::IndexUpdate { index_uid, primary_key: pk2 } => {
                            self.index_tasks
                                .get(&rtxn, index_uid.as_str())
                                .unwrap()
                                .unwrap()
                                .contains(uid);
                            assert_eq!(&pk1, pk2);
                        }
                        _ => panic!(),
                    },
                    Details::DocumentDeletion {
                        provided_ids: received_document_ids,
                        deleted_documents,
                    } => {
                        assert_eq!(kind.as_kind(), Kind::DocumentDeletion);
                        let (index_uid, documents_ids) =
                            if let KindWithContent::DocumentDeletion {
                                ref index_uid,
                                ref documents_ids,
                            } = kind
                            {
                                (index_uid, documents_ids)
                            } else {
                                unreachable!()
                            };
                        assert_eq!(&task_index_uid.unwrap(), index_uid);

                        match status {
                            Status::Enqueued | Status::Processing => (),
                            Status::Succeeded => {
                                assert!(deleted_documents.unwrap() <= received_document_ids as u64);
                                assert!(documents_ids.len() == received_document_ids);
                            }
                            Status::Failed | Status::Canceled => {
                                assert!(deleted_documents == Some(0));
                                assert!(documents_ids.len() == received_document_ids);
                            }
                        }
                    }
                    Details::DocumentDeletionByFilter { deleted_documents, original_filter: _ } => {
                        assert_eq!(kind.as_kind(), Kind::DocumentDeletion);
                        let (index_uid, _) = if let KindWithContent::DocumentDeletionByFilter {
                            ref index_uid,
                            ref filter_expr,
                        } = kind
                        {
                            (index_uid, filter_expr)
                        } else {
                            unreachable!()
                        };
                        assert_eq!(&task_index_uid.unwrap(), index_uid);

                        match status {
                            Status::Enqueued | Status::Processing => (),
                            Status::Succeeded => {
                                assert!(deleted_documents.is_some());
                            }
                            Status::Failed | Status::Canceled => {
                                assert!(deleted_documents == Some(0));
                            }
                        }
                    }
                    Details::ClearAll { deleted_documents } => {
                        assert!(matches!(
                            kind.as_kind(),
                            Kind::DocumentDeletion | Kind::IndexDeletion
                        ));
                        if deleted_documents.is_some() {
                            assert_eq!(status, Status::Succeeded);
                        } else {
                            assert_ne!(status, Status::Succeeded);
                        }
                    }
                    Details::TaskCancelation { matched_tasks, canceled_tasks, original_filter } => {
                        if let Some(canceled_tasks) = canceled_tasks {
                            assert_eq!(status, Status::Succeeded);
                            assert!(canceled_tasks <= matched_tasks);
                            match &kind {
                                KindWithContent::TaskCancelation { query, tasks } => {
                                    assert_eq!(query, &original_filter);
                                    assert_eq!(tasks.len(), matched_tasks);
                                }
                                _ => panic!(),
                            }
                        } else {
                            assert_ne!(status, Status::Succeeded);
                        }
                    }
                    Details::TaskDeletion { matched_tasks, deleted_tasks, original_filter } => {
                        if let Some(deleted_tasks) = deleted_tasks {
                            assert_eq!(status, Status::Succeeded);
                            assert!(deleted_tasks <= matched_tasks);
                            match &kind {
                                KindWithContent::TaskDeletion { query, tasks } => {
                                    assert_eq!(query, &original_filter);
                                    assert_eq!(tasks.len(), matched_tasks);
                                }
                                _ => panic!(),
                            }
                        } else {
                            assert_ne!(status, Status::Succeeded);
                        }
                    }
                    Details::Dump { dump_uid: _ } => {
                        assert_eq!(kind.as_kind(), Kind::DumpCreation);
                    }
                }
            }

            assert!(self.get_status(&rtxn, status).unwrap().contains(uid));
            assert!(self.get_kind(&rtxn, kind.as_kind()).unwrap().contains(uid));

            if let KindWithContent::DocumentAdditionOrUpdate { content_file, .. } = kind {
                match status {
                    Status::Enqueued | Status::Processing => {
                        assert!(self
                            .file_store
                            .all_uuids()
                            .unwrap()
                            .any(|uuid| uuid.as_ref().unwrap() == &content_file),
                            "Could not find uuid `{content_file}` in the file_store. Available uuids are {:?}.",
                            self.file_store.all_uuids().unwrap().collect::<std::result::Result<Vec<_>, file_store::Error>>().unwrap(),
                        );
                    }
                    Status::Succeeded | Status::Failed | Status::Canceled => {
                        assert!(self
                            .file_store
                            .all_uuids()
                            .unwrap()
                            .all(|uuid| uuid.as_ref().unwrap() != &content_file));
                    }
                }
            }
        }
    }
}

pub fn dichotomic_search(start_point: usize, mut is_good: impl FnMut(usize) -> bool) -> usize {
    let mut biggest_good = None;
    let mut smallest_bad = None;
    let mut current = start_point;
    loop {
        let is_good = is_good(current);

        (biggest_good, smallest_bad, current) = match (biggest_good, smallest_bad, is_good) {
            (None, None, false) => (None, Some(current), current / 2),
            (None, None, true) => (Some(current), None, current * 2),
            (None, Some(smallest_bad), true) => {
                (Some(current), Some(smallest_bad), (current + smallest_bad) / 2)
            }
            (None, Some(_), false) => (None, Some(current), current / 2),
            (Some(_), None, true) => (Some(current), None, current * 2),
            (Some(biggest_good), None, false) => {
                (Some(biggest_good), Some(current), (biggest_good + current) / 2)
            }
            (Some(_), Some(smallest_bad), true) => {
                (Some(current), Some(smallest_bad), (smallest_bad + current) / 2)
            }
            (Some(biggest_good), Some(_), false) => {
                (Some(biggest_good), Some(current), (biggest_good + current) / 2)
            }
        };
        if current == 0 {
            return current;
        }
        if smallest_bad.is_some() && biggest_good.is_some() && biggest_good >= Some(current) {
            return current;
        }
    }
}

/*!
This module handles the creation and processing of batch operations.

A batch is a combination of multiple tasks that can be processed at once.
Executing a batch operation should always be functionally equivalent to
executing each of its tasks' operations individually and in order.

For example, if the user sends two tasks:
1. import documents X
2. import documents Y

We can combine the two tasks in a single batch:
1. import documents X and Y

Processing this batch is functionally equivalent to processing the two
tasks individually, but should be much faster since we are only performing
one indexing operation.
*/

use std::collections::{BTreeSet, HashSet};
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File};
use std::io::BufWriter;

use dump::IndexMetadata;
use meilisearch_types::error::Code;
use meilisearch_types::heed::{RoTxn, RwTxn};
use meilisearch_types::milli::documents::{obkv_to_object, DocumentsBatchReader};
use meilisearch_types::milli::heed::CompactionOption;
use meilisearch_types::milli::update::{
    IndexDocumentsConfig, IndexDocumentsMethod, IndexerConfig, Settings as MilliSettings,
};
use meilisearch_types::milli::vector::parsed_vectors::{
    ExplicitVectors, VectorOrArrayOfVectors, RESERVED_VECTORS_FIELD_NAME,
};
use meilisearch_types::milli::{self, Filter};
use meilisearch_types::settings::{apply_settings_to_builder, Settings, Unchecked};
use meilisearch_types::tasks::{Details, IndexSwap, Kind, KindWithContent, Status, Task};
use meilisearch_types::{compression, Index, VERSION_FILE_NAME};
use roaring::RoaringBitmap;
use time::macros::format_description;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::autobatcher::{self, BatchKind};
use crate::utils::{self, swap_index_uid_in_task};
use crate::{Error, IndexScheduler, MustStopProcessing, ProcessingTasks, Result, TaskId};

/// Represents a combination of tasks that can all be processed at the same time.
///
/// A batch contains the set of tasks that it represents (accessible through
/// [`self.ids()`](Batch::ids)), as well as additional information on how to
/// be processed.
#[derive(Debug)]
pub(crate) enum Batch {
    TaskCancelation {
        /// The task cancelation itself.
        task: Task,
        /// The date and time at which the previously processing tasks started.
        previous_started_at: OffsetDateTime,
        /// The list of tasks that were processing when this task cancelation appeared.
        previous_processing_tasks: RoaringBitmap,
    },
    TaskDeletions(Vec<Task>),
    SnapshotCreation(Vec<Task>),
    Dump(Task),
    IndexOperation {
        op: IndexOperation,
        must_create_index: bool,
    },
    IndexCreation {
        index_uid: String,
        primary_key: Option<String>,
        task: Task,
    },
    IndexUpdate {
        index_uid: String,
        primary_key: Option<String>,
        task: Task,
    },
    IndexDeletion {
        index_uid: String,
        tasks: Vec<Task>,
        index_has_been_created: bool,
    },
    IndexSwap {
        task: Task,
    },
}

#[derive(Debug)]
pub(crate) enum DocumentOperation {
    Add(Uuid),
    Delete(Vec<String>),
}

/// A [batch](Batch) that combines multiple tasks operating on an index.
#[derive(Debug)]
pub(crate) enum IndexOperation {
    DocumentOperation {
        index_uid: String,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        documents_counts: Vec<u64>,
        operations: Vec<DocumentOperation>,
        tasks: Vec<Task>,
    },
    DocumentEdition {
        index_uid: String,
        task: Task,
    },
    IndexDocumentDeletionByFilter {
        index_uid: String,
        task: Task,
    },
    DocumentClear {
        index_uid: String,
        tasks: Vec<Task>,
    },
    Settings {
        index_uid: String,
        // The boolean indicates if it's a settings deletion or creation.
        settings: Vec<(bool, Settings<Unchecked>)>,
        tasks: Vec<Task>,
    },
    DocumentClearAndSetting {
        index_uid: String,
        cleared_tasks: Vec<Task>,

        // The boolean indicates if it's a settings deletion or creation.
        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<Task>,
    },
    SettingsAndDocumentOperation {
        index_uid: String,

        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        documents_counts: Vec<u64>,
        operations: Vec<DocumentOperation>,
        document_import_tasks: Vec<Task>,

        // The boolean indicates if it's a settings deletion or creation.
        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<Task>,
    },
}

impl Batch {
    /// Return the task ids associated with this batch.
    pub fn ids(&self) -> RoaringBitmap {
        match self {
            Batch::TaskCancelation { task, .. }
            | Batch::Dump(task)
            | Batch::IndexCreation { task, .. }
            | Batch::IndexUpdate { task, .. } => {
                RoaringBitmap::from_sorted_iter(std::iter::once(task.uid)).unwrap()
            }
            Batch::SnapshotCreation(tasks)
            | Batch::TaskDeletions(tasks)
            | Batch::IndexDeletion { tasks, .. } => {
                RoaringBitmap::from_iter(tasks.iter().map(|task| task.uid))
            }
            Batch::IndexOperation { op, .. } => match op {
                IndexOperation::DocumentOperation { tasks, .. }
                | IndexOperation::Settings { tasks, .. }
                | IndexOperation::DocumentClear { tasks, .. } => {
                    RoaringBitmap::from_iter(tasks.iter().map(|task| task.uid))
                }
                IndexOperation::DocumentEdition { task, .. }
                | IndexOperation::IndexDocumentDeletionByFilter { task, .. } => {
                    RoaringBitmap::from_sorted_iter(std::iter::once(task.uid)).unwrap()
                }
                IndexOperation::SettingsAndDocumentOperation {
                    document_import_tasks: tasks,
                    settings_tasks: other,
                    ..
                }
                | IndexOperation::DocumentClearAndSetting {
                    cleared_tasks: tasks,
                    settings_tasks: other,
                    ..
                } => RoaringBitmap::from_iter(tasks.iter().chain(other).map(|task| task.uid)),
            },
            Batch::IndexSwap { task } => {
                RoaringBitmap::from_sorted_iter(std::iter::once(task.uid)).unwrap()
            }
        }
    }

    /// Return the index UID associated with this batch
    pub fn index_uid(&self) -> Option<&str> {
        use Batch::*;
        match self {
            TaskCancelation { .. }
            | TaskDeletions(_)
            | SnapshotCreation(_)
            | Dump(_)
            | IndexSwap { .. } => None,
            IndexOperation { op, .. } => Some(op.index_uid()),
            IndexCreation { index_uid, .. }
            | IndexUpdate { index_uid, .. }
            | IndexDeletion { index_uid, .. } => Some(index_uid),
        }
    }
}

impl fmt::Display for Batch {
    /// A text used when we debug the profiling reports.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let index_uid = self.index_uid();
        let tasks = self.ids();
        match self {
            Batch::TaskCancelation { .. } => f.write_str("TaskCancelation")?,
            Batch::TaskDeletions(_) => f.write_str("TaskDeletion")?,
            Batch::SnapshotCreation(_) => f.write_str("SnapshotCreation")?,
            Batch::Dump(_) => f.write_str("Dump")?,
            Batch::IndexOperation { op, .. } => write!(f, "{op}")?,
            Batch::IndexCreation { .. } => f.write_str("IndexCreation")?,
            Batch::IndexUpdate { .. } => f.write_str("IndexUpdate")?,
            Batch::IndexDeletion { .. } => f.write_str("IndexDeletion")?,
            Batch::IndexSwap { .. } => f.write_str("IndexSwap")?,
        };
        match index_uid {
            Some(name) => f.write_fmt(format_args!(" on {name:?} from tasks: {tasks:?}")),
            None => f.write_fmt(format_args!(" from tasks: {tasks:?}")),
        }
    }
}

impl IndexOperation {
    pub fn index_uid(&self) -> &str {
        match self {
            IndexOperation::DocumentOperation { index_uid, .. }
            | IndexOperation::DocumentEdition { index_uid, .. }
            | IndexOperation::IndexDocumentDeletionByFilter { index_uid, .. }
            | IndexOperation::DocumentClear { index_uid, .. }
            | IndexOperation::Settings { index_uid, .. }
            | IndexOperation::DocumentClearAndSetting { index_uid, .. }
            | IndexOperation::SettingsAndDocumentOperation { index_uid, .. } => index_uid,
        }
    }
}

impl fmt::Display for IndexOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexOperation::DocumentOperation { .. } => {
                f.write_str("IndexOperation::DocumentOperation")
            }
            IndexOperation::DocumentEdition { .. } => {
                f.write_str("IndexOperation::DocumentEdition")
            }
            IndexOperation::IndexDocumentDeletionByFilter { .. } => {
                f.write_str("IndexOperation::IndexDocumentDeletionByFilter")
            }
            IndexOperation::DocumentClear { .. } => f.write_str("IndexOperation::DocumentClear"),
            IndexOperation::Settings { .. } => f.write_str("IndexOperation::Settings"),
            IndexOperation::DocumentClearAndSetting { .. } => {
                f.write_str("IndexOperation::DocumentClearAndSetting")
            }
            IndexOperation::SettingsAndDocumentOperation { .. } => {
                f.write_str("IndexOperation::SettingsAndDocumentOperation")
            }
        }
    }
}

impl IndexScheduler {
    /// Convert an [`BatchKind`](crate::autobatcher::BatchKind) into a [`Batch`].
    ///
    /// ## Arguments
    /// - `rtxn`: read transaction
    /// - `index_uid`: name of the index affected by the operations of the autobatch
    /// - `batch`: the result of the autobatcher
    pub(crate) fn create_next_batch_index(
        &self,
        rtxn: &RoTxn,
        index_uid: String,
        batch: BatchKind,
        must_create_index: bool,
    ) -> Result<Option<Batch>> {
        match batch {
            BatchKind::DocumentClear { ids } => Ok(Some(Batch::IndexOperation {
                op: IndexOperation::DocumentClear {
                    tasks: self.get_existing_tasks(rtxn, ids)?,
                    index_uid,
                },
                must_create_index,
            })),
            BatchKind::DocumentDeletionByFilter { id } => {
                let task = self.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                match &task.kind {
                    KindWithContent::DocumentDeletionByFilter { index_uid, .. } => {
                        Ok(Some(Batch::IndexOperation {
                            op: IndexOperation::IndexDocumentDeletionByFilter {
                                index_uid: index_uid.clone(),
                                task,
                            },
                            must_create_index: false,
                        }))
                    }
                    _ => unreachable!(),
                }
            }
            BatchKind::DocumentEdition { id } => {
                let task = self.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                match &task.kind {
                    KindWithContent::DocumentEdition { index_uid, .. } => {
                        Ok(Some(Batch::IndexOperation {
                            op: IndexOperation::DocumentEdition {
                                index_uid: index_uid.clone(),
                                task,
                            },
                            must_create_index: false,
                        }))
                    }
                    _ => unreachable!(),
                }
            }
            BatchKind::DocumentOperation { method, operation_ids, .. } => {
                let tasks = self.get_existing_tasks(rtxn, operation_ids)?;
                let primary_key = tasks
                    .iter()
                    .find_map(|task| match task.kind {
                        KindWithContent::DocumentAdditionOrUpdate { ref primary_key, .. } => {
                            // we want to stop on the first document addition
                            Some(primary_key.clone())
                        }
                        KindWithContent::DocumentDeletion { .. } => None,
                        _ => unreachable!(),
                    })
                    .flatten();

                let mut documents_counts = Vec::new();
                let mut operations = Vec::new();

                for task in tasks.iter() {
                    match task.kind {
                        KindWithContent::DocumentAdditionOrUpdate {
                            content_file,
                            documents_count,
                            ..
                        } => {
                            documents_counts.push(documents_count);
                            operations.push(DocumentOperation::Add(content_file));
                        }
                        KindWithContent::DocumentDeletion { ref documents_ids, .. } => {
                            documents_counts.push(documents_ids.len() as u64);
                            operations.push(DocumentOperation::Delete(documents_ids.clone()));
                        }
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::IndexOperation {
                    op: IndexOperation::DocumentOperation {
                        index_uid,
                        primary_key,
                        method,
                        documents_counts,
                        operations,
                        tasks,
                    },
                    must_create_index,
                }))
            }
            BatchKind::DocumentDeletion { deletion_ids } => {
                let tasks = self.get_existing_tasks(rtxn, deletion_ids)?;

                let mut operations = Vec::with_capacity(tasks.len());
                let mut documents_counts = Vec::with_capacity(tasks.len());
                for task in &tasks {
                    match task.kind {
                        KindWithContent::DocumentDeletion { ref documents_ids, .. } => {
                            operations.push(DocumentOperation::Delete(documents_ids.clone()));
                            documents_counts.push(documents_ids.len() as u64);
                        }
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::IndexOperation {
                    op: IndexOperation::DocumentOperation {
                        index_uid,
                        primary_key: None,
                        method: IndexDocumentsMethod::ReplaceDocuments,
                        documents_counts,
                        operations,
                        tasks,
                    },
                    must_create_index,
                }))
            }
            BatchKind::Settings { settings_ids, .. } => {
                let tasks = self.get_existing_tasks(rtxn, settings_ids)?;

                let mut settings = Vec::new();
                for task in &tasks {
                    match task.kind {
                        KindWithContent::SettingsUpdate {
                            ref new_settings, is_deletion, ..
                        } => settings.push((is_deletion, *new_settings.clone())),
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::IndexOperation {
                    op: IndexOperation::Settings { index_uid, settings, tasks },
                    must_create_index,
                }))
            }
            BatchKind::ClearAndSettings { other, settings_ids, allow_index_creation } => {
                let (index_uid, settings, settings_tasks) = match self
                    .create_next_batch_index(
                        rtxn,
                        index_uid,
                        BatchKind::Settings { settings_ids, allow_index_creation },
                        must_create_index,
                    )?
                    .unwrap()
                {
                    Batch::IndexOperation {
                        op: IndexOperation::Settings { index_uid, settings, tasks, .. },
                        ..
                    } => (index_uid, settings, tasks),
                    _ => unreachable!(),
                };
                let (index_uid, cleared_tasks) = match self
                    .create_next_batch_index(
                        rtxn,
                        index_uid,
                        BatchKind::DocumentClear { ids: other },
                        must_create_index,
                    )?
                    .unwrap()
                {
                    Batch::IndexOperation {
                        op: IndexOperation::DocumentClear { index_uid, tasks },
                        ..
                    } => (index_uid, tasks),
                    _ => unreachable!(),
                };

                Ok(Some(Batch::IndexOperation {
                    op: IndexOperation::DocumentClearAndSetting {
                        index_uid,
                        cleared_tasks,
                        settings,
                        settings_tasks,
                    },
                    must_create_index,
                }))
            }
            BatchKind::SettingsAndDocumentOperation {
                settings_ids,
                method,
                allow_index_creation,
                primary_key,
                operation_ids,
            } => {
                let settings = self.create_next_batch_index(
                    rtxn,
                    index_uid.clone(),
                    BatchKind::Settings { settings_ids, allow_index_creation },
                    must_create_index,
                )?;

                let document_import = self.create_next_batch_index(
                    rtxn,
                    index_uid.clone(),
                    BatchKind::DocumentOperation {
                        method,
                        allow_index_creation,
                        primary_key,
                        operation_ids,
                    },
                    must_create_index,
                )?;

                match (document_import, settings) {
                    (
                        Some(Batch::IndexOperation {
                            op:
                                IndexOperation::DocumentOperation {
                                    primary_key,
                                    documents_counts,
                                    operations,
                                    tasks: document_import_tasks,
                                    ..
                                },
                            ..
                        }),
                        Some(Batch::IndexOperation {
                            op: IndexOperation::Settings { settings, tasks: settings_tasks, .. },
                            ..
                        }),
                    ) => Ok(Some(Batch::IndexOperation {
                        op: IndexOperation::SettingsAndDocumentOperation {
                            index_uid,
                            primary_key,
                            method,
                            documents_counts,
                            operations,
                            document_import_tasks,
                            settings,
                            settings_tasks,
                        },
                        must_create_index,
                    })),
                    _ => unreachable!(),
                }
            }
            BatchKind::IndexCreation { id } => {
                let task = self.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                let (index_uid, primary_key) = match &task.kind {
                    KindWithContent::IndexCreation { index_uid, primary_key } => {
                        (index_uid.clone(), primary_key.clone())
                    }
                    _ => unreachable!(),
                };
                Ok(Some(Batch::IndexCreation { index_uid, primary_key, task }))
            }
            BatchKind::IndexUpdate { id } => {
                let task = self.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                let primary_key = match &task.kind {
                    KindWithContent::IndexUpdate { primary_key, .. } => primary_key.clone(),
                    _ => unreachable!(),
                };
                Ok(Some(Batch::IndexUpdate { index_uid, primary_key, task }))
            }
            BatchKind::IndexDeletion { ids } => Ok(Some(Batch::IndexDeletion {
                index_uid,
                index_has_been_created: must_create_index,
                tasks: self.get_existing_tasks(rtxn, ids)?,
            })),
            BatchKind::IndexSwap { id } => {
                let task = self.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                Ok(Some(Batch::IndexSwap { task }))
            }
        }
    }

    /// Create the next batch to be processed;
    /// 1. We get the *last* task to cancel.
    /// 2. We get the *next* task to delete.
    /// 3. We get the *next* snapshot to process.
    /// 4. We get the *next* dump to process.
    /// 5. We get the *next* tasks to process for a specific index.
    #[tracing::instrument(level = "trace", skip(self, rtxn), target = "indexing::scheduler")]
    pub(crate) fn create_next_batch(&self, rtxn: &RoTxn) -> Result<Option<Batch>> {
        #[cfg(test)]
        self.maybe_fail(crate::tests::FailureLocation::InsideCreateBatch)?;

        let enqueued = &self.get_status(rtxn, Status::Enqueued)?;
        let to_cancel = self.get_kind(rtxn, Kind::TaskCancelation)? & enqueued;

        // 1. we get the last task to cancel.
        if let Some(task_id) = to_cancel.max() {
            // We retrieve the tasks that were processing before this tasks cancelation started.
            // We must *not* reset the processing tasks before calling this method.
            let ProcessingTasks { started_at, processing } =
                &*self.processing_tasks.read().unwrap();
            return Ok(Some(Batch::TaskCancelation {
                task: self.get_task(rtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?,
                previous_started_at: *started_at,
                previous_processing_tasks: processing.clone(),
            }));
        }

        // 2. we get the next task to delete
        let to_delete = self.get_kind(rtxn, Kind::TaskDeletion)? & enqueued;
        if !to_delete.is_empty() {
            let tasks = self.get_existing_tasks(rtxn, to_delete)?;
            return Ok(Some(Batch::TaskDeletions(tasks)));
        }

        // 3. we batch the snapshot.
        let to_snapshot = self.get_kind(rtxn, Kind::SnapshotCreation)? & enqueued;
        if !to_snapshot.is_empty() {
            return Ok(Some(Batch::SnapshotCreation(self.get_existing_tasks(rtxn, to_snapshot)?)));
        }

        // 4. we batch the dumps.
        let to_dump = self.get_kind(rtxn, Kind::DumpCreation)? & enqueued;
        if let Some(to_dump) = to_dump.min() {
            return Ok(Some(Batch::Dump(
                self.get_task(rtxn, to_dump)?.ok_or(Error::CorruptedTaskQueue)?,
            )));
        }

        // 5. We make a batch from the unprioritised tasks. Start by taking the next enqueued task.
        let task_id = if let Some(task_id) = enqueued.min() { task_id } else { return Ok(None) };
        let task = self.get_task(rtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;

        // If the task is not associated with any index, verify that it is an index swap and
        // create the batch directly. Otherwise, get the index name associated with the task
        // and use the autobatcher to batch the enqueued tasks associated with it

        let index_name = if let Some(&index_name) = task.indexes().first() {
            index_name
        } else {
            assert!(matches!(&task.kind, KindWithContent::IndexSwap { swaps } if swaps.is_empty()));
            return Ok(Some(Batch::IndexSwap { task }));
        };

        let index_already_exists = self.index_mapper.exists(rtxn, index_name)?;
        let mut primary_key = None;
        if index_already_exists {
            let index = self.index_mapper.index(rtxn, index_name)?;
            let rtxn = index.read_txn()?;
            primary_key = index.primary_key(&rtxn)?.map(|pk| pk.to_string());
        }

        let index_tasks = self.index_tasks(rtxn, index_name)? & enqueued;

        // If autobatching is disabled we only take one task at a time.
        // Otherwise, we take only a maximum of tasks to create batches.
        let tasks_limit =
            if self.autobatching_enabled { self.max_number_of_batched_tasks } else { 1 };

        let enqueued = index_tasks
            .into_iter()
            .take(tasks_limit)
            .map(|task_id| {
                self.get_task(rtxn, task_id)
                    .and_then(|task| task.ok_or(Error::CorruptedTaskQueue))
                    .map(|task| (task.uid, task.kind))
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some((batchkind, create_index)) =
            autobatcher::autobatch(enqueued, index_already_exists, primary_key.as_deref())
        {
            return self.create_next_batch_index(
                rtxn,
                index_name.to_string(),
                batchkind,
                create_index,
            );
        }

        // If we found no tasks then we were notified for something that got autobatched
        // somehow and there is nothing to do.
        Ok(None)
    }

    /// Apply the operation associated with the given batch.
    ///
    /// ## Return
    /// The list of tasks that were processed. The metadata of each task in the returned
    /// list is updated accordingly, with the exception of the its date fields
    /// [`finished_at`](meilisearch_types::tasks::Task::finished_at) and [`started_at`](meilisearch_types::tasks::Task::started_at).
    #[tracing::instrument(level = "trace", skip(self, batch), target = "indexing::scheduler", fields(batch=batch.to_string()))]
    pub(crate) fn process_batch(&self, batch: Batch) -> Result<Vec<Task>> {
        #[cfg(test)]
        {
            self.maybe_fail(crate::tests::FailureLocation::InsideProcessBatch)?;
            self.maybe_fail(crate::tests::FailureLocation::PanicInsideProcessBatch)?;
            self.breakpoint(crate::Breakpoint::InsideProcessBatch);
        }

        match batch {
            Batch::TaskCancelation { mut task, previous_started_at, previous_processing_tasks } => {
                // 1. Retrieve the tasks that matched the query at enqueue-time.
                let matched_tasks =
                    if let KindWithContent::TaskCancelation { tasks, query: _ } = &task.kind {
                        tasks
                    } else {
                        unreachable!()
                    };

                let mut wtxn = self.env.write_txn()?;
                let canceled_tasks_content_uuids = self.cancel_matched_tasks(
                    &mut wtxn,
                    task.uid,
                    matched_tasks,
                    previous_started_at,
                    &previous_processing_tasks,
                )?;

                task.status = Status::Succeeded;
                match &mut task.details {
                    Some(Details::TaskCancelation {
                        matched_tasks: _,
                        canceled_tasks,
                        original_filter: _,
                    }) => {
                        *canceled_tasks = Some(canceled_tasks_content_uuids.len() as u64);
                    }
                    _ => unreachable!(),
                }

                // We must only remove the content files if the transaction is successfully committed
                // and if errors occurs when we are deleting files we must do our best to delete
                // everything. We do not return the encountered errors when deleting the content
                // files as it is not a breaking operation and we can safely continue our job.
                match wtxn.commit() {
                    Ok(()) => {
                        for content_uuid in canceled_tasks_content_uuids {
                            if let Err(error) = self.delete_update_file(content_uuid) {
                                tracing::error!(
                                    file_content_uuid = %content_uuid,
                                    %error,
                                    "Failed deleting content file"
                                )
                            }
                        }
                    }
                    Err(e) => return Err(e.into()),
                }

                Ok(vec![task])
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

                let mut wtxn = self.env.write_txn()?;
                let mut deleted_tasks = self.delete_matched_tasks(&mut wtxn, &matched_tasks)?;
                wtxn.commit()?;

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
                Ok(tasks)
            }
            Batch::SnapshotCreation(mut tasks) => {
                fs::create_dir_all(&self.snapshots_path)?;
                let temp_snapshot_dir = tempfile::tempdir()?;

                // 1. Snapshot the version file.
                let dst = temp_snapshot_dir.path().join(VERSION_FILE_NAME);
                fs::copy(&self.version_file_path, dst)?;

                // 2. Snapshot the index-scheduler LMDB env
                //
                // When we call copy_to_file, LMDB opens a read transaction by itself,
                // we can't provide our own. It is an issue as we would like to know
                // the update files to copy but new ones can be enqueued between the copy
                // of the env and the new transaction we open to retrieve the enqueued tasks.
                // So we prefer opening a new transaction after copying the env and copy more
                // update files than not enough.
                //
                // Note that there cannot be any update files deleted between those
                // two read operations as the task processing is synchronous.

                // 2.1 First copy the LMDB env of the index-scheduler
                let dst = temp_snapshot_dir.path().join("tasks");
                fs::create_dir_all(&dst)?;
                self.env.copy_to_file(dst.join("data.mdb"), CompactionOption::Enabled)?;

                // 2.2 Create a read transaction on the index-scheduler
                let rtxn = self.env.read_txn()?;

                // 2.3 Create the update files directory
                let update_files_dir = temp_snapshot_dir.path().join("update_files");
                fs::create_dir_all(&update_files_dir)?;

                // 2.4 Only copy the update files of the enqueued tasks
                for task_id in self.get_status(&rtxn, Status::Enqueued)? {
                    let task = self.get_task(&rtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;
                    if let Some(content_uuid) = task.content_uuid() {
                        let src = self.file_store.get_update_path(content_uuid);
                        let dst = update_files_dir.join(content_uuid.to_string());
                        fs::copy(src, dst)?;
                    }
                }

                // 3. Snapshot every indexes
                for result in self.index_mapper.index_mapping.iter(&rtxn)? {
                    let (name, uuid) = result?;
                    let index = self.index_mapper.index(&rtxn, name)?;
                    let dst = temp_snapshot_dir.path().join("indexes").join(uuid.to_string());
                    fs::create_dir_all(&dst)?;
                    index.copy_to_file(dst.join("data.mdb"), CompactionOption::Enabled)?;
                }

                drop(rtxn);

                // 4. Snapshot the auth LMDB env
                let dst = temp_snapshot_dir.path().join("auth");
                fs::create_dir_all(&dst)?;
                // TODO We can't use the open_auth_store_env function here but we should
                let auth = unsafe {
                    milli::heed::EnvOpenOptions::new()
                        .map_size(1024 * 1024 * 1024) // 1 GiB
                        .max_dbs(2)
                        .open(&self.auth_path)
                }?;
                auth.copy_to_file(dst.join("data.mdb"), CompactionOption::Enabled)?;

                // 5. Copy and tarball the flat snapshot
                // 5.1 Find the original name of the database
                // TODO find a better way to get this path
                let mut base_path = self.env.path().to_owned();
                base_path.pop();
                let db_name = base_path.file_name().and_then(OsStr::to_str).unwrap_or("data.ms");

                // 5.2 Tarball the content of the snapshot in a tempfile with a .snapshot extension
                let snapshot_path = self.snapshots_path.join(format!("{}.snapshot", db_name));
                let temp_snapshot_file = tempfile::NamedTempFile::new_in(&self.snapshots_path)?;
                compression::to_tar_gz(temp_snapshot_dir.path(), temp_snapshot_file.path())?;
                let file = temp_snapshot_file.persist(snapshot_path)?;

                // 5.3 Change the permission to make the snapshot readonly
                let mut permissions = file.metadata()?.permissions();
                permissions.set_readonly(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    #[allow(clippy::non_octal_unix_permissions)]
                    //                     rwxrwxrwx
                    permissions.set_mode(0b100100100);
                }

                file.set_permissions(permissions)?;

                for task in &mut tasks {
                    task.status = Status::Succeeded;
                }

                Ok(tasks)
            }
            Batch::Dump(mut task) => {
                let started_at = OffsetDateTime::now_utc();
                let (keys, instance_uid) =
                    if let KindWithContent::DumpCreation { keys, instance_uid } = &task.kind {
                        (keys, instance_uid)
                    } else {
                        unreachable!();
                    };
                let dump = dump::DumpWriter::new(*instance_uid)?;

                // 1. dump the keys
                let mut dump_keys = dump.create_keys()?;
                for key in keys {
                    dump_keys.push_key(key)?;
                }
                dump_keys.flush()?;

                let rtxn = self.env.read_txn()?;

                // 2. dump the tasks
                let mut dump_tasks = dump.create_tasks_queue()?;
                for ret in self.all_tasks.iter(&rtxn)? {
                    if self.must_stop_processing.get() {
                        return Err(Error::AbortedTask);
                    }

                    let (_, mut t) = ret?;
                    let status = t.status;
                    let content_file = t.content_uuid();

                    // In the case we're dumping ourselves we want to be marked as finished
                    // to not loop over ourselves indefinitely.
                    if t.uid == task.uid {
                        let finished_at = OffsetDateTime::now_utc();

                        // We're going to fake the date because we don't know if everything is going to go well.
                        // But we need to dump the task as finished and successful.
                        // If something fail everything will be set appropriately in the end.
                        t.status = Status::Succeeded;
                        t.started_at = Some(started_at);
                        t.finished_at = Some(finished_at);
                    }
                    let mut dump_content_file = dump_tasks.push_task(&t.into())?;

                    // 2.1. Dump the `content_file` associated with the task if there is one and the task is not finished yet.
                    if let Some(content_file) = content_file {
                        if self.must_stop_processing.get() {
                            return Err(Error::AbortedTask);
                        }
                        if status == Status::Enqueued {
                            let content_file = self.file_store.get_update(content_file)?;

                            let reader = DocumentsBatchReader::from_reader(content_file)
                                .map_err(milli::Error::from)?;

                            let (mut cursor, documents_batch_index) =
                                reader.into_cursor_and_fields_index();

                            while let Some(doc) =
                                cursor.next_document().map_err(milli::Error::from)?
                            {
                                dump_content_file.push_document(&obkv_to_object(
                                    &doc,
                                    &documents_batch_index,
                                )?)?;
                            }
                            dump_content_file.flush()?;
                        }
                    }
                }
                dump_tasks.flush()?;

                // 3. Dump the indexes
                self.index_mapper.try_for_each_index(&rtxn, |uid, index| -> Result<()> {
                    let rtxn = index.read_txn()?;
                    let metadata = IndexMetadata {
                        uid: uid.to_owned(),
                        primary_key: index.primary_key(&rtxn)?.map(String::from),
                        created_at: index.created_at(&rtxn)?,
                        updated_at: index.updated_at(&rtxn)?,
                    };
                    let mut index_dumper = dump.create_index(uid, &metadata)?;

                    let fields_ids_map = index.fields_ids_map(&rtxn)?;
                    let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();
                    let embedding_configs = index.embedding_configs(&rtxn)?;

                    // 3.1. Dump the documents
                    for ret in index.all_documents(&rtxn)? {
                        if self.must_stop_processing.get() {
                            return Err(Error::AbortedTask);
                        }

                        let (id, doc) = ret?;

                        let mut document = milli::obkv_to_json(&all_fields, &fields_ids_map, doc)?;

                        'inject_vectors: {
                            let embeddings = index.embeddings(&rtxn, id)?;

                            if embeddings.is_empty() {
                                break 'inject_vectors;
                            }

                            let vectors = document
                                .entry(RESERVED_VECTORS_FIELD_NAME.to_owned())
                                .or_insert(serde_json::Value::Object(Default::default()));

                            let serde_json::Value::Object(vectors) = vectors else {
                                return Err(milli::Error::UserError(
                                    milli::UserError::InvalidVectorsMapType {
                                        document_id: {
                                            if let Ok(Some(Ok(index))) = index
                                                .external_id_of(&rtxn, std::iter::once(id))
                                                .map(|it| it.into_iter().next())
                                            {
                                                index
                                            } else {
                                                format!("internal docid={id}")
                                            }
                                        },
                                        value: vectors.clone(),
                                    },
                                )
                                .into());
                            };

                            for (embedder_name, embeddings) in embeddings {
                                let user_provided = embedding_configs
                                    .iter()
                                    .find(|conf| conf.name == embedder_name)
                                    .is_some_and(|conf| conf.user_provided.contains(id));

                                let embeddings = ExplicitVectors {
                                    embeddings: Some(
                                        VectorOrArrayOfVectors::from_array_of_vectors(embeddings),
                                    ),
                                    regenerate: !user_provided,
                                };
                                vectors.insert(
                                    embedder_name,
                                    serde_json::to_value(embeddings).unwrap(),
                                );
                            }
                        }

                        index_dumper.push_document(&document)?;
                    }

                    // 3.2. Dump the settings
                    let settings = meilisearch_types::settings::settings(
                        index,
                        &rtxn,
                        meilisearch_types::settings::SecretPolicy::RevealSecrets,
                    )?;
                    index_dumper.settings(&settings)?;
                    Ok(())
                })?;

                // 4. Dump experimental feature settings
                let features = self.features().runtime_features();
                dump.create_experimental_features(features)?;

                let dump_uid = started_at.format(format_description!(
                    "[year repr:full][month repr:numerical][day padding:zero]-[hour padding:zero][minute padding:zero][second padding:zero][subsecond digits:3]"
                )).unwrap();

                if self.must_stop_processing.get() {
                    return Err(Error::AbortedTask);
                }
                let path = self.dumps_path.join(format!("{}.dump", dump_uid));
                let file = File::create(path)?;
                dump.persist_to(BufWriter::new(file))?;

                // if we reached this step we can tell the scheduler we succeeded to dump ourselves.
                task.status = Status::Succeeded;
                task.details = Some(Details::Dump { dump_uid: Some(dump_uid) });
                Ok(vec![task])
            }
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

                // the index operation can take a long time, so save this handle to make it available to the search for the duration of the tick
                self.index_mapper
                    .set_currently_updating_index(Some((index_uid.clone(), index.clone())));

                let mut index_wtxn = index.write_txn()?;
                let tasks = self.apply_index_operation(&mut index_wtxn, &index, op)?;
                index_wtxn.commit()?;

                // if the update processed successfully, we're going to store the new
                // stats of the index. Since the tasks have already been processed and
                // this is a non-critical operation. If it fails, we should not fail
                // the entire batch.
                let res = || -> Result<()> {
                    let index_rtxn = index.read_txn()?;
                    let stats = crate::index_mapper::IndexStats::new(&index, &index_rtxn)?;
                    let mut wtxn = self.env.write_txn()?;
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

                Ok(tasks)
            }
            Batch::IndexCreation { index_uid, primary_key, task } => {
                let wtxn = self.env.write_txn()?;
                if self.index_mapper.exists(&wtxn, &index_uid)? {
                    return Err(Error::IndexAlreadyExists(index_uid));
                }
                self.index_mapper.create_index(wtxn, &index_uid, None)?;

                self.process_batch(Batch::IndexUpdate { index_uid, primary_key, task })
            }
            Batch::IndexUpdate { index_uid, primary_key, mut task } => {
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
                    let must_stop_processing = self.must_stop_processing.clone();
                    builder.execute(
                        |indexing_step| tracing::debug!(update = ?indexing_step),
                        || must_stop_processing.get(),
                    )?;
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
                    let stats = crate::index_mapper::IndexStats::new(&index, &index_rtxn)?;
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

                Ok(vec![task])
            }
            Batch::IndexDeletion { index_uid, index_has_been_created, mut tasks } => {
                let wtxn = self.env.write_txn()?;

                // it's possible that the index doesn't exist
                let number_of_documents = || -> Result<u64> {
                    let index = self.index_mapper.index(&wtxn, &index_uid)?;
                    let index_rtxn = index.read_txn()?;
                    Ok(index.number_of_documents(&index_rtxn)?)
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

                Ok(tasks)
            }
            Batch::IndexSwap { mut task } => {
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
                for swap in swaps {
                    self.apply_index_swap(&mut wtxn, task.uid, &swap.indexes.0, &swap.indexes.1)?;
                }
                wtxn.commit()?;
                task.status = Status::Succeeded;
                Ok(vec![task])
            }
        }
    }

    /// Swap the index `lhs` with the index `rhs`.
    fn apply_index_swap(&self, wtxn: &mut RwTxn, task_id: u32, lhs: &str, rhs: &str) -> Result<()> {
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
        let mut index_lhs_task_ids = self.index_tasks(wtxn, lhs)?;
        index_lhs_task_ids.remove_range(task_id..);
        let mut index_rhs_task_ids = self.index_tasks(wtxn, rhs)?;
        index_rhs_task_ids.remove_range(task_id..);

        // 3. before_name -> new_name in the task's KindWithContent
        for task_id in &index_lhs_task_ids | &index_rhs_task_ids {
            let mut task = self.get_task(wtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;
            swap_index_uid_in_task(&mut task, (lhs, rhs));
            self.all_tasks.put(wtxn, &task_id, &task)?;
        }

        // 4. remove the task from indexuid = before_name
        // 5. add the task to indexuid = after_name
        self.update_index(wtxn, lhs, |lhs_tasks| {
            *lhs_tasks -= &index_lhs_task_ids;
            *lhs_tasks |= &index_rhs_task_ids;
        })?;
        self.update_index(wtxn, rhs, |rhs_tasks| {
            *rhs_tasks -= &index_rhs_task_ids;
            *rhs_tasks |= &index_lhs_task_ids;
        })?;

        // 6. Swap in the index mapper
        self.index_mapper.swap(wtxn, lhs, rhs)?;

        Ok(())
    }

    /// Process the index operation on the given index.
    ///
    /// ## Return
    /// The list of processed tasks.
    #[tracing::instrument(
        level = "trace",
        skip(self, index_wtxn, index),
        target = "indexing::scheduler"
    )]
    fn apply_index_operation<'i>(
        &self,
        index_wtxn: &mut RwTxn<'i>,
        index: &'i Index,
        operation: IndexOperation,
    ) -> Result<Vec<Task>> {
        match operation {
            IndexOperation::DocumentClear { mut tasks, .. } => {
                let count = milli::update::ClearDocuments::new(index_wtxn, index).execute()?;

                let mut first_clear_found = false;
                for task in &mut tasks {
                    task.status = Status::Succeeded;
                    // The first document clear will effectively delete every documents
                    // in the database but the next ones will clear 0 documents.
                    task.details = match &task.kind {
                        KindWithContent::DocumentClear { .. } => {
                            let count = if first_clear_found { 0 } else { count };
                            first_clear_found = true;
                            Some(Details::ClearAll { deleted_documents: Some(count) })
                        }
                        otherwise => otherwise.default_details(),
                    };
                }

                Ok(tasks)
            }
            IndexOperation::DocumentOperation {
                index_uid: _,
                primary_key,
                method,
                documents_counts: _,
                operations,
                mut tasks,
            } => {
                let mut primary_key_has_been_set = false;
                let must_stop_processing = self.must_stop_processing.clone();
                let indexer_config = self.index_mapper.indexer_config();

                if let Some(primary_key) = primary_key {
                    match index.primary_key(index_wtxn)? {
                        // if a primary key was set AND had already been defined in the index
                        // but to a different value, we can make the whole batch fail.
                        Some(pk) => {
                            if primary_key != pk {
                                return Err(milli::Error::from(
                                    milli::UserError::PrimaryKeyCannotBeChanged(pk.to_string()),
                                )
                                .into());
                            }
                        }
                        // if the primary key was set and there was no primary key set for this index
                        // we set it to the received value before starting the indexing process.
                        None => {
                            let mut builder =
                                milli::update::Settings::new(index_wtxn, index, indexer_config);
                            builder.set_primary_key(primary_key);
                            builder.execute(
                                |indexing_step| tracing::debug!(update = ?indexing_step),
                                || must_stop_processing.clone().get(),
                            )?;
                            primary_key_has_been_set = true;
                        }
                    }
                }

                let config = IndexDocumentsConfig { update_method: method, ..Default::default() };

                let embedder_configs = index.embedding_configs(index_wtxn)?;
                // TODO: consider Arc'ing the map too (we only need read access + we'll be cloning it multiple times, so really makes sense)
                let embedders = self.embedders(embedder_configs)?;

                let mut builder = milli::update::IndexDocuments::new(
                    index_wtxn,
                    index,
                    indexer_config,
                    config,
                    |indexing_step| tracing::trace!(?indexing_step, "Update"),
                    || must_stop_processing.get(),
                )?;

                for (operation, task) in operations.into_iter().zip(tasks.iter_mut()) {
                    match operation {
                        DocumentOperation::Add(content_uuid) => {
                            let content_file = self.file_store.get_update(content_uuid)?;
                            let reader = DocumentsBatchReader::from_reader(content_file)
                                .map_err(milli::Error::from)?;
                            let (new_builder, user_result) = builder.add_documents(reader)?;
                            builder = new_builder;

                            builder = builder.with_embedders(embedders.clone());

                            let received_documents =
                                if let Some(Details::DocumentAdditionOrUpdate {
                                    received_documents,
                                    ..
                                }) = task.details
                                {
                                    received_documents
                                } else {
                                    // In the case of a `documentAdditionOrUpdate` the details MUST be set
                                    unreachable!();
                                };

                            match user_result {
                                Ok(count) => {
                                    task.status = Status::Succeeded;
                                    task.details = Some(Details::DocumentAdditionOrUpdate {
                                        received_documents,
                                        indexed_documents: Some(count),
                                    })
                                }
                                Err(e) => {
                                    task.status = Status::Failed;
                                    task.details = Some(Details::DocumentAdditionOrUpdate {
                                        received_documents,
                                        indexed_documents: Some(0),
                                    });
                                    task.error = Some(milli::Error::from(e).into());
                                }
                            }
                        }
                        DocumentOperation::Delete(document_ids) => {
                            let (new_builder, user_result) =
                                builder.remove_documents(document_ids)?;
                            builder = new_builder;
                            // Uses Invariant: remove documents actually always returns Ok for the inner result
                            let count = user_result.unwrap();
                            let provided_ids =
                                if let Some(Details::DocumentDeletion { provided_ids, .. }) =
                                    task.details
                                {
                                    provided_ids
                                } else {
                                    // In the case of a `documentAdditionOrUpdate` the details MUST be set
                                    unreachable!();
                                };

                            task.status = Status::Succeeded;
                            task.details = Some(Details::DocumentDeletion {
                                provided_ids,
                                deleted_documents: Some(count),
                            });
                        }
                    }
                }

                if !tasks.iter().all(|res| res.error.is_some()) {
                    let addition = builder.execute()?;
                    tracing::info!(indexing_result = ?addition, "document indexing done");
                } else if primary_key_has_been_set {
                    // Everything failed but we've set a primary key.
                    // We need to remove it.
                    let mut builder =
                        milli::update::Settings::new(index_wtxn, index, indexer_config);
                    builder.reset_primary_key();
                    builder.execute(
                        |indexing_step| tracing::trace!(update = ?indexing_step),
                        || must_stop_processing.clone().get(),
                    )?;
                }

                Ok(tasks)
            }
            IndexOperation::DocumentEdition { mut task, .. } => {
                let (filter, edition_code) =
                    if let KindWithContent::DocumentEdition { filter_expr, edition_code, .. } =
                        &task.kind
                    {
                        (filter_expr, edition_code)
                    } else {
                        unreachable!()
                    };
                let edited_documents = edit_documents_by_function(
                    index_wtxn,
                    filter,
                    edition_code,
                    self.index_mapper.indexer_config(),
                    self.must_stop_processing.clone(),
                    index,
                );
                let (original_filter, edition_code) =
                    if let Some(Details::DocumentEdition {
                        original_filter, edition_code, ..
                    }) = task.details
                    {
                        (original_filter, edition_code)
                    } else {
                        // In the case of a `documentDeleteByFilter` the details MUST be set
                        unreachable!();
                    };

                match edited_documents {
                    Ok(edited_documents) => {
                        task.status = Status::Succeeded;
                        task.details = Some(Details::DocumentEdition {
                            original_filter,
                            edition_code,
                            edited_documents: Some(edited_documents),
                        });
                    }
                    Err(e) => {
                        task.status = Status::Failed;
                        task.details = Some(Details::DocumentEdition {
                            original_filter,
                            edition_code,
                            edited_documents: Some(0),
                        });
                        task.error = Some(e.into());
                    }
                }

                Ok(vec![task])
            }
            IndexOperation::IndexDocumentDeletionByFilter { mut task, index_uid: _ } => {
                let filter =
                    if let KindWithContent::DocumentDeletionByFilter { filter_expr, .. } =
                        &task.kind
                    {
                        filter_expr
                    } else {
                        unreachable!()
                    };
                let deleted_documents = delete_document_by_filter(
                    index_wtxn,
                    filter,
                    self.index_mapper.indexer_config(),
                    self.must_stop_processing.clone(),
                    index,
                );
                let original_filter = if let Some(Details::DocumentDeletionByFilter {
                    original_filter,
                    deleted_documents: _,
                }) = task.details
                {
                    original_filter
                } else {
                    // In the case of a `documentDeleteByFilter` the details MUST be set
                    unreachable!();
                };

                match deleted_documents {
                    Ok(deleted_documents) => {
                        task.status = Status::Succeeded;
                        task.details = Some(Details::DocumentDeletionByFilter {
                            original_filter,
                            deleted_documents: Some(deleted_documents),
                        });
                    }
                    Err(e) => {
                        task.status = Status::Failed;
                        task.details = Some(Details::DocumentDeletionByFilter {
                            original_filter,
                            deleted_documents: Some(0),
                        });
                        task.error = Some(e.into());
                    }
                }

                Ok(vec![task])
            }
            IndexOperation::Settings { index_uid: _, settings, mut tasks } => {
                let indexer_config = self.index_mapper.indexer_config();
                let mut builder = milli::update::Settings::new(index_wtxn, index, indexer_config);

                for (task, (_, settings)) in tasks.iter_mut().zip(settings) {
                    let checked_settings = settings.clone().check();
                    task.details = Some(Details::SettingsUpdate { settings: Box::new(settings) });
                    apply_settings_to_builder(&checked_settings, &mut builder);

                    // We can apply the status right now and if an update fail later
                    // the whole batch will be marked as failed.
                    task.status = Status::Succeeded;
                }

                let must_stop_processing = self.must_stop_processing.clone();
                builder.execute(
                    |indexing_step| tracing::debug!(update = ?indexing_step),
                    || must_stop_processing.get(),
                )?;

                Ok(tasks)
            }
            IndexOperation::SettingsAndDocumentOperation {
                index_uid,
                primary_key,
                method,
                documents_counts,
                operations,
                document_import_tasks,
                settings,
                settings_tasks,
            } => {
                let settings_tasks = self.apply_index_operation(
                    index_wtxn,
                    index,
                    IndexOperation::Settings {
                        index_uid: index_uid.clone(),
                        settings,
                        tasks: settings_tasks,
                    },
                )?;

                let mut import_tasks = self.apply_index_operation(
                    index_wtxn,
                    index,
                    IndexOperation::DocumentOperation {
                        index_uid,
                        primary_key,
                        method,
                        documents_counts,
                        operations,
                        tasks: document_import_tasks,
                    },
                )?;

                let mut tasks = settings_tasks;
                tasks.append(&mut import_tasks);
                Ok(tasks)
            }
            IndexOperation::DocumentClearAndSetting {
                index_uid,
                cleared_tasks,
                settings,
                settings_tasks,
            } => {
                let mut import_tasks = self.apply_index_operation(
                    index_wtxn,
                    index,
                    IndexOperation::DocumentClear {
                        index_uid: index_uid.clone(),
                        tasks: cleared_tasks,
                    },
                )?;

                let settings_tasks = self.apply_index_operation(
                    index_wtxn,
                    index,
                    IndexOperation::Settings { index_uid, settings, tasks: settings_tasks },
                )?;

                let mut tasks = settings_tasks;
                tasks.append(&mut import_tasks);
                Ok(tasks)
            }
        }
    }

    /// Delete each given task from all the databases (if it is deleteable).
    ///
    /// Return the number of tasks that were actually deleted.
    fn delete_matched_tasks(
        &self,
        wtxn: &mut RwTxn,
        matched_tasks: &RoaringBitmap,
    ) -> Result<RoaringBitmap> {
        // 1. Remove from this list the tasks that we are not allowed to delete
        let enqueued_tasks = self.get_status(wtxn, Status::Enqueued)?;
        let processing_tasks = &self.processing_tasks.read().unwrap().processing.clone();

        let all_task_ids = self.all_task_ids(wtxn)?;
        let mut to_delete_tasks = all_task_ids & matched_tasks;
        to_delete_tasks -= processing_tasks;
        to_delete_tasks -= enqueued_tasks;

        // 2. We now have a list of tasks to delete, delete them

        let mut affected_indexes = HashSet::new();
        let mut affected_statuses = HashSet::new();
        let mut affected_kinds = HashSet::new();
        let mut affected_canceled_by = RoaringBitmap::new();

        for task_id in to_delete_tasks.iter() {
            let task = self.get_task(wtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;

            affected_indexes.extend(task.indexes().into_iter().map(|x| x.to_owned()));
            affected_statuses.insert(task.status);
            affected_kinds.insert(task.kind.as_kind());
            // Note: don't delete the persisted task data since
            // we can only delete succeeded, failed, and canceled tasks.
            // In each of those cases, the persisted data is supposed to
            // have been deleted already.
            utils::remove_task_datetime(wtxn, self.enqueued_at, task.enqueued_at, task.uid)?;
            if let Some(started_at) = task.started_at {
                utils::remove_task_datetime(wtxn, self.started_at, started_at, task.uid)?;
            }
            if let Some(finished_at) = task.finished_at {
                utils::remove_task_datetime(wtxn, self.finished_at, finished_at, task.uid)?;
            }
            if let Some(canceled_by) = task.canceled_by {
                affected_canceled_by.insert(canceled_by);
            }
        }

        for index in affected_indexes {
            self.update_index(wtxn, &index, |bitmap| *bitmap -= &to_delete_tasks)?;
        }

        for status in affected_statuses {
            self.update_status(wtxn, status, |bitmap| *bitmap -= &to_delete_tasks)?;
        }

        for kind in affected_kinds {
            self.update_kind(wtxn, kind, |bitmap| *bitmap -= &to_delete_tasks)?;
        }

        for task in to_delete_tasks.iter() {
            self.all_tasks.delete(wtxn, &task)?;
        }
        for canceled_by in affected_canceled_by {
            if let Some(mut tasks) = self.canceled_by.get(wtxn, &canceled_by)? {
                tasks -= &to_delete_tasks;
                if tasks.is_empty() {
                    self.canceled_by.delete(wtxn, &canceled_by)?;
                } else {
                    self.canceled_by.put(wtxn, &canceled_by, &tasks)?;
                }
            }
        }

        Ok(to_delete_tasks)
    }

    /// Cancel each given task from all the databases (if it is cancelable).
    ///
    /// Returns the content files that the transaction owner must delete if the commit is successful.
    fn cancel_matched_tasks(
        &self,
        wtxn: &mut RwTxn,
        cancel_task_id: TaskId,
        matched_tasks: &RoaringBitmap,
        previous_started_at: OffsetDateTime,
        previous_processing_tasks: &RoaringBitmap,
    ) -> Result<Vec<Uuid>> {
        let now = OffsetDateTime::now_utc();

        // 1. Remove from this list the tasks that we are not allowed to cancel
        //    Notice that only the _enqueued_ ones are cancelable and we should
        //    have already aborted the indexation of the _processing_ ones
        let cancelable_tasks = self.get_status(wtxn, Status::Enqueued)?;
        let tasks_to_cancel = cancelable_tasks & matched_tasks;

        // 2. We now have a list of tasks to cancel, cancel them
        let mut content_files_to_delete = Vec::new();
        for mut task in self.get_existing_tasks(wtxn, tasks_to_cancel.iter())? {
            if let Some(uuid) = task.content_uuid() {
                content_files_to_delete.push(uuid);
            }
            if previous_processing_tasks.contains(task.uid) {
                task.started_at = Some(previous_started_at);
            }
            task.status = Status::Canceled;
            task.canceled_by = Some(cancel_task_id);
            task.finished_at = Some(now);
            task.details = task.details.map(|d| d.to_failed());
            self.update_task(wtxn, &task)?;
        }
        self.canceled_by.put(wtxn, &cancel_task_id, &tasks_to_cancel)?;

        Ok(content_files_to_delete)
    }
}

fn delete_document_by_filter<'a>(
    wtxn: &mut RwTxn<'a>,
    filter: &serde_json::Value,
    indexer_config: &IndexerConfig,
    must_stop_processing: MustStopProcessing,
    index: &'a Index,
) -> Result<u64> {
    let filter = Filter::from_json(filter)?;
    Ok(if let Some(filter) = filter {
        let candidates = filter.evaluate(wtxn, index).map_err(|err| match err {
            milli::Error::UserError(milli::UserError::InvalidFilter(_)) => {
                Error::from(err).with_custom_error_code(Code::InvalidDocumentFilter)
            }
            e => e.into(),
        })?;

        let config = IndexDocumentsConfig {
            update_method: IndexDocumentsMethod::ReplaceDocuments,
            ..Default::default()
        };

        let mut builder = milli::update::IndexDocuments::new(
            wtxn,
            index,
            indexer_config,
            config,
            |indexing_step| tracing::debug!(update = ?indexing_step),
            || must_stop_processing.get(),
        )?;

        let (new_builder, count) = builder.remove_documents_from_db_no_batch(&candidates)?;
        builder = new_builder;

        let _ = builder.execute()?;
        count
    } else {
        0
    })
}

fn edit_documents_by_function<'a>(
    wtxn: &mut RwTxn<'a>,
    filter: &serde_json::Value,
    code: &str,
    indexer_config: &IndexerConfig,
    must_stop_processing: MustStopProcessing,
    index: &'a Index,
) -> Result<u64> {
    let filter = Filter::from_json(filter)?;
    Ok(if let Some(filter) = filter {
        let candidates = filter.evaluate(wtxn, index).map_err(|err| match err {
            milli::Error::UserError(milli::UserError::InvalidFilter(_)) => {
                Error::from(err).with_custom_error_code(Code::InvalidDocumentFilter)
            }
            e => e.into(),
        })?;

        let config = IndexDocumentsConfig {
            update_method: IndexDocumentsMethod::ReplaceDocuments,
            ..Default::default()
        };

        let mut builder = milli::update::IndexDocuments::new(
            wtxn,
            index,
            indexer_config,
            config,
            |indexing_step| tracing::debug!(update = ?indexing_step),
            || must_stop_processing.get(),
        )?;

        todo!("edit documents with the code and reinsert them in the builder")
        // let (new_builder, count) = builder.remove_documents_from_db_no_batch(&candidates)?;
        // builder = new_builder;

        // let _ = builder.execute()?;
        // count
    } else {
        0
    })
}

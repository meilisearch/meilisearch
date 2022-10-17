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
tasks individally, but should be much faster since we are only performing
one indexing operation.
*/

use crate::{autobatcher::AutoBatch, Error, IndexScheduler, Result, TaskId};
use log::{debug, info};
use meilisearch_types::milli::update::IndexDocumentsConfig;
use meilisearch_types::milli::update::{
    DocumentAdditionResult, DocumentDeletionResult, IndexDocumentsMethod,
};
use meilisearch_types::milli::{
    self, documents::DocumentsBatchReader, update::Settings as MilliSettings, BEU32,
};
use meilisearch_types::settings::{apply_settings_to_builder, Settings, Unchecked};
use meilisearch_types::tasks::{Details, Kind, Status, Task, TaskOperation};
use meilisearch_types::{
    heed::{RoTxn, RwTxn},
    Index,
};
use roaring::RoaringBitmap;
use std::collections::HashSet;
use uuid::Uuid;

/// Represents a combination of tasks that can all be processed at the same time.
///
/// A batch contains the set of tasks that it represents (accessible through
/// [`self.ids()`](Batch::ids)), as well as additional information on how to
/// be processed.
#[derive(Debug)]
pub(crate) enum Batch {
    Cancel(Task),
    TaskDeletion(Task),
    Snapshot(Vec<Task>),
    Dump(Vec<Task>),
    IndexOperation(IndexOperation),
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
    },
}

/// A [batch](Batch) that combines multiple tasks operating on an index.
#[derive(Debug)]
pub(crate) enum IndexOperation {
    DocumentImport {
        index_uid: String,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        allow_index_creation: bool,
        documents_counts: Vec<u64>,
        content_files: Vec<Uuid>,
        tasks: Vec<Task>,
    },
    DocumentDeletion {
        index_uid: String,
        documents: Vec<String>,
        tasks: Vec<Task>,
    },
    DocumentClear {
        index_uid: String,
        tasks: Vec<Task>,
    },
    Settings {
        index_uid: String,
        // TODO what's that boolean, does it mean that it removes things or what?
        settings: Vec<(bool, Settings<Unchecked>)>,
        allow_index_creation: bool,
        tasks: Vec<Task>,
    },
    DocumentClearAndSetting {
        index_uid: String,
        cleared_tasks: Vec<Task>,

        // TODO what's that boolean, does it mean that it removes things or what?
        settings: Vec<(bool, Settings<Unchecked>)>,
        allow_index_creation: bool,
        settings_tasks: Vec<Task>,
    },
    SettingsAndDocumentImport {
        index_uid: String,

        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        allow_index_creation: bool,
        documents_counts: Vec<u64>,
        content_files: Vec<Uuid>,
        document_import_tasks: Vec<Task>,

        // TODO what's that boolean, does it mean that it removes things or what?
        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<Task>,
    },
}

impl Batch {
    /// Return the task ids associated with this batch.
    pub fn ids(&self) -> Vec<TaskId> {
        match self {
            Batch::Cancel(task)
            | Batch::TaskDeletion(task)
            | Batch::IndexCreation { task, .. }
            | Batch::IndexUpdate { task, .. } => vec![task.uid],
            Batch::Snapshot(tasks) | Batch::Dump(tasks) | Batch::IndexDeletion { tasks, .. } => {
                tasks.iter().map(|task| task.uid).collect()
            }
            Batch::IndexOperation(operation) => match operation {
                IndexOperation::DocumentImport { tasks, .. }
                | IndexOperation::DocumentDeletion { tasks, .. }
                | IndexOperation::Settings { tasks, .. }
                | IndexOperation::DocumentClear { tasks, .. } => {
                    tasks.iter().map(|task| task.uid).collect()
                }
                IndexOperation::SettingsAndDocumentImport {
                    document_import_tasks: tasks,
                    settings_tasks: other,
                    ..
                }
                | IndexOperation::DocumentClearAndSetting {
                    cleared_tasks: tasks,
                    settings_tasks: other,
                    ..
                } => tasks.iter().chain(other).map(|task| task.uid).collect(),
            },
        }
    }
}

impl IndexScheduler {
    /// Convert an [`AutoBatch`](crate::autobatcher::AutoBatch) into a [`Batch`].
    ///
    /// ## Arguments
    /// - `rtxn`: read transaction
    /// - `index_uid`: name of the index affected by the operations of the autobatch
    /// - `batch`: the result of the autobatcher
    pub(crate) fn batch_from_autobatch(
        &self,
        rtxn: &RoTxn,
        index_uid: String,
        batch: AutoBatch,
    ) -> Result<Option<Batch>> {
        match batch {
            AutoBatch::DocumentClear { ids } => {
                Ok(Some(Batch::IndexOperation(IndexOperation::DocumentClear {
                    tasks: self.get_existing_tasks(rtxn, ids)?,
                    index_uid,
                })))
            }
            AutoBatch::DocumentImport {
                method,
                import_ids,
                allow_index_creation,
            } => {
                let tasks = self.get_existing_tasks(rtxn, import_ids)?;
                let primary_key = match &tasks[0].operation {
                    TaskOperation::DocumentImport { primary_key, .. } => primary_key.clone(),
                    _ => unreachable!(),
                };

                let mut documents_counts = Vec::new();
                let mut content_files = Vec::new();
                for task in &tasks {
                    match task.operation {
                        TaskOperation::DocumentImport {
                            content_file,
                            documents_count,
                            ..
                        } => {
                            documents_counts.push(documents_count);
                            content_files.push(content_file);
                        }
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::IndexOperation(
                    IndexOperation::DocumentImport {
                        index_uid,
                        primary_key,
                        method,
                        allow_index_creation,
                        documents_counts,
                        content_files,
                        tasks,
                    },
                )))
            }
            AutoBatch::DocumentDeletion { deletion_ids } => {
                let tasks = self.get_existing_tasks(rtxn, deletion_ids)?;

                let mut documents = Vec::new();
                for task in &tasks {
                    match task.operation {
                        TaskOperation::DocumentDeletion {
                            ref documents_ids, ..
                        } => documents.extend_from_slice(documents_ids),
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::IndexOperation(
                    IndexOperation::DocumentDeletion {
                        index_uid,
                        documents,
                        tasks,
                    },
                )))
            }
            AutoBatch::Settings {
                settings_ids,
                allow_index_creation,
            } => {
                let tasks = self.get_existing_tasks(rtxn, settings_ids)?;

                let mut settings = Vec::new();
                for task in &tasks {
                    match task.operation {
                        TaskOperation::Settings {
                            ref new_settings,
                            is_deletion,
                            ..
                        } => settings.push((is_deletion, new_settings.clone())),
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::IndexOperation(IndexOperation::Settings {
                    index_uid,
                    settings,
                    allow_index_creation,
                    tasks,
                })))
            }
            AutoBatch::ClearAndSettings {
                other,
                settings_ids,
                allow_index_creation,
            } => {
                let (index_uid, settings, settings_tasks) = match self
                    .batch_from_autobatch(
                        rtxn,
                        index_uid,
                        AutoBatch::Settings {
                            settings_ids,
                            allow_index_creation,
                        },
                    )?
                    .unwrap()
                {
                    Batch::IndexOperation(IndexOperation::Settings {
                        index_uid,
                        settings,
                        tasks,
                        ..
                    }) => (index_uid, settings, tasks),
                    _ => unreachable!(),
                };
                let (index_uid, cleared_tasks) = match self
                    .batch_from_autobatch(rtxn, index_uid, AutoBatch::DocumentClear { ids: other })?
                    .unwrap()
                {
                    Batch::IndexOperation(IndexOperation::DocumentClear { index_uid, tasks }) => {
                        (index_uid, tasks)
                    }
                    _ => unreachable!(),
                };

                Ok(Some(Batch::IndexOperation(
                    IndexOperation::DocumentClearAndSetting {
                        index_uid,
                        cleared_tasks,
                        allow_index_creation,
                        settings,
                        settings_tasks,
                    },
                )))
            }
            AutoBatch::SettingsAndDocumentImport {
                settings_ids,
                method,
                allow_index_creation,
                import_ids,
            } => {
                let settings = self.batch_from_autobatch(
                    rtxn,
                    index_uid.clone(),
                    AutoBatch::Settings {
                        settings_ids,
                        allow_index_creation,
                    },
                )?;

                let document_import = self.batch_from_autobatch(
                    rtxn,
                    index_uid.clone(),
                    AutoBatch::DocumentImport {
                        method,
                        allow_index_creation,
                        import_ids,
                    },
                )?;

                match (document_import, settings) {
                    (
                        Some(Batch::IndexOperation(IndexOperation::DocumentImport {
                            primary_key,
                            documents_counts,
                            content_files,
                            tasks: document_import_tasks,
                            ..
                        })),
                        Some(Batch::IndexOperation(IndexOperation::Settings {
                            settings,
                            tasks: settings_tasks,
                            ..
                        })),
                    ) => Ok(Some(Batch::IndexOperation(
                        IndexOperation::SettingsAndDocumentImport {
                            index_uid,
                            primary_key,
                            method,
                            allow_index_creation,
                            documents_counts,
                            content_files,
                            document_import_tasks,
                            settings,
                            settings_tasks,
                        },
                    ))),
                    _ => unreachable!(),
                }
            }
            AutoBatch::IndexCreation { id } => {
                let task = self.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                let (index_uid, primary_key) = match &task.operation {
                    TaskOperation::IndexCreation {
                        index_uid,
                        primary_key,
                    } => (index_uid.clone(), primary_key.clone()),
                    _ => unreachable!(),
                };
                Ok(Some(Batch::IndexCreation {
                    index_uid,
                    primary_key,
                    task,
                }))
            }
            AutoBatch::IndexUpdate { id } => {
                let task = self.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                let primary_key = match &task.operation {
                    TaskOperation::IndexUpdate { primary_key, .. } => primary_key.clone(),
                    _ => unreachable!(),
                };
                Ok(Some(Batch::IndexUpdate {
                    index_uid,
                    primary_key,
                    task,
                }))
            }
            AutoBatch::IndexDeletion { ids } => Ok(Some(Batch::IndexDeletion {
                index_uid,
                tasks: self.get_existing_tasks(rtxn, ids)?,
            })),
            AutoBatch::IndexSwap { id: _ } => todo!(),
        }
    }

    /// Create the next batch to be processed;
    /// 1. We get the *last* task to cancel.
    /// 2. We get the *next* task to delete.
    /// 3. We get the *next* snapshot to process.
    /// 4. We get the *next* dump to process.
    /// 5. We get the *next* tasks to process for a specific index.
    pub(crate) fn create_next_batch(&self, rtxn: &RoTxn) -> Result<Option<Batch>> {
        let enqueued = &self.get_status(rtxn, Status::Enqueued)?;
        let to_cancel = self.get_kind(rtxn, Kind::CancelTask)? & enqueued;

        // 1. we get the last task to cancel.
        if let Some(task_id) = to_cancel.max() {
            return Ok(Some(Batch::Cancel(
                self.get_task(rtxn, task_id)?
                    .ok_or(Error::CorruptedTaskQueue)?,
            )));
        }

        // 2. we get the next task to delete
        let to_delete = self.get_kind(rtxn, Kind::TaskDeletion)? & enqueued;
        if let Some(task_id) = to_delete.min() {
            let task = self
                .get_task(rtxn, task_id)?
                .ok_or(Error::CorruptedTaskQueue)?;

            return Ok(Some(Batch::TaskDeletion(task)));
        }

        // 3. we batch the snapshot.
        let to_snapshot = self.get_kind(rtxn, Kind::Snapshot)? & enqueued;
        if !to_snapshot.is_empty() {
            return Ok(Some(Batch::Snapshot(
                self.get_existing_tasks(rtxn, to_snapshot)?,
            )));
        }

        // 4. we batch the dumps.
        let to_dump = self.get_kind(rtxn, Kind::DumpExport)? & enqueued;
        if !to_dump.is_empty() {
            return Ok(Some(Batch::Dump(self.get_existing_tasks(rtxn, to_dump)?)));
        }

        // 5. We take the next task and try to batch all the tasks associated with this index.
        if let Some(task_id) = enqueued.min() {
            let task = self
                .get_task(rtxn, task_id)?
                .ok_or(Error::CorruptedTaskQueue)?;

            // This is safe because all the remaining task are associated with
            // AT LEAST one index. We can use the right or left one it doesn't
            // matter.
            let index_name = task.indexes().unwrap()[0];

            let index_tasks = self.index_tasks(rtxn, index_name)? & enqueued;

            // If autobatching is disabled we only take one task at a time.
            let tasks_limit = if self.autobatching_enabled {
                usize::MAX
            } else {
                1
            };

            let enqueued = index_tasks
                .into_iter()
                .take(tasks_limit)
                .map(|task_id| {
                    self.get_task(rtxn, task_id)
                        .and_then(|task| task.ok_or(Error::CorruptedTaskQueue))
                        .map(|task| (task.uid, task.operation))
                })
                .collect::<Result<Vec<_>>>()?;

            if let Some(batchkind) = crate::autobatcher::next_autobatch(enqueued) {
                return self.batch_from_autobatch(rtxn, index_name.to_string(), batchkind);
            }
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
    /// [`finished_at`](Task::finished_at) and [`started_at`](Task::started_at).
    pub(crate) fn process_batch(&self, batch: Batch) -> Result<Vec<Task>> {
        match batch {
            Batch::Cancel(_) => todo!(),
            Batch::TaskDeletion(mut task) => {
                // 1. Retrieve the tasks that matched the query at enqueue-time.
                let matched_tasks =
                    if let TaskOperation::TaskDeletion { tasks, query: _ } = &task.operation {
                        tasks
                    } else {
                        unreachable!()
                    };

                let mut wtxn = self.env.write_txn()?;
                let nbr_deleted_tasks = self.delete_matched_tasks(&mut wtxn, matched_tasks)?;

                task.status = Status::Succeeded;
                match &mut task.details {
                    Some(Details::TaskDeletion {
                        matched_tasks: _,
                        deleted_tasks,
                        original_query: _,
                    }) => {
                        *deleted_tasks = Some(nbr_deleted_tasks);
                    }
                    _ => unreachable!(),
                }

                wtxn.commit()?;
                Ok(vec![task])
            }
            Batch::Snapshot(_) => todo!(),
            Batch::Dump(_) => todo!(),
            Batch::IndexOperation(operation) => {
                #[rustfmt::skip]
                let index = match operation {
                    IndexOperation::DocumentDeletion { ref index_uid, .. }
                    | IndexOperation::DocumentClear { ref index_uid, .. } => {
                        // only get the index, don't create it
                        let rtxn = self.env.read_txn()?;
                        self.index_mapper.index(&rtxn, index_uid)?
                    }
                    IndexOperation::DocumentImport { ref index_uid, allow_index_creation, .. }
                    | IndexOperation::Settings { ref index_uid, allow_index_creation, .. }
                    | IndexOperation::DocumentClearAndSetting { ref index_uid, allow_index_creation, .. }
                    | IndexOperation::SettingsAndDocumentImport {ref index_uid, allow_index_creation, .. } => {
                        if allow_index_creation {
                            // create the index if it doesn't already exist
                            let mut wtxn = self.env.write_txn()?;
                            let index = self.index_mapper.create_index(&mut wtxn, index_uid)?;
                            wtxn.commit()?;
                            index
                        } else {
                            let rtxn = self.env.read_txn()?;
                            self.index_mapper.index(&rtxn, index_uid)?
                        }
                    }
                };

                let mut index_wtxn = index.write_txn()?;
                let tasks = self.apply_index_operation(&mut index_wtxn, &index, operation)?;
                index_wtxn.commit()?;

                Ok(tasks)
            }
            Batch::IndexCreation {
                index_uid,
                primary_key,
                task,
            } => {
                let mut wtxn = self.env.write_txn()?;
                self.index_mapper.create_index(&mut wtxn, &index_uid)?;
                wtxn.commit()?;

                self.process_batch(Batch::IndexUpdate {
                    index_uid,
                    primary_key,
                    task,
                })
            }
            Batch::IndexUpdate {
                index_uid,
                primary_key,
                mut task,
            } => {
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
                    builder.execute(|_| ())?;
                    index_wtxn.commit()?;
                }

                task.status = Status::Succeeded;
                task.details = Some(Details::IndexInfo { primary_key });

                Ok(vec![task])
            }
            Batch::IndexDeletion {
                index_uid,
                mut tasks,
            } => {
                let wtxn = self.env.write_txn()?;

                let number_of_documents = {
                    let index = self.index_mapper.index(&wtxn, &index_uid)?;
                    let index_rtxn = index.read_txn()?;
                    index.number_of_documents(&index_rtxn)?
                };

                // The write transaction is directly owned and commited inside.
                self.index_mapper.delete_index(wtxn, &index_uid)?;

                // We set all the tasks details to the default value.
                for task in &mut tasks {
                    task.status = Status::Succeeded;
                    task.details = match &task.operation {
                        TaskOperation::IndexDeletion { .. } => Some(Details::ClearAll {
                            deleted_documents: Some(number_of_documents),
                        }),
                        otherwise => otherwise.default_details(),
                    };
                }

                Ok(tasks)
            }
        }
    }

    /// Process the index operation on the given index.
    ///
    /// ## Return
    /// The list of processed tasks.
    fn apply_index_operation<'txn, 'i>(
        &self,
        index_wtxn: &'txn mut RwTxn<'i, '_>,
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
                    task.details = match &task.operation {
                        TaskOperation::DocumentClear { .. } => {
                            let count = if first_clear_found { 0 } else { count };
                            first_clear_found = true;
                            Some(Details::ClearAll {
                                deleted_documents: Some(count),
                            })
                        }
                        otherwise => otherwise.default_details(),
                    };
                }

                Ok(tasks)
            }
            IndexOperation::DocumentImport {
                index_uid: _,
                primary_key,
                method,
                allow_index_creation: _,
                documents_counts,
                content_files,
                mut tasks,
            } => {
                let indexer_config = self.index_mapper.indexer_config();
                // TODO use the code from the IndexCreate operation
                if let Some(primary_key) = primary_key {
                    if index.primary_key(index_wtxn)?.is_none() {
                        let mut builder =
                            milli::update::Settings::new(index_wtxn, index, indexer_config);
                        builder.set_primary_key(primary_key);
                        builder.execute(|_| ())?;
                    }
                }

                let config = IndexDocumentsConfig {
                    update_method: method,
                    ..Default::default()
                };

                let mut builder = milli::update::IndexDocuments::new(
                    index_wtxn,
                    index,
                    indexer_config,
                    config,
                    |indexing_step| debug!("update: {:?}", indexing_step),
                )?;

                let mut results = Vec::new();
                for content_uuid in content_files.into_iter() {
                    let content_file = self.file_store.get_update(content_uuid)?;
                    let reader = DocumentsBatchReader::from_reader(content_file)
                        .map_err(milli::Error::from)?;
                    let (new_builder, user_result) = builder.add_documents(reader)?;
                    builder = new_builder;

                    let user_result = match user_result {
                        Ok(count) => Ok(DocumentAdditionResult {
                            indexed_documents: count,
                            number_of_documents: count,
                        }),
                        Err(e) => Err(milli::Error::from(e)),
                    };

                    results.push(user_result);
                }

                if results.iter().any(|res| res.is_ok()) {
                    let addition = builder.execute()?;
                    info!("document addition done: {:?}", addition);
                }

                for (task, (ret, count)) in tasks
                    .iter_mut()
                    .zip(results.into_iter().zip(documents_counts))
                {
                    match ret {
                        Ok(DocumentAdditionResult {
                            indexed_documents,
                            number_of_documents,
                        }) => {
                            task.status = Status::Succeeded;
                            task.details = Some(Details::DocumentAddition {
                                received_documents: number_of_documents,
                                indexed_documents,
                            });
                        }
                        Err(error) => {
                            task.status = Status::Failed;
                            task.details = Some(Details::DocumentAddition {
                                received_documents: count,
                                indexed_documents: count,
                            });
                            task.error = Some(error.into())
                        }
                    }
                }

                Ok(tasks)
            }
            IndexOperation::DocumentDeletion {
                index_uid: _,
                documents,
                mut tasks,
            } => {
                let mut builder = milli::update::DeleteDocuments::new(index_wtxn, index)?;
                documents.iter().for_each(|id| {
                    builder.delete_external_id(id);
                });

                let DocumentDeletionResult {
                    deleted_documents, ..
                } = builder.execute()?;

                for (task, documents) in tasks.iter_mut().zip(documents) {
                    task.status = Status::Succeeded;
                    task.details = Some(Details::DocumentDeletion {
                        received_document_ids: documents.len(),
                        deleted_documents: Some(deleted_documents),
                    });
                }

                Ok(tasks)
            }
            IndexOperation::Settings {
                index_uid: _,
                settings,
                allow_index_creation: _,
                mut tasks,
            } => {
                let indexer_config = self.index_mapper.indexer_config();
                // TODO merge the settings to only do *one* reindexation.
                for (task, (_, settings)) in tasks.iter_mut().zip(settings) {
                    let checked_settings = settings.clone().check();
                    task.details = Some(Details::Settings { settings });

                    let mut builder =
                        milli::update::Settings::new(index_wtxn, index, indexer_config);
                    apply_settings_to_builder(&checked_settings, &mut builder);
                    builder.execute(|indexing_step| {
                        debug!("update: {:?}", indexing_step);
                    })?;

                    task.status = Status::Succeeded;
                }

                Ok(tasks)
            }
            IndexOperation::SettingsAndDocumentImport {
                index_uid,
                primary_key,
                method,
                allow_index_creation,
                documents_counts,
                content_files,
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
                        allow_index_creation,
                        tasks: settings_tasks,
                    },
                )?;

                let mut import_tasks = self.apply_index_operation(
                    index_wtxn,
                    index,
                    IndexOperation::DocumentImport {
                        index_uid,
                        primary_key,
                        method,
                        allow_index_creation,
                        documents_counts,
                        content_files,
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
                allow_index_creation,
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
                    IndexOperation::Settings {
                        index_uid,
                        settings,
                        allow_index_creation,
                        tasks: settings_tasks,
                    },
                )?;

                let mut tasks = settings_tasks;
                tasks.append(&mut import_tasks);
                Ok(tasks)
            }
        }
    }

    /// Delete each given task from all the databases (if it is deleteable).
    ///
    /// Return the number of tasks that were actually deleted
    fn delete_matched_tasks(
        &self,
        wtxn: &mut RwTxn,
        matched_tasks: &RoaringBitmap,
    ) -> Result<usize> {
        // 1. Remove from this list the tasks that we are not allowed to delete
        let enqueued_tasks = self.get_status(wtxn, Status::Enqueued)?;

        let processing_tasks = &self.processing_tasks.read().unwrap().1;

        // TODO: Lo: Take the intersection of `matched_tasks` and `all_tasks` first,
        // so that we end up with the correct count for `deleted_tasks` (the value returned
        // by this function). Related snapshot test:
        // `task_deletion_delete_same_task_twice/task_deletion_processed.snap`
        let mut to_delete_tasks = matched_tasks - processing_tasks;
        to_delete_tasks -= enqueued_tasks;

        // 2. We now have a list of tasks to delete, delete them

        let mut affected_indexes = HashSet::new();
        let mut affected_statuses = HashSet::new();
        let mut affected_kinds = HashSet::new();

        for task_id in to_delete_tasks.iter() {
            if let Some(task) = self.get_task(wtxn, task_id)? {
                if let Some(task_indexes) = task.indexes() {
                    affected_indexes.extend(task_indexes.into_iter().map(|x| x.to_owned()));
                }
                affected_statuses.insert(task.status);
                affected_kinds.insert(task.operation.as_kind());
            }
        }
        for index in affected_indexes {
            self.update_index(wtxn, &index, |bitmap| {
                *bitmap -= &to_delete_tasks;
            })?;
        }
        for status in affected_statuses {
            self.update_status(wtxn, status, |bitmap| {
                *bitmap -= &to_delete_tasks;
            })?;
        }
        for kind in affected_kinds {
            self.update_kind(wtxn, kind, |bitmap| {
                *bitmap -= &to_delete_tasks;
            })?;
        }
        for task in to_delete_tasks.iter() {
            self.all_tasks.delete(wtxn, &BEU32::new(task))?;
        }
        Ok(to_delete_tasks.len() as usize)
    }
}

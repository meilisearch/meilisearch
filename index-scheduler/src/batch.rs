use crate::{
    autobatcher::BatchKind,
    task::{Details, Kind, KindWithContent, Status, Task},
    Error, IndexScheduler, Result, TaskId,
};
use index::{Settings, Unchecked};
use milli::{
    heed::{RoTxn, RwTxn},
    update::{DocumentAdditionResult, IndexDocumentsMethod},
    DocumentId,
};
use uuid::Uuid;

pub(crate) enum Batch {
    Cancel(Task),
    Snapshot(Vec<Task>),
    Dump(Vec<Task>),
    DocumentAddition {
        index_uid: String,
        primary_key: Option<String>,
        content_files: Vec<Uuid>,
        tasks: Vec<Task>,
    },
    DocumentUpdate {
        index_uid: String,
        primary_key: Option<String>,
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
        settings: Vec<(bool, Settings<Unchecked>)>,
        tasks: Vec<Task>,
    },
    DocumentClearAndSetting {
        index_uid: String,
        cleared_tasks: Vec<Task>,

        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<Task>,
    },
    SettingsAndDocumentAddition {
        index_uid: String,

        primary_key: Option<String>,
        content_files: Vec<Uuid>,
        document_addition_tasks: Vec<Task>,

        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<Task>,
    },
    SettingsAndDocumentUpdate {
        index_uid: String,

        primary_key: Option<String>,
        content_files: Vec<Uuid>,
        document_update_tasks: Vec<Task>,

        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<Task>,
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
    },
}

impl Batch {
    pub fn ids(&self) -> Vec<TaskId> {
        match self {
            Batch::Cancel(task)
            | Batch::IndexCreation { task, .. }
            | Batch::IndexUpdate { task, .. } => vec![task.uid],
            Batch::Snapshot(tasks)
            | Batch::Dump(tasks)
            | Batch::DocumentAddition { tasks, .. }
            | Batch::DocumentUpdate { tasks, .. }
            | Batch::DocumentDeletion { tasks, .. }
            | Batch::Settings { tasks, .. }
            | Batch::DocumentClear { tasks, .. }
            | Batch::IndexDeletion { tasks, .. } => tasks.iter().map(|task| task.uid).collect(),
            Batch::SettingsAndDocumentAddition {
                document_addition_tasks: tasks,
                settings_tasks: other,
                ..
            }
            | Batch::DocumentClearAndSetting {
                cleared_tasks: tasks,
                settings_tasks: other,
                ..
            }
            | Batch::SettingsAndDocumentUpdate {
                document_update_tasks: tasks,
                settings_tasks: other,
                ..
            } => tasks.iter().chain(other).map(|task| task.uid).collect(),
        }
    }
}

impl IndexScheduler {
    pub(crate) fn create_next_batch_index(
        &self,
        rtxn: &RoTxn,
        index_uid: String,
        batch: BatchKind,
    ) -> Result<Option<Batch>> {
        match batch {
            BatchKind::DocumentClear { ids } => Ok(Some(Batch::DocumentClear {
                tasks: self.get_existing_tasks(rtxn, ids)?,
                index_uid,
            })),
            BatchKind::DocumentAddition { addition_ids } => {
                let tasks = self.get_existing_tasks(rtxn, addition_ids)?;
                let primary_key = match &tasks[0].kind {
                    KindWithContent::DocumentAddition { primary_key, .. } => primary_key.clone(),
                    _ => unreachable!(),
                };
                let content_files = tasks
                    .iter()
                    .map(|task| match task.kind {
                        KindWithContent::DocumentAddition { content_file, .. } => content_file,
                        _ => unreachable!(),
                    })
                    .collect();

                Ok(Some(Batch::DocumentAddition {
                    index_uid,
                    primary_key,
                    content_files,
                    tasks,
                }))
            }
            BatchKind::DocumentUpdate { update_ids } => {
                let tasks = self.get_existing_tasks(rtxn, update_ids)?;
                let primary_key = match &tasks[0].kind {
                    KindWithContent::DocumentUpdate { primary_key, .. } => primary_key.clone(),
                    _ => unreachable!(),
                };
                let content_files = tasks
                    .iter()
                    .map(|task| match task.kind {
                        KindWithContent::DocumentUpdate { content_file, .. } => content_file,
                        _ => unreachable!(),
                    })
                    .collect();

                Ok(Some(Batch::DocumentUpdate {
                    index_uid,
                    primary_key,
                    content_files,
                    tasks,
                }))
            }
            BatchKind::DocumentDeletion { deletion_ids } => {
                let tasks = self.get_existing_tasks(rtxn, deletion_ids)?;

                let mut documents = Vec::new();
                for task in &tasks {
                    match task.kind {
                        KindWithContent::DocumentDeletion {
                            ref documents_ids, ..
                        } => documents.extend_from_slice(documents_ids),
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::DocumentDeletion {
                    index_uid,
                    documents,
                    tasks,
                }))
            }
            BatchKind::Settings { settings_ids } => {
                let tasks = self.get_existing_tasks(rtxn, settings_ids)?;

                let mut settings = Vec::new();
                for task in &tasks {
                    match task.kind {
                        KindWithContent::Settings {
                            ref new_settings,
                            is_deletion,
                            ..
                        } => settings.push((is_deletion, new_settings.clone())),
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::Settings {
                    index_uid,
                    settings,
                    tasks,
                }))
            }
            BatchKind::ClearAndSettings {
                other,
                settings_ids,
            } => {
                let (index_uid, settings, settings_tasks) = match self
                    .create_next_batch_index(rtxn, index_uid, BatchKind::Settings { settings_ids })?
                    .unwrap()
                {
                    Batch::Settings {
                        index_uid,
                        settings,
                        tasks,
                    } => (index_uid, settings, tasks),
                    _ => unreachable!(),
                };
                let (index_uid, cleared_tasks) = match self
                    .create_next_batch_index(
                        rtxn,
                        index_uid,
                        BatchKind::DocumentClear { ids: other },
                    )?
                    .unwrap()
                {
                    Batch::DocumentClear { index_uid, tasks } => (index_uid, tasks),
                    _ => unreachable!(),
                };

                Ok(Some(Batch::DocumentClearAndSetting {
                    index_uid,
                    cleared_tasks,
                    settings,
                    settings_tasks,
                }))
            }
            BatchKind::SettingsAndDocumentAddition {
                addition_ids,
                settings_ids,
            } => {
                let (index_uid, settings, settings_tasks) = match self
                    .create_next_batch_index(rtxn, index_uid, BatchKind::Settings { settings_ids })?
                    .unwrap()
                {
                    Batch::Settings {
                        index_uid,
                        settings,
                        tasks,
                    } => (index_uid, settings, tasks),
                    _ => unreachable!(),
                };

                let (index_uid, primary_key, content_files, document_addition_tasks) = match self
                    .create_next_batch_index(
                        rtxn,
                        index_uid,
                        BatchKind::DocumentAddition { addition_ids },
                    )?
                    .unwrap()
                {
                    Batch::DocumentAddition {
                        index_uid,
                        primary_key,
                        content_files,
                        tasks,
                    } => (index_uid, primary_key, content_files, tasks),
                    _ => unreachable!(),
                };

                Ok(Some(Batch::SettingsAndDocumentAddition {
                    index_uid,
                    primary_key,
                    content_files,
                    document_addition_tasks,
                    settings,
                    settings_tasks,
                }))
            }
            BatchKind::SettingsAndDocumentUpdate {
                update_ids,
                settings_ids,
            } => {
                let settings = self.create_next_batch_index(
                    rtxn,
                    index_uid.clone(),
                    BatchKind::Settings { settings_ids },
                )?;

                let document_update = self.create_next_batch_index(
                    rtxn,
                    index_uid.clone(),
                    BatchKind::DocumentUpdate { update_ids },
                )?;

                match (document_update, settings) {
                    (
                        Some(Batch::DocumentUpdate {
                            primary_key,
                            content_files,
                            tasks: document_update_tasks,
                            ..
                        }),
                        Some(Batch::Settings {
                            settings,
                            tasks: settings_tasks,
                            ..
                        }),
                    ) => Ok(Some(Batch::SettingsAndDocumentUpdate {
                        index_uid,
                        primary_key,
                        content_files,
                        document_update_tasks,
                        settings,
                        settings_tasks,
                    })),
                    _ => unreachable!(),
                }
            }
            BatchKind::IndexCreation { id } => {
                let task = self.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                let (index_uid, primary_key) = match &task.kind {
                    KindWithContent::IndexCreation {
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
            BatchKind::IndexUpdate { id } => {
                let task = self.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                let primary_key = match &task.kind {
                    KindWithContent::IndexUpdate { primary_key, .. } => primary_key.clone(),
                    _ => unreachable!(),
                };
                Ok(Some(Batch::IndexUpdate {
                    index_uid,
                    primary_key,
                    task,
                }))
            }
            BatchKind::IndexDeletion { ids } => Ok(Some(Batch::IndexDeletion {
                index_uid,
                tasks: self.get_existing_tasks(rtxn, ids)?,
            })),
            BatchKind::IndexSwap { id: _ } => todo!(),
            BatchKind::IndexRename { id: _ } => todo!(),
        }
    }

    /// Create the next batch to be processed;
    /// 1. We get the *last* task to cancel.
    /// 2. We get the *next* snapshot to process.
    /// 3. We get the *next* dump to process.
    /// 4. We get the *next* tasks to process for a specific index.
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

        // 2. we batch the snapshot.
        let to_snapshot = self.get_kind(rtxn, Kind::Snapshot)? & enqueued;
        if !to_snapshot.is_empty() {
            return Ok(Some(Batch::Snapshot(
                self.get_existing_tasks(rtxn, to_snapshot)?,
            )));
        }

        // 3. we batch the dumps.
        let to_dump = self.get_kind(rtxn, Kind::DumpExport)? & enqueued;
        if !to_dump.is_empty() {
            return Ok(Some(Batch::Dump(self.get_existing_tasks(rtxn, to_dump)?)));
        }

        // 4. We take the next task and try to batch all the tasks associated with this index.
        if let Some(task_id) = enqueued.min() {
            let task = self
                .get_task(rtxn, task_id)?
                .ok_or(Error::CorruptedTaskQueue)?;

            // This is safe because all the remaining task are associated with
            // AT LEAST one index. We can use the right or left one it doesn't
            // matter.
            let index_name = task.indexes().unwrap()[0];

            let _index = self.get_index(rtxn, &index_name)? & enqueued;

            let enqueued = enqueued
                .into_iter()
                .map(|task_id| {
                    self.get_task(rtxn, task_id)
                        .and_then(|task| task.ok_or(Error::CorruptedTaskQueue))
                        .map(|task| (task.uid, task.kind.as_kind()))
                })
                .collect::<Result<Vec<_>>>()?;

            if let Some(batchkind) = crate::autobatcher::autobatch(enqueued) {
                return self.create_next_batch_index(rtxn, index_name.to_string(), batchkind);
            }
        }

        // If we found no tasks then we were notified for something that got autobatched
        // somehow and there is nothing to do.
        Ok(None)
    }

    pub(crate) fn process_batch(&self, batch: Batch) -> Result<Vec<Task>> {
        match batch {
            Batch::Cancel(_) => todo!(),
            Batch::Snapshot(_) => todo!(),
            Batch::Dump(_) => todo!(),
            Batch::DocumentClear { tasks, .. } => todo!(),
            Batch::DocumentAddition {
                index_uid,
                primary_key,
                content_files,
                mut tasks,
            } => {
                // we NEED a write transaction for the index creation.
                // To avoid blocking the whole process we're going to commit asap.
                let mut wtxn = self.env.write_txn()?;
                let index = self.index_mapper.create_index(&mut wtxn, &index_uid)?;
                wtxn.commit()?;

                let ret = index.update_documents(
                    IndexDocumentsMethod::ReplaceDocuments,
                    primary_key,
                    self.file_store.clone(),
                    content_files,
                )?;

                for (task, ret) in tasks.iter_mut().zip(ret) {
                    match ret {
                        Ok(DocumentAdditionResult {
                            indexed_documents,
                            number_of_documents,
                        }) => {
                            task.details = Some(Details::DocumentAddition {
                                received_documents: number_of_documents,
                                indexed_documents,
                            });
                        }
                        Err(error) => {
                            task.error = Some(error.into());
                        }
                    }
                }

                Ok(tasks)
            }
            Batch::SettingsAndDocumentAddition {
                index_uid,
                primary_key,
                content_files,
                document_addition_tasks,
                settings: _,
                settings_tasks: _,
            } => {
                todo!();
            }
            Batch::DocumentUpdate {
                index_uid,
                primary_key,
                content_files,
                tasks,
            } => todo!(),
            Batch::DocumentDeletion {
                index_uid,
                documents,
                tasks,
            } => todo!(),
            Batch::Settings {
                index_uid,
                settings,
                tasks,
            } => todo!(),
            Batch::DocumentClearAndSetting {
                index_uid,
                cleared_tasks,
                settings,
                settings_tasks,
            } => todo!(),
            Batch::SettingsAndDocumentUpdate {
                index_uid,
                primary_key,
                content_files,
                document_update_tasks,
                settings,
                settings_tasks,
            } => todo!(),
            Batch::IndexCreation {
                index_uid,
                primary_key,
                task,
            } => todo!(),
            Batch::IndexUpdate {
                index_uid,
                primary_key,
                task,
            } => todo!(),
            Batch::IndexDeletion { index_uid, tasks } => todo!(),
        }
    }
}

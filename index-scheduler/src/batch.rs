use crate::{
    autobatcher::BatchKind,
    task::{Details, Kind, KindWithContent, Status, Task},
    Error, IndexScheduler, Result, TaskId,
};
use index::{Settings, Unchecked};
use milli::heed::RoTxn;
use milli::update::{DocumentAdditionResult, DocumentDeletionResult, IndexDocumentsMethod};
use uuid::Uuid;

pub(crate) enum Batch {
    Cancel(Task),
    Snapshot(Vec<Task>),
    Dump(Vec<Task>),
    DocumentImport {
        index_uid: String,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
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
        tasks: Vec<Task>,
    },
    DocumentClearAndSetting {
        index_uid: String,
        cleared_tasks: Vec<Task>,

        // TODO what's that boolean, does it mean that it removes things or what?
        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<Task>,
    },
    SettingsAndDocumentImport {
        index_uid: String,

        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_files: Vec<Uuid>,
        document_import_tasks: Vec<Task>,

        // TODO what's that boolean, does it mean that it removes things or what?
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
            | Batch::DocumentImport { tasks, .. }
            | Batch::DocumentDeletion { tasks, .. }
            | Batch::Settings { tasks, .. }
            | Batch::DocumentClear { tasks, .. }
            | Batch::IndexDeletion { tasks, .. } => tasks.iter().map(|task| task.uid).collect(),
            Batch::SettingsAndDocumentImport {
                document_import_tasks: tasks,
                settings_tasks: other,
                ..
            }
            | Batch::DocumentClearAndSetting {
                cleared_tasks: tasks,
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
            BatchKind::DocumentImport { method, import_ids } => {
                let tasks = self.get_existing_tasks(rtxn, import_ids)?;
                let primary_key = match &tasks[0].kind {
                    KindWithContent::DocumentImport { primary_key, .. } => primary_key.clone(),
                    _ => unreachable!(),
                };
                let content_files = tasks
                    .iter()
                    .map(|task| match task.kind {
                        KindWithContent::DocumentImport { content_file, .. } => content_file,
                        _ => unreachable!(),
                    })
                    .collect();

                Ok(Some(Batch::DocumentImport {
                    index_uid,
                    primary_key,
                    method,
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
            BatchKind::SettingsAndDocumentImport {
                settings_ids,
                method,
                import_ids,
            } => {
                let settings = self.create_next_batch_index(
                    rtxn,
                    index_uid.clone(),
                    BatchKind::Settings { settings_ids },
                )?;

                let document_import = self.create_next_batch_index(
                    rtxn,
                    index_uid.clone(),
                    BatchKind::DocumentImport { method, import_ids },
                )?;

                match (document_import, settings) {
                    (
                        Some(Batch::DocumentImport {
                            primary_key,
                            content_files,
                            tasks: document_import_tasks,
                            ..
                        }),
                        Some(Batch::Settings {
                            settings,
                            tasks: settings_tasks,
                            ..
                        }),
                    ) => Ok(Some(Batch::SettingsAndDocumentImport {
                        index_uid,
                        primary_key,
                        method,
                        content_files,
                        document_import_tasks,
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
            Batch::DocumentClear {
                index_uid,
                mut tasks,
            } => {
                let rtxn = self.env.read_txn()?;
                let index = self.index_mapper.index(&rtxn, &index_uid)?;
                rtxn.abort()?;

                let ret = index.clear_documents();
                for task in &mut tasks {
                    task.details = Some(Details::ClearAll {
                        // TODO where can I find this information of how many documents did we delete?
                        deleted_documents: None,
                    });
                    if let Err(ref error) = ret {
                        task.error = Some(error.into());
                    }
                }

                Ok(tasks)
            }
            Batch::DocumentImport {
                index_uid,
                primary_key,
                method,
                content_files,
                mut tasks,
            } => {
                // we NEED a write transaction for the index creation.
                // To avoid blocking the whole process we're going to commit asap.
                let mut wtxn = self.env.write_txn()?;
                let index = self.index_mapper.create_index(&mut wtxn, &index_uid)?;
                wtxn.commit()?;

                let ret = index.update_documents(
                    method,
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
                        Err(error) => task.error = Some(error.into()),
                    }
                }

                Ok(tasks)
            }
            Batch::SettingsAndDocumentImport {
                index_uid,
                primary_key,
                method,
                content_files,
                document_import_tasks,
                settings: _,
                settings_tasks: _,
            } => {
                todo!();
            }
            Batch::DocumentDeletion {
                index_uid,
                documents,
                mut tasks,
            } => {
                let rtxn = self.env.read_txn()?;
                let index = self.index_mapper.index(&rtxn, &index_uid)?;

                let ret = index.delete_documents(&documents);
                for task in &mut tasks {
                    match ret {
                        Ok(DocumentDeletionResult {
                            deleted_documents,
                            remaining_documents: _,
                        }) => {
                            // TODO we are assigning the same amount of documents to
                            //      all the tasks that are in the same batch. That's wrong!
                            task.details = Some(Details::DocumentDeletion {
                                received_document_ids: documents.len(),
                                deleted_documents: Some(deleted_documents),
                            });
                        }
                        Err(ref error) => task.error = Some(error.into()),
                    }
                }

                Ok(tasks)
            }
            Batch::Settings {
                index_uid,
                settings,
                mut tasks,
            } => {
                // we NEED a write transaction for the index creation.
                // To avoid blocking the whole process we're going to commit asap.
                let mut wtxn = self.env.write_txn()?;
                let index = self.index_mapper.create_index(&mut wtxn, &index_uid)?;
                wtxn.commit()?;

                // TODO merge the settings to only do a reindexation once.
                for (task, (_, settings)) in tasks.iter_mut().zip(settings) {
                    let checked_settings = settings.clone().check();
                    task.details = Some(Details::Settings { settings });
                    if let Err(error) = index.update_settings(&checked_settings) {
                        task.error = Some(error.into());
                    }
                }

                Ok(tasks)
            }
            Batch::DocumentClearAndSetting {
                index_uid,
                mut cleared_tasks,
                settings,
                mut settings_tasks,
            } => {
                // If the settings were given before the document clear
                // we must create the index first.
                // we NEED a write transaction for the index creation.
                // To avoid blocking the whole process we're going to commit asap.
                let mut wtxn = self.env.write_txn()?;
                let index = self.index_mapper.create_index(&mut wtxn, &index_uid)?;
                wtxn.commit()?;

                // TODO We must use the same write transaction to commit
                //      the clear AND the settings in one transaction.

                let ret = index.clear_documents();
                for task in &mut cleared_tasks {
                    task.details = Some(Details::ClearAll {
                        // TODO where can I find this information of how many documents did we delete?
                        deleted_documents: None,
                    });
                    if let Err(ref error) = ret {
                        task.error = Some(error.into());
                    }
                }

                // TODO merge the settings to only do a reindexation once.
                for (task, (_, settings)) in settings_tasks.iter_mut().zip(settings) {
                    let checked_settings = settings.clone().check();
                    task.details = Some(Details::Settings { settings });
                    if let Err(error) = index.update_settings(&checked_settings) {
                        task.error = Some(error.into());
                    }
                }

                let mut tasks = cleared_tasks;
                tasks.append(&mut settings_tasks);
                Ok(tasks)
            }
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

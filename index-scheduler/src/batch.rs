use crate::{
    autobatcher::BatchKind,
    task::{KindWithContent, Status},
    Error, IndexScheduler, Result, TaskId,
};
use index::{Settings, Unchecked};
use milli::{
    heed::{RoTxn, RwTxn},
    update::IndexDocumentsMethod,
};
use uuid::Uuid;

use crate::{task::Kind, Task};

pub(crate) enum Batch {
    Cancel(Task),
    Snapshot(Vec<Task>),
    Dump(Vec<Task>),
    // IndexSpecific { index_uid: String, kind: BatchKind },
    DocumentAddition {
        index_uid: String,
        primary_key: Option<String>,
        content_files: Vec<Uuid>,
        tasks: Vec<Task>,
    },
    SettingsAndDocumentAddition {
        index_uid: String,

        primary_key: Option<String>,
        content_files: Vec<Uuid>,
        document_addition_tasks: Vec<Task>,

        settings: Vec<Settings<Unchecked>>,
        settings_tasks: Vec<Task>,
    },
}

impl IndexScheduler {
    pub(crate) fn create_next_batch_index(
        &self,
        rtxn: &RoTxn,
        index_uid: String,
        batch: BatchKind,
    ) -> Result<Option<Batch>> {
        match batch {
            BatchKind::DocumentClear { ids } => todo!(),
            BatchKind::DocumentAddition { addition_ids } => todo!(),
            BatchKind::DocumentUpdate { update_ids } => todo!(),
            BatchKind::DocumentDeletion { deletion_ids } => todo!(),
            BatchKind::ClearAndSettings {
                other,
                settings_ids,
            } => todo!(),
            BatchKind::SettingsAndDocumentAddition {
                addition_ids,
                settings_ids,
            } => {
                // you're not supposed to create an empty BatchKind.
                assert!(addition_ids.len() > 0);
                assert!(settings_ids.len() > 0);

                let document_addition_tasks = addition_ids
                    .iter()
                    .map(|tid| {
                        self.get_task(rtxn, *tid)
                            .and_then(|task| task.ok_or(Error::CorruptedTaskQueue))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let settings_tasks = settings_ids
                    .iter()
                    .map(|tid| {
                        self.get_task(rtxn, *tid)
                            .and_then(|task| task.ok_or(Error::CorruptedTaskQueue))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let primary_key = match &document_addition_tasks[0].kind {
                    KindWithContent::DocumentAddition { primary_key, .. } => primary_key.clone(),
                    _ => unreachable!(),
                };
                let content_files = document_addition_tasks
                    .iter()
                    .map(|task| match task.kind {
                        KindWithContent::DocumentAddition { content_file, .. } => content_file,
                        _ => unreachable!(),
                    })
                    .collect();

                let settings = settings_tasks
                    .iter()
                    .map(|task| match &task.kind {
                        KindWithContent::Settings { new_settings, .. } => new_settings.clone(),
                        _ => unreachable!(),
                    })
                    .collect();

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
            } => todo!(),
            BatchKind::Settings { settings_ids } => todo!(),
            BatchKind::IndexCreation { id } => todo!(),
            BatchKind::IndexDeletion { ids } => todo!(),
            BatchKind::IndexUpdate { id } => todo!(),
            BatchKind::IndexSwap { id } => todo!(),
            BatchKind::IndexRename { id } => todo!(),
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

            let index = self.get_index(rtxn, &index_name)? & enqueued;

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

    pub(crate) fn process_batch(&self, wtxn: &mut RwTxn, batch: Batch) -> Result<Vec<Task>> {
        match batch {
            Batch::Cancel(_) => todo!(),
            Batch::Snapshot(_) => todo!(),
            Batch::Dump(_) => todo!(),
            Batch::DocumentAddition {
                index_uid,
                primary_key,
                content_files,
                tasks,
            } => todo!(),
            Batch::SettingsAndDocumentAddition {
                index_uid,
                primary_key,
                content_files,
                document_addition_tasks,
                settings,
                settings_tasks,
            } => {
                let index = self.create_index(wtxn, &index_uid)?;
                let mut updated_tasks = Vec::new();

                /*
                let ret = index.update_settings(settings)?;
                for (ret, task) in ret.iter().zip(settings_tasks) {
                    match ret {
                        Ok(ret) => task.status = Some(ret),
                        Err(err) => task.error = Some(err),
                    }
                }
                */

                /*
                for (ret, task) in ret.iter().zip(settings_tasks) {
                    match ret {
                        Ok(ret) => task.status = Some(ret),
                        Err(err) => task.error = Some(err),
                    }
                    updated_tasks.push(task);
                }
                */

                let ret = index.update_documents(
                    IndexDocumentsMethod::ReplaceDocuments,
                    primary_key,
                    self.file_store.clone(),
                    content_files.into_iter(),
                )?;

                for (ret, mut task) in ret.iter().zip(document_addition_tasks.into_iter()) {
                    match ret {
                        Ok(ret) => task.info = Some(format!("{:?}", ret)),
                        Err(err) => task.error = Some(err.to_string()),
                    }
                    updated_tasks.push(task);
                }
                Ok(updated_tasks)
            }
        }
    }
}

/*
impl Batch {
    pub fn task_ids(&self) -> impl IntoIterator<Item = TaskId> + '_ {
        match self {
            Batch::Cancel(task) | Batch::One(task) => {
                Box::new(std::iter::once(task.uid)) as Box<dyn Iterator<Item = TaskId>>
            }
            Batch::Snapshot(tasks) | Batch::Dump(tasks) | Batch::Contiguous { tasks, .. } => {
                Box::new(tasks.iter().map(|task| task.uid)) as Box<dyn Iterator<Item = TaskId>>
            }
            Batch::Empty => Box::new(std::iter::empty()) as Box<dyn Iterator<Item = TaskId>>,
        }
    }
}
*/

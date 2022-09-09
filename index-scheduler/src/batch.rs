use crate::{autobatcher::BatchKind, task::Status, Error, IndexScheduler, Result, TaskId};
use milli::{heed::RoTxn, update::IndexDocumentsMethod};
use uuid::Uuid;

use crate::{task::Kind, Task};

pub(crate) enum Batch {
    Cancel(Task),
    Snapshot(Vec<Task>),
    Dump(Vec<Task>),
    IndexSpecific { index_uid: String, kind: BatchKind },
}

impl IndexScheduler {
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

            return Ok(crate::autobatcher::autobatch(enqueued).map(|batch_kind| {
                Batch::IndexSpecific {
                    index_uid: index_name.to_string(),
                    kind: batch_kind,
                }
            }));
        }

        // If we found no tasks then we were notified for something that got autobatched
        // somehow and there is nothing to do.
        Ok(None)
    }

    pub(crate) fn process_batch(&self, wtxn: &mut RwTxn, batch: Batch) -> Result<Vec<Task>> {
        match batch {
            Batch::IndexSpecific { index_uid, kind } => {
                let index = create_index();
                match kind {
                    BatchKind::ClearAll { ids } => todo!(),
                    BatchKind::DocumentAddition { addition_ids } => {
                        let index = self.create_index(wtxn, &index_uid)?;
                        let ret = index.update_documents(
                            IndexDocumentsMethod::UpdateDocuments,
                            None, // TODO primary key
                            self.file_store,
                            content_files,
                        )?;

                        assert_eq!(ret.len(), tasks.len(), "Update documents must return the same number of `Result` than the number of tasks.");

                        Ok(tasks
                            .into_iter()
                            .zip(ret)
                            .map(|(mut task, res)| match res {
                                Ok(info) => {
                                    task.status = Status::Succeeded;
                                    task.info = Some(info.to_string());
                                }
                                Err(error) => {
                                    task.status = Status::Failed;
                                    task.error = Some(error.to_string());
                                }
                            })
                            .collect())
                    }
                    BatchKind::DocumentDeletion { deletion_ids } => todo!(),
                    BatchKind::ClearAllAndSettings {
                        other,
                        settings_ids,
                    } => todo!(),
                    BatchKind::SettingsAndDocumentAddition {
                        settings_ids,
                        addition_ids,
                    } => todo!(),
                    BatchKind::Settings { settings_ids } => todo!(),
                    BatchKind::DeleteIndex { ids } => todo!(),
                    BatchKind::CreateIndex { id } => todo!(),
                    BatchKind::SwapIndex { id } => todo!(),
                    BatchKind::RenameIndex { id } => todo!(),
                }
            }
            _ => unreachable!(),
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

use crate::{
    task::{KindWithContent, Status},
    Error, IndexScheduler, Result, TaskId,
};
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
    /*
    pub(crate) fn process_batch(&self, wtxn: &mut RwTxn, batch: Batch) -> Result<Vec<Task>> {
        match batch {
            Batch::DocumentAddition {
                tasks,
                primary_key,
                content_files,
                index_uid,
            } => {
                let index = self.create_index(wtxn, &index_uid)?;
                let ret = index.update_documents(
                    IndexDocumentsMethod::UpdateDocuments,
                    primary_key,
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
            _ => unreachable!(),
        }
    }
    */
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

            return Ok(
                autobatcher(enqueued).map(|batch_kind| Batch::IndexSpecific {
                    index_uid: index_name.to_string(),
                    kind: batch_kind,
                }),
            );
        }

        // If we found no tasks then we were notified for something that got autobatched
        // somehow and there is nothing to do.
        Ok(None)
    }

    /// Batch all the consecutive tasks coming next that shares the same `Kind`
    /// for a specific index. There *MUST* be at least ONE task of this kind.
    fn batch_contiguous_kind(&self, rtxn: &RoTxn, index: &str, kind: Kind) -> Result<Batch> {
        let enqueued = &self.get_status(rtxn, Status::Enqueued)?;

        // [1, 2, 4, 5]
        let index_tasks = self.get_index(rtxn, &index)? & enqueued;
        // [1, 2, 5]
        let tasks_kind = &index_tasks & self.get_kind(rtxn, kind)?;
        // [4]
        let not_kind = &index_tasks - &tasks_kind;

        // [1, 2]
        let mut to_process = tasks_kind.clone();
        if let Some(max) = not_kind.max() {
            // it's safe to unwrap since we already ensured there
            // was AT LEAST one task with the document addition tasks_kind.
            to_process.remove_range(tasks_kind.min().unwrap()..max);
        }

        Ok(Batch::Contiguous {
            tasks: self.get_existing_tasks(rtxn, to_process)?,
            kind,
        })
    }
}

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

pub(crate) enum BatchKind {
    ClearAll {
        ids: Vec<TaskId>,
    },
    DocumentAddition {
        addition_ids: Vec<TaskId>,
    },
    DocumentDeletion {
        deletion_ids: Vec<TaskId>,
    },
    ClearAllAndSettings {
        other: Vec<TaskId>,
        settings_ids: Vec<TaskId>,
    },
    SettingsAndDocumentAddition {
        settings_ids: Vec<TaskId>,
        addition_ids: Vec<TaskId>,
    },
    Settings {
        settings_ids: Vec<TaskId>,
    },
    DeleteIndex {
        ids: Vec<TaskId>,
    },
    CreateIndex {
        id: TaskId,
    },
    SwapIndex {
        id: TaskId,
    },
    RenameIndex {
        id: TaskId,
    },
}

impl BatchKind {
    /// return true if you must stop right there.
    pub fn new(task_id: TaskId, kind: Kind) -> (Self, bool) {
        match kind {
            Kind::CreateIndex => (BatchKind::CreateIndex { id: task_id }, true),
            Kind::DeleteIndex => (BatchKind::DeleteIndex { ids: vec![task_id] }, true),
            Kind::RenameIndex => (BatchKind::RenameIndex { id: task_id }, true),
            Kind::SwapIndex => (BatchKind::SwapIndex { id: task_id }, true),
            Kind::ClearAllDocuments => (BatchKind::ClearAll { ids: vec![task_id] }, false),
            Kind::DocumentAddition => (
                BatchKind::DocumentAddition {
                    addition_ids: vec![task_id],
                },
                false,
            ),
            Kind::DocumentDeletion => (
                BatchKind::DocumentDeletion {
                    deletion_ids: vec![task_id],
                },
                false,
            ),
            Kind::Settings => (
                BatchKind::Settings {
                    settings_ids: vec![task_id],
                },
                false,
            ),

            Kind::DumpExport | Kind::Snapshot | Kind::CancelTask => unreachable!(),
        }
    }

    /// Return true if you must stop.
    fn accumulate(&mut self, id: TaskId, kind: Kind) -> bool {
        match (self, kind) {
            // must handle the deleteIndex
            (_, Kind::CreateIndex | Kind::RenameIndex | Kind::SwapIndex) => true,

            (BatchKind::ClearAll { ids }, Kind::ClearAllDocuments | Kind::DocumentDeletion) => {
                ids.push(id);
                false
            }
            (BatchKind::ClearAll { .. }, Kind::DocumentAddition | Kind::Settings) => true,
            (BatchKind::DocumentAddition { addition_ids }, Kind::ClearAllDocuments) => {
                addition_ids.push(id);
                *self = BatchKind::ClearAll {
                    ids: addition_ids.clone(),
                };
                false
            }

            (BatchKind::DocumentAddition { addition_ids }, Kind::DocumentAddition) => {
                addition_ids.push(id);
                false
            }
            (BatchKind::DocumentAddition { .. }, Kind::DocumentDeletion) => true,
            (BatchKind::DocumentAddition { addition_ids }, Kind::Settings) => {
                *self = BatchKind::SettingsAndDocumentAddition {
                    settings_ids: vec![id],
                    addition_ids: addition_ids.clone(),
                };
                false
            }

            (BatchKind::DocumentDeletion { deletion_ids }, Kind::ClearAllDocuments) => {
                deletion_ids.push(id);
                *self = BatchKind::ClearAll {
                    ids: deletion_ids.clone(),
                };
                false
            }
            (BatchKind::DocumentDeletion { .. }, Kind::DocumentAddition) => true,
            (BatchKind::DocumentDeletion { deletion_ids }, Kind::DocumentDeletion) => {
                deletion_ids.push(id);
                false
            }
            (BatchKind::DocumentDeletion { .. }, Kind::Settings) => true,

            (BatchKind::Settings { settings_ids }, Kind::ClearAllDocuments) => {
                *self = BatchKind::ClearAllAndSettings {
                    settings_ids: settings_ids.clone(),
                    other: vec![id],
                };
                false
            }
            (BatchKind::Settings { .. }, Kind::DocumentAddition) => true,
            (BatchKind::Settings { .. }, Kind::DocumentDeletion) => true,
            (BatchKind::Settings { settings_ids }, Kind::Settings) => {
                settings_ids.push(id);
                false
            }

            (
                BatchKind::ClearAllAndSettings {
                    other,
                    settings_ids,
                },
                Kind::ClearAllDocuments,
            ) => {
                other.push(id);
                false
            }
            (BatchKind::ClearAllAndSettings { .. }, Kind::DocumentAddition) => true,
            (
                BatchKind::ClearAllAndSettings {
                    other,
                    settings_ids,
                },
                Kind::DocumentDeletion,
            ) => {
                other.push(id);
                false
            }
            (
                BatchKind::ClearAllAndSettings {
                    settings_ids,
                    other,
                },
                Kind::Settings,
            ) => {
                settings_ids.push(id);
                false
            }
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids,
                },
                Kind::ClearAllDocuments,
            ) => {
                addition_ids.push(id);
                *self = BatchKind::ClearAllAndSettings {
                    settings_ids: settings_ids.clone(),
                    other: addition_ids.clone(),
                };
                false
            }
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids,
                },
                Kind::DocumentAddition,
            ) => {
                addition_ids.push(id);
                false
            }
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids,
                },
                Kind::DocumentDeletion,
            ) => true,
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids,
                },
                Kind::Settings,
            ) => {
                settings_ids.push(id);
                false
            }
            (_, Kind::CancelTask | Kind::DumpExport | Kind::Snapshot) => unreachable!(),
            (
                BatchKind::CreateIndex { .. }
                | BatchKind::DeleteIndex { .. }
                | BatchKind::SwapIndex { .. }
                | BatchKind::RenameIndex { .. },
                _,
            ) => {
                unreachable!()
            }
        }
    }
}

pub fn autobatcher(enqueued: Vec<(TaskId, Kind)>) -> Option<BatchKind> {
    let mut enqueued = enqueued.into_iter();
    let (id, kind) = enqueued.next()?;
    let (mut acc, is_finished) = BatchKind::new(id, kind);
    if is_finished {
        return Some(acc);
    }

    for (id, kind) in enqueued {
        if acc.accumulate(id, kind) {
            break;
        }
    }

    Some(acc)
}

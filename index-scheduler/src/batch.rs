use crate::{
    task::{KindWithContent, Status},
    Error, IndexScheduler, Result, TaskId,
};
use milli::heed::RoTxn;

use crate::{task::Kind, Task};

pub(crate) enum Batch {
    Cancel(Task),
    Snapshot(Vec<Task>),
    Dump(Vec<Task>),
    Contiguous { tasks: Vec<Task>, kind: Kind },
    One(Task),
    Empty,
}

impl IndexScheduler {
    /// Create the next batch to be processed;
    /// 1. We get the *last* task to cancel.
    /// 2. We get the *next* snapshot to process.
    /// 3. We get the *next* dump to process.
    /// 4. We get the *next* tasks to process for a specific index.
    pub(crate) fn get_next_batch(&self, rtxn: &RoTxn) -> Result<Batch> {
        let enqueued = &self.get_status(rtxn, Status::Enqueued)?;
        let to_cancel = self.get_kind(rtxn, Kind::CancelTask)? & enqueued;

        // 1. we get the last task to cancel.
        if let Some(task_id) = to_cancel.max() {
            return Ok(Batch::Cancel(
                self.get_task(rtxn, task_id)?
                    .ok_or(Error::CorruptedTaskQueue)?,
            ));
        }

        // 2. we batch the snapshot.
        let to_snapshot = self.get_kind(rtxn, Kind::Snapshot)? & enqueued;
        if !to_snapshot.is_empty() {
            return Ok(Batch::Snapshot(self.get_existing_tasks(rtxn, to_snapshot)?));
        }

        // 3. we batch the dumps.
        let to_dump = self.get_kind(rtxn, Kind::DumpExport)? & enqueued;
        if !to_dump.is_empty() {
            return Ok(Batch::Dump(self.get_existing_tasks(rtxn, to_dump)?));
        }

        // 4. We take the next task and try to batch all the tasks associated with this index.
        if let Some(task_id) = enqueued.min() {
            let task = self
                .get_task(rtxn, task_id)?
                .ok_or(Error::CorruptedTaskQueue)?;
            match task.kind {
                // We can batch all the consecutive tasks coming next which
                // have the kind `DocumentAddition`.
                KindWithContent::DocumentAddition { index_name, .. } => {
                    return self.batch_contiguous_kind(rtxn, &index_name, Kind::DocumentAddition)
                }
                // We can batch all the consecutive tasks coming next which
                // have the kind `DocumentDeletion`.
                KindWithContent::DocumentDeletion { index_name, .. } => {
                    return self.batch_contiguous_kind(rtxn, &index_name, Kind::DocumentAddition)
                }
                // The following tasks can't be batched
                KindWithContent::ClearAllDocuments { .. }
                | KindWithContent::RenameIndex { .. }
                | KindWithContent::CreateIndex { .. }
                | KindWithContent::DeleteIndex { .. }
                | KindWithContent::SwapIndex { .. } => return Ok(Batch::One(task)),

                // The following tasks have already been batched and thus can't appear here.
                KindWithContent::CancelTask { .. }
                | KindWithContent::DumpExport { .. }
                | KindWithContent::Snapshot => {
                    unreachable!()
                }
            }
        }

        // If we found no tasks then we were notified for something that got autobatched
        // somehow and there is nothing to do.
        Ok(Batch::Empty)
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

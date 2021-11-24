mod store;

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use log::debug;
use tokio::sync::RwLock;

use crate::index_resolver::IndexUid;
use crate::tasks::task::TaskEvent;

use super::error::TaskError;
use super::task::{GhostTask, Task, TaskContent, TaskId};
use super::Result;

#[cfg(test)]
pub use store::test::MockStore as Store;
#[cfg(not(test))]
pub use store::Store;

/// Defines constraints to be applied when querying for Tasks from the store.
#[derive(Default, Debug)]
pub struct TaskFilter {
    indexes: Option<HashSet<String>>,
}

impl TaskFilter {
    fn pass(&self, task: &Task) -> bool {
        self.indexes
            .as_ref()
            .map(|indexes| indexes.contains(&*task.index_uid))
            .unwrap_or(true)
    }

    /// Adds an index to the filter, so the filter must match this index.
    pub fn filter_index(&mut self, index: String) {
        self.indexes
            .get_or_insert_with(Default::default)
            .insert(index);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PendingTask<T> {
    // The id of a task to process
    Real(T),
    // A ghost task, without an id. Ghost tasks always have a higher priority over normal tasks
    Ghost(GhostTask),
}

impl<T> PendingTask<T> {
    /// Map the content of the `PendingTask::Real(content)` changing the type of the `PendingTask`.
    pub fn map_real<U, F: FnOnce(T) -> U>(self, f: F) -> PendingTask<U> {
        match self {
            Self::Real(task) => PendingTask::Real(f(task)),
            Self::Ghost(task) => PendingTask::Ghost(task),
        }
    }
}

impl PendingTask<TaskId> {
    /// Return the `TaskId` of the task if it's a realy Task.
    pub fn get_task_id(&self) -> Option<TaskId> {
        match self {
            Self::Real(tid) => Some(*tid),
            _ => None,
        }
    }
}

impl PendingTask<Task> {
    /// Return the `TaskId` of the task if it's a realy Task.
    pub fn get_task_id(&self) -> Option<TaskId> {
        match self {
            Self::Real(task) => Some(task.id),
            _ => None,
        }
    }
}

impl Eq for PendingTask<TaskId> {}

impl PartialOrd for PendingTask<TaskId> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (PendingTask::Real(lhs), PendingTask::Real(rhs)) => Some(lhs.cmp(rhs)),
            (PendingTask::Real(_), PendingTask::Ghost(_)) => Some(Ordering::Less),
            (PendingTask::Ghost(_), PendingTask::Real(_)) => Some(Ordering::Greater),
            (PendingTask::Ghost(_), PendingTask::Ghost(_)) => Some(Ordering::Equal),
        }
    }
}

impl Ord for PendingTask<TaskId> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct TaskStore {
    store: Arc<Store>,
    pending_queue: Arc<RwLock<BinaryHeap<Reverse<PendingTask<TaskId>>>>>,
}

impl Clone for TaskStore {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            pending_queue: self.pending_queue.clone(),
        }
    }
}

impl TaskStore {
    pub fn new(path: impl AsRef<Path>, size: usize) -> Result<Self> {
        Ok(Self {
            store: Arc::new(Store::new(path, size)?),
            pending_queue: Arc::default(),
        })
    }

    pub async fn register(&self, index_uid: IndexUid, content: TaskContent) -> Result<Task> {
        debug!("registering update: {:?}", content);
        let store = self.store.clone();
        let task = tokio::task::spawn_blocking(move || -> Result<Task> {
            let mut txn = store.wtxn()?;
            let next_task_id = store.next_task_id(&mut txn)?;
            let created_at = TaskEvent::Created(Utc::now());
            let task = Task {
                id: next_task_id,
                index_uid,
                content,
                events: vec![created_at],
            };

            store.put(&mut txn, &task)?;

            txn.commit()?;

            Ok(task)
        })
        .await??;

        self.pending_queue
            .write()
            .await
            .push(Reverse(PendingTask::Real(task.id)));

        Ok(task)
    }

    /// Register an update that applies on multiple indexes.
    /// Currently the update is considered as a priority.
    pub async fn register_ghost_task(&self, content: GhostTask) {
        debug!("registering a ghost task: {:?}", content);
        self.pending_queue
            .write()
            .await
            .push(Reverse(PendingTask::Ghost(content)));
    }

    /// Returns the next task to process.
    pub async fn peek_pending_task(&self) -> Option<PendingTask<TaskId>> {
        self.pending_queue
            .read()
            .await
            .peek()
            // we don't want to keep the mutex thus we clone the data.
            .map(|Reverse(pending_task)| pending_task.clone())
    }

    /// Returns the next task to process if there is one.
    pub async fn get_processing_task(&self) -> Result<Option<Task>> {
        match self.peek_pending_task().await {
            Some(PendingTask::Real(tid)) => {
                let task = self.get_task(tid, None).await?;
                Ok(matches!(task.events.last(), Some(TaskEvent::Processing(_))).then(|| task))
            }
            _ => Ok(None),
        }
    }

    pub async fn get_task(&self, id: TaskId, filter: Option<TaskFilter>) -> Result<Task> {
        let store = self.store.clone();
        let task = tokio::task::spawn_blocking(move || -> Result<_> {
            let txn = store.rtxn()?;
            let task = store.get(&txn, id)?;
            Ok(task)
        })
        .await??
        .ok_or(TaskError::UnexistingTask(id))?;

        match filter {
            Some(filter) => filter
                .pass(&task)
                .then(|| task)
                .ok_or(TaskError::UnexistingTask(id)),
            None => Ok(task),
        }
    }

    pub async fn update_tasks(&self, tasks: Vec<PendingTask<Task>>) -> Result<()> {
        let store = self.store.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut txn = store.wtxn()?;

            for task in tasks {
                match task {
                    PendingTask::Real(task) => store.put(&mut txn, &task)?,
                    PendingTask::Ghost(_) => (),
                }
            }

            txn.commit()?;

            Ok(())
        })
        .await??;

        Ok(())
    }

    pub async fn delete_tasks(&self, to_remove: HashSet<TaskId>) -> Result<()> {
        let mut pending_queue = self.pending_queue.write().await;

        // currently retain is not stable: https://doc.rust-lang.org/stable/std/collections/struct.BinaryHeap.html#method.retain
        // pending_queue.retain(|id| !to_remove.contains(&id.0));
        *pending_queue = pending_queue
            .drain()
            .filter(|Reverse(task)| {
                task.get_task_id()
                    // If it's a ghost task we keep it.
                    // If it was a task to delete we remove it.
                    .map_or(true, |id| !to_remove.contains(&id))
            })
            .collect();
        Ok(())
    }

    pub async fn list_tasks(
        &self,
        offset: Option<TaskId>,
        filter: Option<TaskFilter>,
        limit: Option<usize>,
    ) -> Result<Vec<Task>> {
        let store = self.store.clone();

        tokio::task::spawn_blocking(move || {
            let txn = store.rtxn()?;
            let tasks = store.list_tasks(&txn, offset, filter, limit)?;
            Ok(tasks)
        })
        .await?
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    use nelson::Mocker;
    use quickcheck::{Arbitrary, Gen};
    use tempfile::tempdir;

    pub enum MockTaskStore {
        Real(TaskStore),
        Mock(Arc<Mocker>),
    }

    impl Clone for MockTaskStore {
        fn clone(&self) -> Self {
            match self {
                Self::Real(x) => Self::Real(x.clone()),
                Self::Mock(x) => Self::Mock(x.clone()),
            }
        }
    }

    impl MockTaskStore {
        pub fn new(path: impl AsRef<Path>, size: usize) -> Result<Self> {
            Ok(Self::Real(TaskStore::new(path, size)?))
        }

        pub fn mock(mocker: Mocker) -> Self {
            Self::Mock(Arc::new(mocker))
        }

        pub async fn update_tasks(&self, tasks: Vec<PendingTask<Task>>) -> Result<()> {
            match self {
                Self::Real(s) => s.update_tasks(tasks).await,
                Self::Mock(m) => unsafe { m.get::<_, Result<()>>("update_tasks").call(tasks) },
            }
        }

        pub async fn delete_tasks(&self, to_delete: HashSet<TaskId>) -> Result<()> {
            match self {
                Self::Real(s) => s.delete_tasks(to_delete).await,
                Self::Mock(m) => unsafe { m.get::<_, Result<()>>("delete_tasks").call(to_delete) },
            }
        }

        pub async fn get_task(&self, id: TaskId, filter: Option<TaskFilter>) -> Result<Task> {
            match self {
                Self::Real(s) => s.get_task(id, filter).await,
                Self::Mock(m) => unsafe { m.get::<_, Result<Task>>("get_task").call((id, filter)) },
            }
        }

        pub async fn get_processing_task(&self) -> Result<Option<Task>> {
            match self {
                Self::Real(s) => s.get_processing_task().await,
                Self::Mock(m) => unsafe {
                    m.get::<_, Result<Option<Task>>>("get_pending_task")
                        .call(())
                },
            }
        }

        pub async fn peek_pending_task(&self) -> Option<PendingTask<TaskId>> {
            match self {
                Self::Real(s) => s.peek_pending_task().await,
                Self::Mock(m) => unsafe {
                    m.get::<_, Option<PendingTask<TaskId>>>("peek_pending_task")
                        .call(())
                },
            }
        }

        pub async fn list_tasks(
            &self,
            from: Option<TaskId>,
            filter: Option<TaskFilter>,
            limit: Option<usize>,
        ) -> Result<Vec<Task>> {
            match self {
                Self::Real(s) => s.list_tasks(from, filter, limit).await,
                Self::Mock(_m) => todo!(),
            }
        }

        pub async fn register(&self, index_uid: IndexUid, content: TaskContent) -> Result<Task> {
            match self {
                Self::Real(s) => s.register(index_uid, content).await,
                Self::Mock(_m) => todo!(),
            }
        }

        pub async fn register_ghost_task(&self, content: GhostTask) {
            match self {
                Self::Real(s) => s.register_ghost_task(content).await,
                Self::Mock(_m) => todo!(),
            }
        }
    }

    #[test]
    fn test_increment_task_id() {
        let temp_dir = tempdir().unwrap();
        let store = Store::new(temp_dir.path(), 4096 * 100).unwrap();

        let mut txn = store.wtxn().unwrap();
        assert_eq!(store.next_task_id(&mut txn).unwrap(), 0);
        assert_eq!(store.next_task_id(&mut txn).unwrap(), 0);

        let mut g = Gen::new(10);
        let mut task = Task::arbitrary(&mut g);
        task.id = 0;

        store.put(&mut txn, &task).unwrap();

        txn.commit().unwrap();

        let mut txn = store.wtxn().unwrap();
        assert_eq!(store.next_task_id(&mut txn).unwrap(), 1);
    }
}

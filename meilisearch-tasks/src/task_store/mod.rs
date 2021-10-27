mod store;

use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;
use log::debug;

use crate::task::{Task, TaskContent, TaskEvent, TaskId};
use crate::Result;

#[cfg(test)]
pub use store::test::MockStore as Store;
#[cfg(not(test))]
pub use store::Store;

#[derive(Clone)]
pub struct TaskStore {
    store: Arc<Store>,
    pending_queue: Arc<RwLock<VecDeque<TaskId>>>,
}

impl TaskStore {
    pub fn new(path: impl AsRef<Path>, size: usize) -> Result<Self> {
        let store = Arc::new(Store::new(path, size)?);
        let pending_queue = Arc::default();
        Ok(Self { store, pending_queue })
    }

    pub async fn register(&self, index_uid: String, content: TaskContent) -> Result<Task> {
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
        }).await??;


        self.pending_queue.write().await.push_back(task.id);

        Ok(task)
    }

    // Returns the next task to process.
    pub async fn peek_pending(&self) -> Option<TaskId> {
        self.pending_queue.read().await.front().copied()
    }

    pub async fn get_task(&self, id: TaskId) -> Result<Option<Task>> {
        let store = self.store.clone();
        let task = tokio::task::spawn_blocking(move || -> Result<_> {
            let txn = store.rtxn()?;
            let task = store.get(&txn, id)?;
            Ok(task)
        }).await??;

        Ok(task)
    }

    pub async fn update_tasks(&self, tasks: Vec<Task>) -> Result<()> {
        let store = self.store.clone();
        let pending_queue = self.pending_queue.clone();

        let to_remove = tasks.iter().map(|t| t.id).collect::<HashSet<_>>();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut txn = store.wtxn()?;

            for task in tasks {
                store.put(&mut txn, &task)?;
            }

            txn.commit()?;

            Ok(())

        }).await??;

        let mut pending_queue = pending_queue.write().await;
        pending_queue.retain(|id| !to_remove.contains(id));

        Ok(())
    }

    pub async fn list_updates(
        &self,
        filter: Option<impl Fn(&Task) -> bool + Send + Sync +  'static>,
        limit: usize,
        offset: Option<TaskId>,
        ) -> Result<Vec<Task>> {
        let store = self.store.clone();

        tokio::task::spawn_blocking(move || {
            let txn = store.rtxn()?;
            let tasks = store.list_updates(&txn, offset)?
                .filter_map(|t| t.ok())
                .filter(|t| filter.as_ref().map(|f| f(t)).unwrap_or(true))
                .take(limit)
                .collect::<Vec<_>>();
                Ok(tasks)
        }).await?
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    use nelson::Mocker;

    #[derive(Clone)]
    pub enum MockTaskStore {
        Real(TaskStore),
        Mock(Arc<Mocker>),
    }

    impl MockTaskStore {
        pub fn new(path: impl AsRef<Path>, size: usize) -> Result<Self> {
            Ok(Self::Real(TaskStore::new(path, size)?))
        }

        pub fn mock(mocker: Mocker) -> Self {
            Self::Mock(Arc::new(mocker))
        }

        pub async fn update_tasks(&self, tasks: Vec<Task>) -> Result<()> {
            match self {
                Self::Real(s) => s.update_tasks(tasks).await,
                Self::Mock(m) => unsafe { m.get::<_, Result<()>>("update_tasks").call(tasks) },
            }
        }

        pub async fn get_task(&self, id: TaskId) -> Result<Option<Task>> {
            match self {
                Self::Real(s) => s.get_task(id).await,
                Self::Mock(m) => unsafe { m.get::<_, Result<Option<Task>>>("get_task").call(id) },
            }
        }

        pub async fn peek_pending(&self) -> Option<TaskId> {
            match self {
                Self::Real(s) => s.peek_pending().await,
                Self::Mock(m) => unsafe { m.get::<_, Option<TaskId>>("peek_pending").call(()) },
            }
        }
    }
}

mod store;

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use heed::{Env, RwTxn};
use log::debug;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::error::TaskError;
use super::task::{Job, Task, TaskContent, TaskId};
use super::Result;
use crate::index_resolver::IndexUid;
use crate::tasks::task::TaskEvent;
use crate::update_file_store::UpdateFileStore;

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

/// You can't clone a job because of its volatile nature.
/// If you need to take the `Job` with you though. You can call the method
/// `Pending::take`. It'll return the `Pending` as-is but `Empty` the original.
#[derive(Debug, PartialEq)]
pub enum Pending<T> {
    /// A task stored on disk that must be processed.
    Task(T),
    /// Job always have a higher priority over normal tasks and are not stored on disk.
    /// It can be refered as `Volatile job`.
    Job(Job),
}

impl Pending<TaskId> {
    /// Makes a copy of the task or take the content of the volatile job.
    pub(crate) fn take(&mut self) -> Self {
        match self {
            Self::Task(id) => Self::Task(*id),
            Self::Job(job) => Self::Job(job.take()),
        }
    }
}

impl Eq for Pending<TaskId> {}

impl PartialOrd for Pending<TaskId> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // in case of two tasks we want to return the lowest taskId first.
            (Pending::Task(lhs), Pending::Task(rhs)) => Some(lhs.cmp(rhs).reverse()),
            // A job is always better than a task.
            (Pending::Task(_), Pending::Job(_)) => Some(Ordering::Less),
            (Pending::Job(_), Pending::Task(_)) => Some(Ordering::Greater),
            // When there is two jobs we consider them equals.
            (Pending::Job(_), Pending::Job(_)) => Some(Ordering::Equal),
        }
    }
}

impl Pending<Task> {
    pub fn get_content_uuid(&self) -> Option<Uuid> {
        match self {
            Pending::Task(task) => task.get_content_uuid(),
            _ => None,
        }
    }
}

impl Ord for Pending<TaskId> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct TaskStore {
    store: Arc<Store>,
    pending_queue: Arc<RwLock<BinaryHeap<Pending<TaskId>>>>,
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
    pub fn new(env: Arc<heed::Env>) -> Result<Self> {
        let mut store = Store::new(env)?;
        let unfinished_tasks = store.reset_and_return_unfinished_tasks()?;
        let store = Arc::new(store);

        Ok(Self {
            store,
            pending_queue: Arc::new(RwLock::new(unfinished_tasks)),
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
            .push(Pending::Task(task.id));

        Ok(task)
    }

    pub fn register_raw_update(&self, wtxn: &mut RwTxn, task: &Task) -> Result<()> {
        self.store.put(wtxn, task)?;
        Ok(())
    }

    /// Register an update that applies on multiple indexes.
    /// Currently the update is considered as a priority.
    pub async fn register_job(&self, content: Job) {
        debug!("registering a job: {:?}", content);
        self.pending_queue.write().await.push(Pending::Job(content));
    }

    /// Returns the next task to process.
    pub async fn peek_pending_task(&self) -> Option<Pending<TaskId>> {
        let mut pending_queue = self.pending_queue.write().await;
        loop {
            match pending_queue.peek()? {
                Pending::Job(Job::Empty) => drop(pending_queue.pop()),
                _ => return Some(pending_queue.peek_mut()?.take()),
            }
        }
    }

    /// Returns the next task to process if there is one.
    pub async fn get_processing_task(&self) -> Result<Option<Task>> {
        match self.peek_pending_task().await {
            Some(Pending::Task(tid)) => {
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

    pub async fn update_tasks(&self, tasks: Vec<Pending<Task>>) -> Result<Vec<Pending<Task>>> {
        let store = self.store.clone();

        let tasks = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut txn = store.wtxn()?;

            for task in &tasks {
                match task {
                    Pending::Task(task) => store.put(&mut txn, task)?,
                    Pending::Job(_) => (),
                }
            }

            txn.commit()?;

            Ok(tasks)
        })
        .await??;

        Ok(tasks)
    }

    /// Delete one task from the queue and remove all `Empty` job.
    pub async fn delete_pending(&self, to_delete: &Pending<Task>) {
        if let Pending::Task(Task { id: pending_id, .. }) = to_delete {
            let mut pending_queue = self.pending_queue.write().await;
            *pending_queue = std::mem::take(&mut *pending_queue)
                .into_iter()
                .filter(|pending| match pending {
                    Pending::Job(Job::Empty) => false,
                    Pending::Task(id) => pending_id != id,
                    _ => true,
                })
                .collect::<BinaryHeap<Pending<TaskId>>>();
        }
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

    pub async fn dump(
        &self,
        dir_path: impl AsRef<Path>,
        update_file_store: UpdateFileStore,
    ) -> Result<()> {
        let update_dir = dir_path.as_ref().join("updates");
        let updates_file = update_dir.join("data.jsonl");
        let tasks = self.list_tasks(None, None, None).await?;

        let dir_path = dir_path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || -> Result<()> {
            std::fs::create_dir(&update_dir)?;
            let updates_file = std::fs::File::create(updates_file)?;
            let mut updates_file = BufWriter::new(updates_file);

            for task in tasks {
                serde_json::to_writer(&mut updates_file, &task)?;
                updates_file.write_all(b"\n")?;

                if !task.is_finished() {
                    if let Some(content_uuid) = task.get_content_uuid() {
                        update_file_store.dump(content_uuid, &dir_path)?;
                    }
                }
            }
            updates_file.flush()?;
            Ok(())
        })
        .await??;

        Ok(())
    }

    pub fn load_dump(src: impl AsRef<Path>, env: Arc<Env>) -> anyhow::Result<()> {
        // create a dummy update field store, since it is not needed right now.
        let store = Self::new(env.clone())?;

        let src_update_path = src.as_ref().join("updates");
        let update_data = std::fs::File::open(&src_update_path.join("data.jsonl"))?;
        let update_data = std::io::BufReader::new(update_data);

        let stream = serde_json::Deserializer::from_reader(update_data).into_iter::<Task>();

        let mut wtxn = env.write_txn()?;
        for entry in stream {
            store.register_raw_update(&mut wtxn, &entry?)?;
        }
        wtxn.commit()?;

        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use crate::tasks::task_store::store::test::tmp_env;

    use super::*;

    use nelson::Mocker;
    use proptest::{
        strategy::Strategy,
        test_runner::{Config, TestRunner},
    };

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
        pub fn new(env: Arc<heed::Env>) -> Result<Self> {
            Ok(Self::Real(TaskStore::new(env)?))
        }

        pub fn mock(mocker: Mocker) -> Self {
            Self::Mock(Arc::new(mocker))
        }

        pub async fn update_tasks(&self, tasks: Vec<Pending<Task>>) -> Result<Vec<Pending<Task>>> {
            match self {
                Self::Real(s) => s.update_tasks(tasks).await,
                Self::Mock(m) => unsafe {
                    m.get::<_, Result<Vec<Pending<Task>>>>("update_tasks")
                        .call(tasks)
                },
            }
        }

        pub async fn delete_pending(&self, to_delete: &Pending<Task>) {
            match self {
                Self::Real(s) => s.delete_pending(to_delete).await,
                Self::Mock(m) => unsafe { m.get("delete_pending").call(to_delete) },
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

        pub async fn peek_pending_task(&self) -> Option<Pending<TaskId>> {
            match self {
                Self::Real(s) => s.peek_pending_task().await,
                Self::Mock(m) => unsafe {
                    m.get::<_, Option<Pending<TaskId>>>("peek_pending_task")
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

        pub async fn dump(&self, path: &Path, update_file_store: UpdateFileStore) -> Result<()> {
            match self {
                Self::Real(s) => s.dump(path, update_file_store).await,
                Self::Mock(_m) => todo!(),
            }
        }

        pub async fn register(&self, index_uid: IndexUid, content: TaskContent) -> Result<Task> {
            match self {
                Self::Real(s) => s.register(index_uid, content).await,
                Self::Mock(_m) => todo!(),
            }
        }

        pub fn register_raw_update(&self, wtxn: &mut RwTxn, task: &Task) -> Result<()> {
            match self {
                Self::Real(s) => s.register_raw_update(wtxn, task),
                Self::Mock(_m) => todo!(),
            }
        }

        pub async fn register_job(&self, content: Job) {
            match self {
                Self::Real(s) => s.register_job(content).await,
                Self::Mock(_m) => todo!(),
            }
        }

        pub fn load_dump(path: impl AsRef<Path>, env: Arc<Env>) -> anyhow::Result<()> {
            TaskStore::load_dump(path, env)
        }
    }

    #[test]
    fn test_increment_task_id() {
        let tmp = tmp_env();
        let store = Store::new(tmp.env()).unwrap();

        let mut txn = store.wtxn().unwrap();
        assert_eq!(store.next_task_id(&mut txn).unwrap(), 0);
        txn.abort().unwrap();

        let gen_task = |id: TaskId| Task {
            id,
            index_uid: IndexUid::new_unchecked("test"),
            content: TaskContent::IndexCreation { primary_key: None },
            events: Vec::new(),
        };

        let mut runner = TestRunner::new(Config::default());
        runner
            .run(&(0..100u64).prop_map(gen_task), |task| {
                let mut txn = store.wtxn().unwrap();
                let previous_id = store.next_task_id(&mut txn).unwrap();

                store.put(&mut txn, &task).unwrap();

                let next_id = store.next_task_id(&mut txn).unwrap();

                // if we put a task whose task_id is less than the next_id, then the next_id remains
                // unchanged, otherwise it becomes task.id + 1
                if task.id < previous_id {
                    assert_eq!(next_id, previous_id)
                } else {
                    assert_eq!(next_id, task.id + 1);
                }

                txn.commit().unwrap();

                Ok(())
            })
            .unwrap();
    }
}

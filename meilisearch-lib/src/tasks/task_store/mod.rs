mod store;

use std::collections::HashSet;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use heed::{Env, RwTxn};
use log::debug;

use super::error::TaskError;
use super::task::{Task, TaskContent, TaskId};
use super::Result;
use crate::index_resolver::IndexUid;
use crate::tasks::task::TaskEvent;
use crate::update_file_store::UpdateFileStore;

#[cfg(test)]
pub use store::test::MockStore as Store;
#[cfg(not(test))]
pub use store::Store;

/// Defines constraints to be applied when querying for Tasks from the store.
#[derive(Default)]
pub struct TaskFilter {
    indexes: Option<HashSet<String>>,
    filter_fn: Option<Box<dyn Fn(&Task) -> bool + Sync + Send + 'static>>,
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

    pub fn filter_fn(&mut self, f: impl Fn(&Task) -> bool + Sync + Send + 'static) {
        self.filter_fn.replace(Box::new(f));
    }
}

pub struct TaskStore {
    store: Arc<Store>,
}

impl Clone for TaskStore {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl TaskStore {
    pub fn new(env: Arc<heed::Env>) -> Result<Self> {
        let store = Arc::new(Store::new(env)?);
        Ok(Self { store })
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

        Ok(task)
    }

    pub fn register_raw_update(&self, wtxn: &mut RwTxn, task: &Task) -> Result<()> {
        self.store.put(wtxn, task)?;
        Ok(())
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

    pub async fn get_pending_tasks(&self, ids: Vec<TaskId>) -> Result<(Vec<TaskId>, Vec<Task>)> {
        let store = self.store.clone();
        let tasks = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut tasks = Vec::new();
            let txn = store.rtxn()?;

            for id in ids.iter() {
                let task = store
                    .get(&txn, *id)?
                    .ok_or(TaskError::UnexistingTask(*id))?;
                tasks.push(task);
            }
            Ok((ids, tasks))
        })
        .await??;

        Ok(tasks)
    }

    pub async fn update_tasks(&self, tasks: Vec<Task>) -> Result<Vec<Task>> {
        let store = self.store.clone();

        let tasks = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut txn = store.wtxn()?;

            for task in &tasks {
                store.put(&mut txn, task)?;
            }

            txn.commit()?;

            Ok(tasks)
        })
        .await??;

        Ok(tasks)
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

        pub async fn update_tasks(&self, tasks: Vec<Task>) -> Result<Vec<Task>> {
            match self {
                Self::Real(s) => s.update_tasks(tasks).await,
                Self::Mock(m) => unsafe {
                    m.get::<_, Result<Vec<Task>>>("update_tasks").call(tasks)
                },
            }
        }

        pub async fn get_task(&self, id: TaskId, filter: Option<TaskFilter>) -> Result<Task> {
            match self {
                Self::Real(s) => s.get_task(id, filter).await,
                Self::Mock(m) => unsafe { m.get::<_, Result<Task>>("get_task").call((id, filter)) },
            }
        }

        pub async fn get_pending_tasks(
            &self,
            tasks: Vec<TaskId>,
        ) -> Result<(Vec<TaskId>, Vec<Task>)> {
            match self {
                Self::Real(s) => s.get_pending_tasks(tasks).await,
                Self::Mock(m) => unsafe { m.get("get_pending_task").call(tasks) },
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
                Self::Mock(m) => unsafe { m.get("list_tasks").call((from, filter, limit)) },
            }
        }

        pub async fn dump(
            &self,
            path: impl AsRef<Path>,
            update_file_store: UpdateFileStore,
        ) -> Result<()> {
            match self {
                Self::Real(s) => s.dump(path, update_file_store).await,
                Self::Mock(m) => unsafe { m.get("dump").call((path, update_file_store)) },
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

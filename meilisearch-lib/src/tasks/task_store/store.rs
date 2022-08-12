#[allow(clippy::upper_case_acronyms)]

type BEU32 = milli::heed::zerocopy::U32<milli::heed::byteorder::BE>;

const INDEX_UIDS_TASK_IDS: &str = "index-uids-task-ids";
const TASKS: &str = "tasks";

use std::collections::HashSet;
use std::ops::Bound::{Excluded, Unbounded};
use std::result::Result as StdResult;
use std::sync::Arc;

use milli::heed::types::{OwnedType, SerdeJson, Str};
use milli::heed::{Database, Env, RoTxn, RwTxn};
use milli::heed_codec::RoaringBitmapCodec;
use roaring::RoaringBitmap;

use crate::tasks::task::{Task, TaskId};

use super::super::Result;
use super::TaskFilter;

pub struct Store {
    env: Arc<Env>,
    /// Maps an index uid to the set of tasks ids associated to it.
    index_uid_task_ids: Database<Str, RoaringBitmapCodec>,
    tasks: Database<OwnedType<BEU32>, SerdeJson<Task>>,
}

impl Drop for Store {
    fn drop(&mut self) {
        if Arc::strong_count(&self.env) == 1 {
            self.env.as_ref().clone().prepare_for_closing();
        }
    }
}

impl Store {
    /// Create a new store from the specified `Path`.
    /// Be really cautious when calling this function, the returned `Store` may
    /// be in an invalid state, with dangling processing tasks.
    /// You want to patch  all un-finished tasks and put them in your pending
    /// queue with the `reset_and_return_unfinished_update` method.
    pub fn new(env: Arc<milli::heed::Env>) -> Result<Self> {
        let index_uid_task_ids = env.create_database(Some(INDEX_UIDS_TASK_IDS))?;
        let tasks = env.create_database(Some(TASKS))?;

        Ok(Self {
            env,
            index_uid_task_ids,
            tasks,
        })
    }

    pub fn wtxn(&self) -> Result<RwTxn> {
        Ok(self.env.write_txn()?)
    }

    pub fn rtxn(&self) -> Result<RoTxn> {
        Ok(self.env.read_txn()?)
    }

    /// Returns the id for the next task.
    ///
    /// The required `mut txn` acts as a reservation system. It guarantees that as long as you commit
    /// the task to the store in the same transaction, no one else will hav this task id.
    pub fn next_task_id(&self, txn: &mut RwTxn) -> Result<TaskId> {
        let id = self
            .tasks
            .lazily_decode_data()
            .last(txn)?
            .map(|(id, _)| id.get() + 1)
            .unwrap_or(0);
        Ok(id)
    }

    pub fn put(&self, txn: &mut RwTxn, task: &Task) -> Result<()> {
        self.tasks.put(txn, &BEU32::new(task.id), task)?;
        // only add the task to the indexes index if it has an index_uid
        if let Some(index_uid) = task.index_uid() {
            let mut tasks_set = self
                .index_uid_task_ids
                .get(txn, index_uid)?
                .unwrap_or_default();

            tasks_set.insert(task.id);

            self.index_uid_task_ids.put(txn, index_uid, &tasks_set)?;
        }

        Ok(())
    }

    pub fn get(&self, txn: &RoTxn, id: TaskId) -> Result<Option<Task>> {
        let task = self.tasks.get(txn, &BEU32::new(id))?;
        Ok(task)
    }

    /// Returns the unfinished tasks starting from the given taskId in ascending order.
    pub fn fetch_unfinished_tasks(&self, txn: &RoTxn, from: Option<TaskId>) -> Result<Vec<Task>> {
        // We must NEVER re-enqueue an already processed task! It's content uuid would point to an unexisting file.
        //
        // TODO(marin): This may create some latency when the first batch lazy loads the pending updates.
        let from = from.unwrap_or_default();

        let result: StdResult<Vec<_>, milli::heed::Error> = self
            .tasks
            .range(txn, &(BEU32::new(from)..))?
            .map(|r| r.map(|(_, t)| t))
            .filter(|result| result.as_ref().map_or(true, |t| !t.is_finished()))
            .collect();

        result.map_err(Into::into)
    }

    /// Returns all the tasks starting from the given taskId and going in descending order.
    pub fn list_tasks(
        &self,
        txn: &RoTxn,
        from: Option<TaskId>,
        filter: Option<TaskFilter>,
        limit: Option<usize>,
    ) -> Result<Vec<Task>> {
        let from = match from {
            Some(from) => from,
            None => self.tasks.last(txn)?.map_or(0, |(id, _)| id.get()),
        };

        let filter_fn = |task: &Task| {
            filter
                .as_ref()
                .and_then(|f| f.filter_fn.as_ref())
                .map_or(true, |f| f(task))
        };

        let result: Result<Vec<_>> = match filter.as_ref().and_then(|f| f.filtered_indexes()) {
            Some(indexes) => self
                .compute_candidates(txn, indexes, from)?
                .filter(|result| result.as_ref().map_or(true, filter_fn))
                .take(limit.unwrap_or(usize::MAX))
                .collect(),
            None => self
                .tasks
                .rev_range(txn, &(..=BEU32::new(from)))?
                .map(|r| r.map(|(_, t)| t).map_err(Into::into))
                .filter(|result| result.as_ref().map_or(true, filter_fn))
                .take(limit.unwrap_or(usize::MAX))
                .collect(),
        };

        result.map_err(Into::into)
    }

    fn compute_candidates<'a>(
        &'a self,
        txn: &'a RoTxn,
        indexes: &HashSet<String>,
        from: TaskId,
    ) -> Result<impl Iterator<Item = Result<Task>> + 'a> {
        let mut candidates = RoaringBitmap::new();

        for index_uid in indexes {
            if let Some(tasks_set) = self.index_uid_task_ids.get(txn, index_uid)? {
                candidates |= tasks_set;
            }
        }

        candidates.remove_range((Excluded(from), Unbounded));

        let iter = candidates
            .into_iter()
            .rev()
            .filter_map(|id| self.get(txn, id).transpose());

        Ok(iter)
    }
}

#[cfg(test)]
pub mod test {
    use itertools::Itertools;
    use meilisearch_types::index_uid::IndexUid;
    use milli::heed::EnvOpenOptions;
    use nelson::Mocker;
    use tempfile::TempDir;

    use crate::tasks::task::TaskContent;

    use super::*;

    /// TODO: use this mock to test the task store properly.
    #[allow(dead_code)]
    pub enum MockStore {
        Real(Store),
        Fake(Mocker),
    }

    pub struct TmpEnv(TempDir, Arc<milli::heed::Env>);

    impl TmpEnv {
        pub fn env(&self) -> Arc<milli::heed::Env> {
            self.1.clone()
        }
    }

    pub fn tmp_env() -> TmpEnv {
        let tmp = tempfile::tempdir().unwrap();

        let mut options = EnvOpenOptions::new();
        options.map_size(4096 * 100000);
        options.max_dbs(1000);
        let env = Arc::new(options.open(tmp.path()).unwrap());

        TmpEnv(tmp, env)
    }

    impl MockStore {
        pub fn new(env: Arc<milli::heed::Env>) -> Result<Self> {
            Ok(Self::Real(Store::new(env)?))
        }

        pub fn wtxn(&self) -> Result<RwTxn> {
            match self {
                MockStore::Real(index) => index.wtxn(),
                MockStore::Fake(_) => todo!(),
            }
        }

        pub fn rtxn(&self) -> Result<RoTxn> {
            match self {
                MockStore::Real(index) => index.rtxn(),
                MockStore::Fake(_) => todo!(),
            }
        }

        pub fn next_task_id(&self, txn: &mut RwTxn) -> Result<TaskId> {
            match self {
                MockStore::Real(index) => index.next_task_id(txn),
                MockStore::Fake(_) => todo!(),
            }
        }

        pub fn put(&self, txn: &mut RwTxn, task: &Task) -> Result<()> {
            match self {
                MockStore::Real(index) => index.put(txn, task),
                MockStore::Fake(_) => todo!(),
            }
        }

        pub fn get(&self, txn: &RoTxn, id: TaskId) -> Result<Option<Task>> {
            match self {
                MockStore::Real(index) => index.get(txn, id),
                MockStore::Fake(_) => todo!(),
            }
        }

        pub fn fetch_unfinished_tasks(
            &self,
            txn: &RoTxn,
            from: Option<TaskId>,
        ) -> Result<Vec<Task>> {
            match self {
                MockStore::Real(index) => index.fetch_unfinished_tasks(txn, from),
                MockStore::Fake(_) => todo!(),
            }
        }

        pub fn list_tasks(
            &self,
            txn: &RoTxn,
            from: Option<TaskId>,
            filter: Option<TaskFilter>,
            limit: Option<usize>,
        ) -> Result<Vec<Task>> {
            match self {
                MockStore::Real(index) => index.list_tasks(txn, from, filter, limit),
                MockStore::Fake(_) => todo!(),
            }
        }
    }

    #[test]
    fn test_ordered_filtered_updates() {
        let tmp = tmp_env();
        let store = Store::new(tmp.env()).unwrap();

        let tasks = (0..100)
            .map(|_| Task {
                id: rand::random(),
                content: TaskContent::IndexDeletion {
                    index_uid: IndexUid::new_unchecked("test"),
                },
                events: vec![],
            })
            .collect::<Vec<_>>();

        let mut txn = store.env.write_txn().unwrap();
        tasks
            .iter()
            .try_for_each(|t| store.put(&mut txn, t))
            .unwrap();

        let mut filter = TaskFilter::default();
        filter.filter_index("test".into());

        let tasks = store.list_tasks(&txn, None, Some(filter), None).unwrap();

        assert!(tasks
            .iter()
            .map(|t| t.id)
            .tuple_windows()
            .all(|(a, b)| a > b));
    }

    #[test]
    fn test_filter_same_index_prefix() {
        let tmp = tmp_env();
        let store = Store::new(tmp.env()).unwrap();

        let task_1 = Task {
            id: 1,
            content: TaskContent::IndexDeletion {
                index_uid: IndexUid::new_unchecked("test"),
            },
            events: vec![],
        };

        let task_2 = Task {
            id: 0,
            content: TaskContent::IndexDeletion {
                index_uid: IndexUid::new_unchecked("test1"),
            },
            events: vec![],
        };

        let mut txn = store.wtxn().unwrap();
        store.put(&mut txn, &task_1).unwrap();
        store.put(&mut txn, &task_2).unwrap();

        let mut filter = TaskFilter::default();
        filter.filter_index("test".into());

        let tasks = store.list_tasks(&txn, None, Some(filter), None).unwrap();

        txn.abort().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks.first().as_ref().unwrap().index_uid().unwrap(), "test");

        // same thing but invert the ids
        let task_1 = Task {
            id: 0,
            content: TaskContent::IndexDeletion {
                index_uid: IndexUid::new_unchecked("test"),
            },
            events: vec![],
        };
        let task_2 = Task {
            id: 1,
            content: TaskContent::IndexDeletion {
                index_uid: IndexUid::new_unchecked("test1"),
            },
            events: vec![],
        };

        let mut txn = store.wtxn().unwrap();
        store.put(&mut txn, &task_1).unwrap();
        store.put(&mut txn, &task_2).unwrap();

        let mut filter = TaskFilter::default();
        filter.filter_index("test".into());

        let tasks = store.list_tasks(&txn, None, Some(filter), None).unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks.first().as_ref().unwrap().index_uid().unwrap(), "test");
    }
}

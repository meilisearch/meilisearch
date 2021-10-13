use std::borrow::Cow;
use std::collections::{HashSet, VecDeque};
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use heed::types::{OwnedType, SerdeBincode, Unit};
use heed::{BytesDecode, BytesEncode, Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use tokio::sync::RwLock;

use crate::task::{Task, TaskContent, TaskEvent, TaskId};
use crate::Result;

#[allow(clippy::upper_case_acronyms)]
type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;

const UID_TASK_IDS: &str = "uid_task_id";
const TASKS: &str = "tasks";

enum IndexUidTaskIdCodec {}

impl<'a> BytesEncode<'a> for IndexUidTaskIdCodec {
    type EItem = (&'a str, TaskId);

    fn bytes_encode((s, id): &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let size = s.len() + std::mem::size_of::<TaskId>() + 1;
        if size > 512 {
            return None
        }
        let mut b = Vec::with_capacity(size);
        b.extend_from_slice(s.as_bytes());
        // null terminate the string
        b.push(0);
        b.extend_from_slice(&id.to_be_bytes());
        Some(Cow::Owned(b))
    }
}

impl<'a> BytesDecode<'a> for IndexUidTaskIdCodec {
    type DItem = (&'a str, TaskId);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let str_end = bytes.iter().position(|&it| it == 0)?;
        let str_bytes = &bytes[..str_end];
        let str = std::str::from_utf8(str_bytes).ok()?;
        let id = TaskId::from_be_bytes(bytes[str_end + 1..].try_into().ok()?);
        Some((str, id))
    }
}

struct Store {
    env: Env,
    uids_task_ids: Database<IndexUidTaskIdCodec, Unit>,
    tasks: Database<OwnedType<BEU32>, SerdeBincode<Task>>,
}

impl Store {
    fn new(path: impl AsRef<Path>, size: usize) -> Result<Self> {
        let mut options = EnvOpenOptions::new();
        options.map_size(size);
        options.max_dbs(1000);
        let env = options.open(path)?;

        let uids_task_ids = env.create_database(Some(UID_TASK_IDS))?;
        let tasks = env.create_database(Some(TASKS))?;

        Ok(Self {
            env,
            uids_task_ids,
            tasks,
        })

    }

    fn wtxn(&self) -> Result<RwTxn> {
        Ok(self.env.write_txn()?)
    }

    fn rtxn(&self) -> Result<RoTxn> {
        Ok(self.env.read_txn()?)
    }

    fn next_task_id(&self, txn: &mut RwTxn) -> Result<TaskId> {
        let id = self
            .tasks
            .lazily_decode_data()
            .last(txn)?
            .map(|(id, _)| id.get())
            .unwrap_or(0);
        Ok(id)
    }

    fn put(&self, txn: &mut RwTxn, task: &Task) -> Result<()> {
        self.tasks.put(txn, &BEU32::new(task.id), task)?;
        self.uids_task_ids.put(txn, &(&task.index_uid, task.id), &())?;

        Ok(())
    }

    fn get(&self, txn: &RoTxn, id: TaskId) -> Result<Option<Task>> {
        let task = self.tasks.get(txn, &BEU32::new(id))?;
        Ok(task)
    }

    fn task_count(&self, txn: &RoTxn) -> Result<usize> {
        Ok(self.tasks.len(txn)?)
    }

    fn list_updates<'a>(&self, txn: &'a RoTxn, from: Option<TaskId>) -> Result<Box<dyn Iterator<Item = heed::Result<Task>> + 'a>> {
        match from {
            Some(id) => Ok(Box::new(self.tasks.rev_range(txn, &(..BEU32::new(id)))?.map(|r| r.map(|(_, t)| t)))),
            None => Ok(Box::new(self.tasks.rev_iter(txn)?.map(|r| r.map(|(_, t)| t)))),
        }
    }
}

pub struct TaskStore {
    store: Arc<Store>,
    pending_queue: Arc<RwLock<VecDeque<TaskId>>>,
}

impl TaskStore {
    pub async fn register(&self, index_uid: String, content: TaskContent) -> Result<Task> {
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
mod test {
    use std::collections::{BTreeMap, HashMap};

    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use super::*;

    #[quickcheck]
    fn put_retrieve_task(tasks: Vec<Task>) -> TestResult {
        // if two task have the same id, we discard the test.
        if tasks.iter().map(|t| t.id).collect::<HashSet<_>>().len() != tasks.len() {
            return TestResult::discard()
        }
        let tmp = tempfile::tempdir().unwrap();

        let store = Store::new(tmp.path(), 4096 * 10000000).unwrap();

        let mut txn = store.wtxn().unwrap();

        for task in tasks.iter() {
            store.put(&mut txn, task).unwrap();
        }

        txn.commit().unwrap();

        let txn = store.rtxn().unwrap();

        assert_eq!(store.task_count(&txn).unwrap(), tasks.len());

        for task in tasks {
            let found_task = store.get(&txn, task.id).unwrap().unwrap();
            assert_eq!(found_task, task);
        }

        TestResult::passed()
    }

    #[quickcheck]
    fn list_updates(tasks: Vec<Task>) -> TestResult {
        // if two task have the same id, we discard the test.
        if tasks.is_empty() || tasks.iter().map(|t| t.id).collect::<HashSet<_>>().len() != tasks.len() {
            return TestResult::discard()
        }

        // if two task have the same id, we discard the test.
        if tasks.iter().map(|t| t.id).collect::<HashSet<_>>().len() != tasks.len() {
            return TestResult::discard()
        }
        let tmp = tempfile::tempdir().unwrap();

        let store = Store::new(tmp.path(), 4096 * 100000).unwrap();

        let mut txn = store.wtxn().unwrap();

        for task in tasks.iter() {
            store.put(&mut txn, task).unwrap();
        }

        txn.commit().unwrap();

        let txn = store.rtxn().unwrap();
        let validator = tasks.into_iter().map(|t| (t.id, t)).collect::<HashMap<_, _>>();

        assert_eq!(store.task_count(&txn).unwrap(), validator.len());

        let iter = store.list_updates(&txn, None)
            .unwrap()
            .map(|t| t.unwrap())
            .map(|t| (t.id, t))
            .collect::<HashMap<_, _>>();

        assert_eq!(iter, validator);

        let randid = validator.values().next().unwrap().id;

        store.list_updates(&txn, Some(randid))
            .unwrap()
            .map(|t| t.unwrap())
            .for_each(|t| assert!(t.id < randid, "id: {}, randid: {}", t.id, randid));

        TestResult::passed()
    }
}

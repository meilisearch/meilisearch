
#[allow(clippy::upper_case_acronyms)]
type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;

const UID_TASK_IDS: &str = "uid_task_id";
const TASKS: &str = "tasks";

use std::{borrow::Cow, convert::TryInto, path::Path};

use heed::{BytesDecode, BytesEncode, Database, Env, EnvOpenOptions, RoTxn, RwTxn, types::{OwnedType, SerdeBincode, Unit}};

use crate::task::{Task, TaskId};
use crate::Result;


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

pub struct Store {
    env: Env,
    uids_task_ids: Database<IndexUidTaskIdCodec, Unit>,
    tasks: Database<OwnedType<BEU32>, SerdeBincode<Task>>,
}

impl Store {
    pub fn new(path: impl AsRef<Path>, size: usize) -> Result<Self> {
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

    pub fn wtxn(&self) -> Result<RwTxn> {
        Ok(self.env.write_txn()?)
    }

    pub fn rtxn(&self) -> Result<RoTxn> {
        Ok(self.env.read_txn()?)
    }

    pub fn next_task_id(&self, txn: &mut RwTxn) -> Result<TaskId> {
        let id = self
            .tasks
            .lazily_decode_data()
            .last(txn)?
            .map(|(id, _)| id.get())
            .unwrap_or(0);
        Ok(id)
    }

    pub fn put(&self, txn: &mut RwTxn, task: &Task) -> Result<()> {
        self.tasks.put(txn, &BEU32::new(task.id), task)?;
        self.uids_task_ids.put(txn, &(&task.index_uid, task.id), &())?;

        Ok(())
    }

    pub fn get(&self, txn: &RoTxn, id: TaskId) -> Result<Option<Task>> {
        let task = self.tasks.get(txn, &BEU32::new(id))?;
        Ok(task)
    }

    pub fn task_count(&self, txn: &RoTxn) -> Result<usize> {
        Ok(self.tasks.len(txn)?)
    }

    pub fn list_updates<'a>(&self, txn: &'a RoTxn, from: Option<TaskId>) -> Result<Box<dyn Iterator<Item = heed::Result<Task>> + 'a>> {
        match from {
            Some(id) => Ok(Box::new(self.tasks.rev_range(txn, &(..BEU32::new(id)))?.map(|r| r.map(|(_, t)| t)))),
            None => Ok(Box::new(self.tasks.rev_iter(txn)?.map(|r| r.map(|(_, t)| t)))),
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;
    use nelson::Mocker;

    use super::*;

    pub enum MockStore {
        Real(Store),
        Fake(Mocker),
    }

    impl MockStore {
        pub fn new(path: impl AsRef<Path>, size: usize) -> Result<Self> {
            Ok(Self::Real(Store::new(path, size)?))
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

        pub fn task_count(&self, txn: &RoTxn) -> Result<usize> {
            match self {
                MockStore::Real(index) => index.task_count(txn),
                MockStore::Fake(_) => todo!(),
            }
        }

        pub fn list_updates<'a>(&self, txn: &'a RoTxn, from: Option<TaskId>) -> Result<Box<dyn Iterator<Item = heed::Result<Task>> + 'a>> {
            match self {
                MockStore::Real(index) => index.list_updates(txn, from),
                MockStore::Fake(_) => todo!(),
            }
        }
    }

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

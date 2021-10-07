use chrono::Utc;
use heed::types::{OwnedType, SerdeBincode};
use heed::{Database, Env, RwTxn};

use crate::task::{Task, TaskContent, TaskEvent, TaskId};
use crate::Result;

struct Store {
    env: Env,
    db: Database<OwnedType<TaskId>, SerdeBincode<Task>>,
}

impl Store {
    fn wtxn(&self) -> Result<RwTxn> {
        Ok(self.env.write_txn()?)
    }

    fn next_task_id(&self, txn: &mut RwTxn) -> Result<TaskId> {
        let id = self
            .db
            .lazily_decode_data()
            .last(txn)?
            .map(|(id, _)| id)
            .unwrap_or(0);
        Ok(id)
    }

    fn put(&self, txn: &mut RwTxn, task: &Task) -> Result<()> {
        let id = task.id;
        self.db.put(txn, &id, &task)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct TaskStore {
    store: Store,
}

impl TaskStore {
    pub fn register(&self, content: TaskContent) -> Result<Task> {
        let mut txn = self.store.wtxn()?;
        let next_task_id = self.store.next_task_id(&mut txn)?;
        let created_at = TaskEvent::Created(Utc::now());
        let task = Task {
            id: next_task_id,
            content,
            events: vec![created_at],
        };

        self.store.put(&mut txn, &task)?;

        Ok(task)
    }
}

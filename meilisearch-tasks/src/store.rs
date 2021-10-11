use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use chrono::Utc;
use heed::types::{OwnedType, SerdeBincode};
use heed::{Database, Env, RoTxn, RwTxn};
use tokio::sync::RwLock;

use crate::task::{Task, TaskContent, TaskEvent, TaskId};
use crate::Result;

struct Store {
    env: Env,
    // TODO: change to BE type
    db: Database<OwnedType<TaskId>, SerdeBincode<Task>>,
}

impl Store {
    fn wtxn(&self) -> Result<RwTxn> {
        Ok(self.env.write_txn()?)
    }

    fn rtxn(&self) -> Result<RoTxn> {
        Ok(self.env.read_txn()?)
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

    fn get(&self, txn: &RoTxn, id: TaskId) -> Result<Option<Task>> {
        let task = self.db.get(txn, &id)?;
        Ok(task)
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
}

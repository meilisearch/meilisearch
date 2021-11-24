use chrono::{DateTime, Utc};

use crate::index_resolver::IndexUid;

use super::{task::Task, task_store::PendingTask};

pub type BatchId = u32;

#[derive(Debug)]
pub struct Batch {
    pub id: BatchId,
    // pub index_uid: IndexUid,
    pub created_at: DateTime<Utc>,
    pub tasks: Vec<PendingTask<Task>>,
}

impl Batch {
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

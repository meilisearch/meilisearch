use chrono::{DateTime, Utc};

use crate::task::Task;

pub type BatchId = u32;

#[derive(Debug)]
pub struct Batch {
    pub id: BatchId,
    pub index_uid: String,
    pub created_at: DateTime<Utc>,
    pub tasks: Vec<Task>,
}

impl Batch {
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

use chrono::{DateTime, Utc};

use super::task::Task;

pub type BatchId = u64;

#[derive(Debug)]
pub struct Batch {
    pub id: BatchId,
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

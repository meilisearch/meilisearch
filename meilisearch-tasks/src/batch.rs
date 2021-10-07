use chrono::{DateTime, Utc};

use crate::task::Task;

pub type BatchId = u32;

pub struct Batch {
    id: BatchId,
    index_uid: String,
    created_at: DateTime<Utc>,
    tasks: Vec<Task>,
}

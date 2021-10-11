use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

use crate::batch::BatchId;

pub type TaskId = u32;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskEvent {
    Created(DateTime<Utc>),
    Batched {
        timestamp: DateTime<Utc>,
        batch_id: BatchId,
    },
    Processing(DateTime<Utc>),
    Processed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub index_uid: String,
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskContent {
    ClearIndex,
}

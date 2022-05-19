use time::OffsetDateTime;

use crate::snapshot::SnapshotJob;

use super::task::{Task, TaskEvent};

pub type BatchId = u64;

#[derive(Debug)]
pub enum BatchContent {
    DocumentAddtitionBatch(Vec<Task>),
    IndexUpdate(Task),
    Dump(Task),
    Snapshot(SnapshotJob),
    // Symbolizes a empty batch. This can occur when we were woken, but there wasn't any work to do.
    Empty,
}

impl BatchContent {
    pub fn first(&self) -> Option<&Task> {
        match self {
            BatchContent::DocumentAddtitionBatch(ts) => ts.first(),
            BatchContent::Dump(t) | BatchContent::IndexUpdate(t) => Some(t),
            BatchContent::Snapshot(_) | BatchContent::Empty => None,
        }
    }

    pub fn push_event(&mut self, event: TaskEvent) {
        match self {
            BatchContent::DocumentAddtitionBatch(ts) => {
                ts.iter_mut().for_each(|t| t.events.push(event.clone()))
            }
            BatchContent::IndexUpdate(t) | BatchContent::Dump(t) => t.events.push(event),
            BatchContent::Snapshot(_) | BatchContent::Empty => (),
        }
    }
}

#[derive(Debug)]
pub struct Batch {
    // Only batches that contains a persistant tasks are given an id. Snapshot batches don't have
    // an id.
    pub id: Option<BatchId>,
    pub created_at: OffsetDateTime,
    pub content: BatchContent,
}

impl Batch {
    pub fn new(id: Option<BatchId>, content: BatchContent) -> Self {
        Self {
            id,
            created_at: OffsetDateTime::now_utc(),
            content,
        }
    }
    pub fn len(&self) -> usize {
        match self.content {
            BatchContent::DocumentAddtitionBatch(ref ts) => ts.len(),
            BatchContent::IndexUpdate(_) | BatchContent::Dump(_) | BatchContent::Snapshot(_) => 1,
            BatchContent::Empty => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn empty() -> Self {
        Self {
            id: None,
            created_at: OffsetDateTime::now_utc(),
            content: BatchContent::Empty,
        }
    }
}

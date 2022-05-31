use meilisearch_error::ResponseError;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::tasks::batch::BatchId;
use crate::tasks::task::{TaskContent, TaskEvent as NewTaskEvent, TaskId, TaskResult};
use crate::IndexUid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub index_uid: IndexUid,
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

impl From<Task> for crate::tasks::task::Task {
    fn from(other: Task) -> Self {
        Self {
            id: other.id,
            index_uid: Some(other.index_uid),
            content: other.content,
            events: other.events.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TaskEvent {
    Created(#[serde(with = "time::serde::rfc3339")] OffsetDateTime),
    Batched {
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
        batch_id: BatchId,
    },
    Processing(#[serde(with = "time::serde::rfc3339")] OffsetDateTime),
    Succeded {
        result: TaskResult,
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
    },
    Failed {
        error: ResponseError,
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
    },
}

impl From<TaskEvent> for NewTaskEvent {
    fn from(other: TaskEvent) -> Self {
        match other {
            TaskEvent::Created(x) => NewTaskEvent::Created(x),
            TaskEvent::Batched {
                timestamp,
                batch_id,
            } => NewTaskEvent::Batched {
                timestamp,
                batch_id,
            },
            TaskEvent::Processing(x) => NewTaskEvent::Processing(x),
            TaskEvent::Succeded { result, timestamp } => {
                NewTaskEvent::Succeeded { result, timestamp }
            }
            TaskEvent::Failed { error, timestamp } => NewTaskEvent::Failed { error, timestamp },
        }
    }
}

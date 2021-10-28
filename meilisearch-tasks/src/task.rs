use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use uuid::Uuid;

use crate::batch::BatchId;

pub type TaskId = u32;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskResult;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskError;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskEvent {
    Created(DateTime<Utc>),
    Batched {
        timestamp: DateTime<Utc>,
        batch_id: BatchId,
    },
    Processing(DateTime<Utc>),
    Succeded {
        result: TaskResult,
        timestamp: DateTime<Utc>,
    },
    Failed {
        error: TaskError,
        timestamp: DateTime<Utc>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: TaskId,
    pub index_uid: String,
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Copy, PartialEq)]
pub enum DocumentAdditionMergeStrategy {
    UpdateDocument,
    ReplaceDocument,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum DocumentDeletion {
    Clear,
    Ids(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TaskContent {
    DocumentAddition {
        content_uuid: Uuid,
        merge_strategy: DocumentAdditionMergeStrategy,
        primary_key: Option<String>,
        documents_count: usize,
    },
    DocumentDeletion(DocumentDeletion),
    IndexDeletion,
    SettingsUpdate,
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck::{Arbitrary, Gen};

    impl Arbitrary for Task {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                id: TaskId::arbitrary(g),
                index_uid: String::arbitrary(g),
                content: TaskContent::arbitrary(g),
                events: Vec::arbitrary(g),
            }
        }
    }

    impl Arbitrary for TaskContent {
        fn arbitrary(g: &mut Gen) -> Self {
           let rand = g.choose(&[1, 2, 3, 4]).unwrap();
           match rand {
               1 => Self::DocumentAddition {
                content_uuid: Uuid::new_v4(),
                merge_strategy: DocumentAdditionMergeStrategy::arbitrary(g),
                primary_key: Option::arbitrary(g),
                documents_count: usize::arbitrary(g),
            },
               2 => Self::DocumentDeletion(DocumentDeletion::arbitrary(g)),
               3 => Self::IndexDeletion,
               4 => Self::SettingsUpdate,
               _ => unreachable!()
           }
        }
    }

    impl Arbitrary for DocumentAdditionMergeStrategy {
        fn arbitrary(g: &mut Gen) -> Self {
            *g.choose(&[Self::ReplaceDocument, Self::UpdateDocument]).unwrap()
        }
    }

    impl Arbitrary for DocumentDeletion {
        fn arbitrary(g: &mut Gen) -> Self {
            let options = &[Self::Clear, Self::Ids(Vec::arbitrary(g))];
            g.choose(options).unwrap().clone()
        }
    }

    impl Arbitrary for TaskEvent {
        fn arbitrary(g: &mut Gen) -> Self {
            let options = &[
                Self::Created(Utc::now()),
                Self::Batched {
                    timestamp: Utc::now(),
                    batch_id: BatchId::arbitrary(g),
                },
                Self::Failed {
                    timestamp: Utc::now(),
                    error: TaskError::arbitrary(g),
                },
                Self::Succeded {
                    timestamp: Utc::now(),
                    result: TaskResult::arbitrary(g),
                },
            ];
            g.choose(options).unwrap().clone()
        }
    }

    impl Arbitrary for TaskError {
        fn arbitrary(_: &mut Gen) -> Self {
            Self
        }
    }

    impl Arbitrary for TaskResult {
        fn arbitrary(_: &mut Gen) -> Self {
            Self
        }
    }
}

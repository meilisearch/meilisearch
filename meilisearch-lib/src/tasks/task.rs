use meilisearch_error::ResponseError;
use milli::update::{DocumentAdditionResult, IndexDocumentsMethod};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use super::batch::BatchId;
use crate::{
    index::{Settings, Unchecked},
    index_resolver::IndexUid,
};

pub type TaskId = u64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum TaskResult {
    DocumentAddition { indexed_documents: u64 },
    DocumentDeletion { deleted_documents: u64 },
    ClearAll { deleted_documents: u64 },
    Other,
}

impl From<DocumentAdditionResult> for TaskResult {
    fn from(other: DocumentAdditionResult) -> Self {
        Self::DocumentAddition {
            indexed_documents: other.indexed_documents,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum TaskEvent {
    Created(
        #[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))]
        #[serde(with = "time::serde::rfc3339")]
        OffsetDateTime,
    ),
    Batched {
        #[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))]
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
        batch_id: BatchId,
    },
    Processing(
        #[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))]
        #[serde(with = "time::serde::rfc3339")]
        OffsetDateTime,
    ),
    Succeded {
        result: TaskResult,
        #[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))]
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
    },
    Failed {
        error: ResponseError,
        #[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))]
        #[serde(with = "time::serde::rfc3339")]
        timestamp: OffsetDateTime,
    },
}

impl TaskEvent {
    pub fn succeeded(result: TaskResult) -> Self {
        Self::Succeded {
            result,
            timestamp: OffsetDateTime::now_utc(),
        }
    }

    pub fn failed(error: ResponseError) -> Self {
        Self::Failed {
            error,
            timestamp: OffsetDateTime::now_utc(),
        }
    }
}

/// A task represents an operation that Meilisearch must do.
/// It's stored on disk and executed from the lowest to highest Task id.
/// Everytime a new task is created it has a higher Task id than the previous one.
/// See also `Job`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Task {
    pub id: TaskId,
    /// The name of the index the task is targeting. If it isn't targeting any idex (i.e Dump task)
    /// then this is None
    // TODO: when next forward breaking dumps, it would be a good idea to move this field inside of
    // the TaskContent.
    #[cfg_attr(
        test,
        proptest(
            strategy = "proptest::option::weighted(proptest::option::Probability::new(0.99), IndexUid::arbitrary())"
        )
    )]
    pub index_uid: Option<IndexUid>,
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

impl Task {
    /// Return true when a task is finished.
    /// A task is finished when its last state is either `Succeeded` or `Failed`.
    pub fn is_finished(&self) -> bool {
        self.events.last().map_or(false, |event| {
            matches!(event, TaskEvent::Succeded { .. } | TaskEvent::Failed { .. })
        })
    }

    /// Return the content_uuid of the `Task` if there is one.
    pub fn get_content_uuid(&self) -> Option<Uuid> {
        match self {
            Task {
                content: TaskContent::DocumentAddition { content_uuid, .. },
                ..
            } => Some(*content_uuid),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum DocumentDeletion {
    Clear,
    Ids(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[allow(clippy::large_enum_variant)]
pub enum TaskContent {
    DocumentAddition {
        #[cfg_attr(test, proptest(value = "Uuid::new_v4()"))]
        content_uuid: Uuid,
        #[cfg_attr(test, proptest(strategy = "test::index_document_method_strategy()"))]
        merge_strategy: IndexDocumentsMethod,
        primary_key: Option<String>,
        documents_count: usize,
        allow_index_creation: bool,
    },
    DocumentDeletion(DocumentDeletion),
    SettingsUpdate {
        settings: Settings<Unchecked>,
        /// Indicates whether the task was a deletion
        is_deletion: bool,
        allow_index_creation: bool,
    },
    IndexDeletion,
    IndexCreation {
        primary_key: Option<String>,
    },
    IndexUpdate {
        primary_key: Option<String>,
    },
    Dump {
        uid: String,
    },
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::*;

    pub(super) fn index_document_method_strategy() -> impl Strategy<Value = IndexDocumentsMethod> {
        prop_oneof![
            Just(IndexDocumentsMethod::ReplaceDocuments),
            Just(IndexDocumentsMethod::UpdateDocuments),
        ]
    }

    pub(super) fn datetime_strategy() -> impl Strategy<Value = OffsetDateTime> {
        Just(OffsetDateTime::now_utc())
    }
}

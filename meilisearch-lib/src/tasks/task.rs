use chrono::{DateTime, Utc};
use meilisearch_error::ResponseError;
use milli::update::{DocumentAdditionResult, IndexDocumentsMethod};
use serde::{Deserialize, Serialize};
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
    DocumentAddition { number_of_documents: usize },
    DocumentDeletion { number_of_documents: u64 },
    Other,
}

impl From<DocumentAdditionResult> for TaskResult {
    fn from(other: DocumentAdditionResult) -> Self {
        Self::DocumentAddition {
            number_of_documents: other.nb_documents,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum TaskEvent {
    Created(#[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))] DateTime<Utc>),
    Batched {
        #[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))]
        timestamp: DateTime<Utc>,
        batch_id: BatchId,
    },
    Processing(#[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))] DateTime<Utc>),
    Succeded {
        result: TaskResult,
        #[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))]
        timestamp: DateTime<Utc>,
    },
    Failed {
        error: ResponseError,
        #[cfg_attr(test, proptest(strategy = "test::datetime_strategy()"))]
        timestamp: DateTime<Utc>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Task {
    pub id: TaskId,
    pub index_uid: IndexUid,
    pub content: TaskContent,
    pub events: Vec<TaskEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum DocumentDeletion {
    Clear,
    Ids(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum TaskContent {
    DocumentAddition {
        #[cfg_attr(test, proptest(value = "Uuid::new_v4()"))]
        content_uuid: Uuid,
        #[cfg_attr(test, proptest(strategy = "test::index_document_method_strategy()"))]
        merge_strategy: IndexDocumentsMethod,
        primary_key: Option<String>,
        documents_count: usize,
    },
    DocumentDeletion(DocumentDeletion),
    SettingsUpdate {
        settings: Settings<Unchecked>,
        /// Indicates whether the task was a deletion
        is_deletion: bool,
    },
    IndexDeletion,
    CreateIndex {
        primary_key: Option<String>,
    },
    UpdateIndex {
        primary_key: Option<String>,
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

    pub(super) fn datetime_strategy() -> impl Strategy<Value = DateTime<Utc>> {
        Just(Utc::now())
    }
}

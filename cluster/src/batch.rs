use meilisearch_types::milli::update::IndexDocumentsMethod;
use meilisearch_types::settings::{Settings, Unchecked};
use meilisearch_types::tasks::TaskId;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// Represents a combination of tasks that can all be processed at the same time.
///
/// A batch contains the set of tasks that it represents (accessible through
/// [`self.ids()`](Batch::ids)), as well as additional information on how to
/// be processed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Batch {
    TaskCancelation {
        /// The task cancelation itself.
        task: TaskId,
        /// The date and time at which the previously processing tasks started.
        previous_started_at: OffsetDateTime,
        /// The list of tasks that were processing when this task cancelation appeared.
        previous_processing_tasks: RoaringBitmap,
    },
    TaskDeletion(TaskId),
    SnapshotCreation(Vec<TaskId>),
    Dump(TaskId),
    IndexOperation {
        op: IndexOperation,
        must_create_index: bool,
    },
    IndexCreation {
        index_uid: String,
        primary_key: Option<String>,
        task: TaskId,
    },
    IndexUpdate {
        index_uid: String,
        primary_key: Option<String>,
        task: TaskId,
    },
    IndexDeletion {
        index_uid: String,
        tasks: Vec<TaskId>,
        index_has_been_created: bool,
    },
    IndexSwap {
        task: TaskId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DocumentOperation {
    Add(Uuid),
    Delete(Vec<String>),
}

/// A [batch](Batch) that combines multiple tasks operating on an index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexOperation {
    DocumentOperation {
        index_uid: String,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        documents_counts: Vec<u64>,
        operations: Vec<DocumentOperation>,
        tasks: Vec<TaskId>,
    },
    DocumentDeletion {
        index_uid: String,
        // The vec associated with each document deletion tasks.
        documents: Vec<Vec<String>>,
        tasks: Vec<TaskId>,
    },
    DocumentClear {
        index_uid: String,
        tasks: Vec<TaskId>,
    },
    Settings {
        index_uid: String,
        // The boolean indicates if it's a settings deletion or creation.
        settings: Vec<(bool, Settings<Unchecked>)>,
        tasks: Vec<TaskId>,
    },
    DocumentClearAndSetting {
        index_uid: String,
        cleared_tasks: Vec<TaskId>,

        // The boolean indicates if it's a settings deletion or creation.
        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<TaskId>,
    },
    SettingsAndDocumentOperation {
        index_uid: String,

        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        documents_counts: Vec<u64>,
        operations: Vec<DocumentOperation>,
        document_import_tasks: Vec<TaskId>,

        // The boolean indicates if it's a settings deletion or creation.
        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<TaskId>,
    },
}

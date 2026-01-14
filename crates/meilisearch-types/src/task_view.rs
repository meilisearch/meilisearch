use std::collections::BTreeMap;

use byte_unit::UnitType;
use milli::Object;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use utoipa::ToSchema;

use crate::batches::BatchId;
use crate::error::ResponseError;
use crate::settings::{Settings, Unchecked};
use crate::tasks::network::DbTaskNetwork;
use crate::tasks::{
    serialize_duration, Details, DetailsExportIndexSettings, IndexSwap, Kind, Status, Task, TaskId,
};

/// Represents the current state and details of an asynchronous task.
///
/// Tasks are created when you perform operations like adding documents,
/// updating settings, or creating indexes. Use this view to monitor task
/// progress and check for errors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct TaskView {
    /// The unique sequential identifier assigned to this task. Task UIDs are
    /// assigned in order of creation and can be used to retrieve specific
    /// task information or track task dependencies.
    #[schema(value_type = u32, example = 4312)]
    pub uid: TaskId,
    /// The unique identifier of the batch that processed this task. Multiple
    /// tasks may share the same batch UID if they were processed together
    /// for efficiency. This is `null` for tasks that haven't been processed.
    #[schema(value_type = Option<u32>, example = json!("movies"))]
    pub batch_uid: Option<BatchId>,
    /// The unique identifier of the index this task operates on. This is
    /// `null` for global tasks like `dumpCreation` or `taskDeletion` that
    /// don't target a specific index.
    #[serde(default)]
    pub index_uid: Option<String>,
    /// The current processing status of the task. Possible values are:
    /// `enqueued` (waiting), `processing` (executing), `succeeded`,
    /// `failed`, or `canceled`.
    pub status: Status,
    /// The type of operation this task performs. Examples include
    /// `documentAdditionOrUpdate`, `documentDeletion`, `settingsUpdate`,
    /// `indexCreation`, `indexDeletion`, `dumpCreation`, etc.
    #[serde(rename = "type")]
    pub kind: Kind,
    /// If this task was canceled, this field contains the UID of the
    /// `taskCancelation` task that canceled it. This is `null` for tasks
    /// that were not canceled.
    #[schema(value_type = Option<u32>, example = json!(4326))]
    pub canceled_by: Option<TaskId>,
    /// Contains type-specific information about the task, such as the number
    /// of documents processed, settings that were applied, or filters that
    /// were used. The structure varies depending on the task type.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<DetailsView>)]
    pub details: Option<DetailsView>,
    /// If the task failed, this field contains detailed error information
    /// including an error message, error code, error type, and a link to
    /// documentation. This is `null` for tasks that succeeded or are still
    /// processing.
    #[schema(value_type = Option<ResponseError>)]
    pub error: Option<ResponseError>,
    /// The total time spent processing this task, formatted as an ISO-8601
    /// duration (e.g., `PT0.5S` for 0.5 seconds). This is `null` for tasks
    /// that haven't finished processing yet.
    #[schema(value_type = Option<String>, example = json!(null))]
    #[serde(serialize_with = "serialize_duration", default)]
    pub duration: Option<Duration>,
    /// The timestamp when this task was added to the queue, formatted as an
    /// RFC 3339 date-time string. All tasks have an enqueued timestamp as
    /// it's set when the task is created.
    #[schema(value_type = String, example = json!("2024-08-08_14:12:09.393Z"))]
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    /// The timestamp when Meilisearch began processing this task, formatted
    /// as an RFC 3339 date-time string. This is `null` for tasks that are
    /// still in the queue waiting to be processed.
    #[schema(value_type = String, example = json!("2024-08-08_14:12:09.393Z"))]
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub started_at: Option<OffsetDateTime>,
    /// The timestamp when this task finished processing (whether successfully
    /// or with an error), formatted as an RFC 3339 date-time string. This is
    /// `null` for tasks that haven't finished yet.
    #[schema(value_type = String, example = json!("2024-08-08_14:12:09.393Z"))]
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub finished_at: Option<OffsetDateTime>,
    /// Network topology information for distributed deployments. Contains
    /// details about which nodes are involved in processing this task. This
    /// is only present when running Meilisearch in a distributed config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<DbTaskNetwork>)]
    pub network: Option<DbTaskNetwork>,
    /// Custom metadata string that was attached to this task when it was
    /// created. This can be used to associate tasks with external systems,
    /// track task origins, or add any application-specific information.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_metadata: Option<String>,
}

impl TaskView {
    pub fn from_task(task: &Task) -> TaskView {
        TaskView {
            uid: task.uid,
            batch_uid: task.batch_uid,
            index_uid: task.index_uid().map(ToOwned::to_owned),
            status: task.status,
            kind: task.kind.as_kind(),
            canceled_by: task.canceled_by,
            details: task.details.clone().map(DetailsView::from),
            error: task.error.clone(),
            duration: task.started_at.zip(task.finished_at).map(|(start, end)| end - start),
            enqueued_at: task.enqueued_at,
            started_at: task.started_at,
            finished_at: task.finished_at,
            network: task.network.clone(),
            custom_metadata: task.custom_metadata.clone(),
        }
    }
}

/// Contains type-specific details about a task's execution.
///
/// The fields present depend on the task type. For example, document addition
/// tasks will have `receivedDocuments` and `indexedDocuments`, while settings
/// update tasks will have the applied settings.
#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct DetailsView {
    /// The number of documents that were sent in the request payload for a
    /// `documentAdditionOrUpdate` task. This count is determined before any
    /// processing occurs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_documents: Option<u64>,
    /// The number of documents that were successfully indexed after
    /// processing a `documentAdditionOrUpdate` task. This may differ from
    /// `receivedDocuments` if some documents were invalid or duplicates.
    /// The inner `null` indicates the task is still processing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_documents: Option<Option<u64>>,
    /// The number of documents that were modified by an `documentEdition`
    /// task using a RHAI function. The inner `null` indicates the task is
    /// still processing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited_documents: Option<Option<u64>>,
    /// The primary key attribute set for the index. For `indexCreation`
    /// tasks, this is the primary key that was specified. For `indexUpdate`
    /// tasks, this shows the new primary key if it was changed. The inner
    /// `null` means no primary key was specified and Meilisearch will infer
    /// it from documents.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Option<String>>,
    /// The number of document IDs that were provided in a `documentDeletion`
    /// request. This is the count before processing - the actual number
    /// deleted may be lower if some IDs didn't exist.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provided_ids: Option<usize>,
    /// The number of documents that were actually removed from the index for
    /// `documentDeletion`, `documentDeletionByFilter`, or `indexDeletion`
    /// tasks. The inner `null` indicates the task is still processing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_documents: Option<Option<u64>>,
    /// The number of tasks that matched the filter criteria for a
    /// `taskCancelation` or `taskDeletion` request. This is determined when
    /// the request is received, before any cancellation or deletion occurs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_tasks: Option<u64>,
    /// The number of tasks that were successfully canceled by a
    /// `taskCancelation` task. This may be less than `matchedTasks` if some
    /// tasks completed before they could be canceled. The inner `null`
    /// indicates the task is still processing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canceled_tasks: Option<Option<u64>>,
    /// The number of tasks that were successfully deleted by a `taskDeletion`
    /// task. The inner `null` indicates the task is still processing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_tasks: Option<Option<u64>>,
    /// The original filter query string that was used to select tasks for a
    /// `taskCancelation` or `taskDeletion` operation. Useful for
    /// understanding which tasks were targeted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_filter: Option<Option<String>>,
    /// The unique identifier assigned to the dump file created by a
    /// `dumpCreation` task. Use this UID to locate the dump file in the
    /// dumps directory. The inner `null` indicates the task is still
    /// processing or failed before generating a UID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dump_uid: Option<Option<String>>,
    /// The context object that was provided to the RHAI function for a
    /// `documentEdition` task. This object contains data that the function
    /// can access during document processing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Option<Object>>,
    /// The RHAI function code that was executed for a `documentEdition`
    /// task. This function is applied to each document matching the filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    /// The complete settings object that was applied by a `settingsUpdate`
    /// task. Only the settings that were modified are included in this
    /// object.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub settings: Option<Box<Settings<Unchecked>>>,
    /// The list of index swap operations that were performed by an
    /// `indexSwap` task. Each swap specifies two indexes that exchanged
    /// their contents.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swaps: Option<Vec<IndexSwap>>,
    /// The Meilisearch version before a database upgrade was performed.
    /// Formatted as `vX.Y.Z`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upgrade_from: Option<String>,
    /// The Meilisearch version after a database upgrade was completed.
    /// Formatted as `vX.Y.Z`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upgrade_to: Option<String>,
    /// The destination URL where data is being exported for an `export`
    /// task. This is the endpoint that receives the exported index data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The API key used for authentication when exporting data to a remote
    /// Meilisearch instance. This value is partially masked for security.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// The maximum payload size configured for an `export` task, formatted
    /// as a human-readable string (e.g., `100 MB`). This limits the size of
    /// each batch of documents sent during export.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_size: Option<String>,
    /// A map of index patterns to their export settings for an `export`
    /// task. The keys are index patterns (which may include wildcards) and
    /// the values contain the specific export configuration for matching
    /// indexes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexes: Option<BTreeMap<String, DetailsExportIndexSettings>>,
    /// The original unique identifier of the index before an `indexRename`
    /// operation. This is the name the index had before it was renamed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_index_uid: Option<String>,
    /// The new unique identifier assigned to the index after an `indexRename`
    /// operation. This is the name the index has after being renamed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_index_uid: Option<String>,
    /// The size of the index before an `indexCompaction` task was performed,
    /// formatted as a human-readable string (e.g., `1.5 GB`). Compare with
    /// `postCompactionSize` to see how much space was reclaimed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_compaction_size: Option<String>,
    /// The size of the index after an `indexCompaction` task completed,
    /// formatted as a human-readable string (e.g., `1.2 GB`). This should
    /// be smaller than or equal to `preCompactionSize`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_compaction_size: Option<String>,
    /// The number of documents that were redistributed during a
    /// `networkTopologyChange` task in a distributed deployment. This
    /// occurs when the cluster configuration changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moved_documents: Option<u64>,
    /// A human-readable message providing additional information about the
    /// task, such as status updates or explanatory text about what occurred
    /// during processing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl DetailsView {
    pub fn accumulate(&mut self, other: &Self) {
        *self = Self {
            received_documents: match (self.received_documents, other.received_documents) {
                (None, None) => None,
                (None, Some(doc)) | (Some(doc), None) => Some(doc),
                (Some(left), Some(right)) => Some(left + right),
            },
            moved_documents: match (self.moved_documents, other.moved_documents) {
                (None, None) => None,
                (None, Some(doc)) | (Some(doc), None) => Some(doc),
                (Some(left), Some(right)) => Some(left + right),
            },
            message: match (&mut self.message, &other.message) {
                (None, None) => None,
                (None, Some(message)) => Some(message.clone()),
                (Some(message), None) => Some(std::mem::take(message)),
                (Some(message), Some(_)) => Some(std::mem::take(message)),
            },
            indexed_documents: match (self.indexed_documents, other.indexed_documents) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(doc))) | (Some(Some(doc)), None | Some(None)) => {
                    Some(Some(doc))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            edited_documents: match (self.edited_documents, other.edited_documents) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(doc))) | (Some(Some(doc)), None | Some(None)) => {
                    Some(Some(doc))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            primary_key: match (&self.primary_key, &other.primary_key) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(doc))) | (Some(Some(doc)), None | Some(None)) => {
                    Some(Some(doc.to_string()))
                }
                // In the case we receive multiple primary keys (which shouldn't happens)
                // we only return the first one encountered.
                (Some(Some(left)), Some(Some(_right))) => Some(Some(left.to_string())),
            },
            provided_ids: match (self.provided_ids, other.provided_ids) {
                (None, None) => None,
                (None, Some(ids)) | (Some(ids), None) => Some(ids),
                (Some(left), Some(right)) => Some(left + right),
            },
            deleted_documents: match (self.deleted_documents, other.deleted_documents) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(doc))) | (Some(Some(doc)), None | Some(None)) => {
                    Some(Some(doc))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            matched_tasks: match (self.matched_tasks, other.matched_tasks) {
                (None, None) => None,
                (None, Some(task)) | (Some(task), None) => Some(task),
                (Some(left), Some(right)) => Some(left + right),
            },
            canceled_tasks: match (self.canceled_tasks, other.canceled_tasks) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(task))) | (Some(Some(task)), None | Some(None)) => {
                    Some(Some(task))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            deleted_tasks: match (self.deleted_tasks, other.deleted_tasks) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(task))) | (Some(Some(task)), None | Some(None)) => {
                    Some(Some(task))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            original_filter: match (&self.original_filter, &other.original_filter) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(filter)))
                | (Some(Some(filter)), None | Some(None)) => Some(Some(filter.to_string())),
                // In this case, we cannot really merge both filters or return an array
                // so we're going to return all the conditions one after the other.
                (Some(Some(left)), Some(Some(right))) => Some(Some(format!("{left}&{right}"))),
            },
            dump_uid: match (&self.dump_uid, &other.dump_uid) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(dump_uid)))
                | (Some(Some(dump_uid)), None | Some(None)) => Some(Some(dump_uid.to_string())),
                // We should never be able to batch multiple dumps at the same time.
                // So we return the first one we encounter but that shouldn't be an
                // issue anyway.
                (Some(Some(left)), Some(Some(_right))) => Some(Some(left.to_string())),
            },
            context: match (&self.context, &other.context) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(ctx))) | (Some(Some(ctx)), None | Some(None)) => {
                    Some(Some(ctx.clone()))
                }
                // We should never be able to batch multiple documents edited at the
                // same time. So we return the first one we encounter but that
                // shouldn't be an issue anyway.
                (Some(Some(left)), Some(Some(_right))) => Some(Some(left.clone())),
            },
            function: match (&self.function, &other.function) {
                (None, None) => None,
                (None, Some(fun)) | (Some(fun), None) => Some(fun.to_string()),
                // We should never be able to batch multiple documents edited at the
                // same time. So we return the first one we encounter but that
                // shouldn't be an issue anyway.
                (Some(left), Some(_right)) => Some(left.to_string()),
            },
            settings: match (self.settings.clone(), other.settings.clone()) {
                (None, None) => None,
                (None, Some(settings)) | (Some(settings), None) => Some(settings),
                (Some(mut left), Some(right)) => {
                    left.merge(&right);
                    Some(left)
                }
            },
            swaps: match (self.swaps.clone(), other.swaps.clone()) {
                (None, None) => None,
                (None, Some(swaps)) | (Some(swaps), None) => Some(swaps),
                (Some(mut left), Some(mut right)) => {
                    left.append(&mut right);
                    Some(left)
                }
            },
            url: match (self.url.clone(), other.url.clone()) {
                (None, None) => None,
                (None, Some(url)) | (Some(url), None) => Some(url),
                // We should never be able to batch multiple exports at the same time.
                // So we return the first one we encounter but that shouldn't be an
                // issue anyway.
                (Some(left), Some(_right)) => Some(left),
            },
            api_key: match (self.api_key.clone(), other.api_key.clone()) {
                (None, None) => None,
                (None, Some(key)) | (Some(key), None) => Some(key),
                // We should never be able to batch multiple exports at the same time.
                // So we return the first one we encounter but that shouldn't be an
                // issue anyway.
                (Some(left), Some(_right)) => Some(left),
            },
            payload_size: match (self.payload_size.clone(), other.payload_size.clone()) {
                (None, None) => None,
                (None, Some(size)) | (Some(size), None) => Some(size),
                // We should never be able to batch multiple exports at the same time.
                // So we return the first one we encounter but that shouldn't be an
                // issue anyway.
                (Some(left), Some(_right)) => Some(left),
            },
            indexes: match (self.indexes.clone(), other.indexes.clone()) {
                (None, None) => None,
                (None, Some(indexes)) | (Some(indexes), None) => Some(indexes),
                // We should never be able to batch multiple exports at the same time.
                // So we return the first one we encounter but that shouldn't be an
                // issue anyway.
                (Some(left), Some(_right)) => Some(left),
            },
            // We want the earliest version
            upgrade_from: match (self.upgrade_from.clone(), other.upgrade_from.clone()) {
                (None, None) => None,
                (None, Some(from)) | (Some(from), None) => Some(from),
                (Some(from), Some(_)) => Some(from),
            },
            // And the latest
            upgrade_to: match (self.upgrade_to.clone(), other.upgrade_to.clone()) {
                (None, None) => None,
                (None, Some(to)) | (Some(to), None) => Some(to),
                (Some(_), Some(to)) => Some(to),
            },
            old_index_uid: match (self.old_index_uid.clone(), other.old_index_uid.clone()) {
                (None, None) => None,
                (None, Some(uid)) | (Some(uid), None) => Some(uid),
                // We should never be able to batch multiple renames at the same time.
                (Some(left), Some(_right)) => Some(left),
            },
            new_index_uid: match (self.new_index_uid.clone(), other.new_index_uid.clone()) {
                (None, None) => None,
                (None, Some(uid)) | (Some(uid), None) => Some(uid),
                // We should never be able to batch multiple renames at the same time.
                (Some(left), Some(_right)) => Some(left),
            },
            pre_compaction_size: match (
                self.pre_compaction_size.clone(),
                other.pre_compaction_size.clone(),
            ) {
                (None, None) => None,
                (None, Some(size)) | (Some(size), None) => Some(size),
                // We should never be able to batch multiple compactions at the
                // same time.
                (Some(left), Some(_right)) => Some(left),
            },
            post_compaction_size: match (
                self.post_compaction_size.clone(),
                other.post_compaction_size.clone(),
            ) {
                (None, None) => None,
                (None, Some(size)) | (Some(size), None) => Some(size),
                // We should never be able to batch multiple compactions at the
                // same time.
                (Some(left), Some(_right)) => Some(left),
            },
        }
    }
}

impl From<Details> for DetailsView {
    fn from(details: Details) -> Self {
        match details {
            Details::DocumentAdditionOrUpdate { received_documents, indexed_documents } => {
                DetailsView {
                    received_documents: Some(received_documents),
                    indexed_documents: Some(indexed_documents),
                    ..DetailsView::default()
                }
            }
            Details::DocumentEdition {
                deleted_documents,
                edited_documents,
                original_filter,
                context,
                function,
            } => DetailsView {
                deleted_documents: Some(deleted_documents),
                edited_documents: Some(edited_documents),
                original_filter: Some(original_filter),
                context: Some(context),
                function: Some(function),
                ..DetailsView::default()
            },
            Details::SettingsUpdate { mut settings } => {
                settings.hide_secrets();
                DetailsView { settings: Some(settings), ..DetailsView::default() }
            }
            Details::IndexInfo { primary_key, new_index_uid, old_index_uid } => DetailsView {
                primary_key: Some(primary_key),
                new_index_uid: new_index_uid.clone(),
                old_index_uid: old_index_uid.clone(),
                ..DetailsView::default()
            },
            Details::DocumentDeletion {
                provided_ids: received_document_ids,
                deleted_documents,
            } => DetailsView {
                provided_ids: Some(received_document_ids),
                deleted_documents: Some(deleted_documents),
                original_filter: Some(None),
                ..DetailsView::default()
            },
            Details::DocumentDeletionByFilter { original_filter, deleted_documents } => {
                DetailsView {
                    provided_ids: Some(0),
                    original_filter: Some(Some(original_filter)),
                    deleted_documents: Some(deleted_documents),
                    ..DetailsView::default()
                }
            }
            Details::ClearAll { deleted_documents } => {
                DetailsView { deleted_documents: Some(deleted_documents), ..DetailsView::default() }
            }
            Details::TaskCancelation { matched_tasks, canceled_tasks, original_filter } => {
                DetailsView {
                    matched_tasks: Some(matched_tasks),
                    canceled_tasks: Some(canceled_tasks),
                    original_filter: Some(Some(original_filter)),
                    ..DetailsView::default()
                }
            }
            Details::TaskDeletion { matched_tasks, deleted_tasks, original_filter } => {
                DetailsView {
                    matched_tasks: Some(matched_tasks),
                    deleted_tasks: Some(deleted_tasks),
                    original_filter: Some(Some(original_filter)),
                    ..DetailsView::default()
                }
            }
            Details::Dump { dump_uid } => {
                DetailsView { dump_uid: Some(dump_uid), ..DetailsView::default() }
            }
            Details::IndexSwap { swaps } => {
                DetailsView { swaps: Some(swaps), ..Default::default() }
            }
            Details::Export { url, api_key, payload_size, indexes } => DetailsView {
                url: Some(url),
                api_key: api_key.map(|mut api_key| {
                    hide_secret(&mut api_key);
                    api_key
                }),
                payload_size: payload_size
                    .map(|ps| ps.get_appropriate_unit(UnitType::Both).to_string()),
                indexes: Some(
                    indexes
                        .into_iter()
                        .map(|(pattern, settings)| (pattern.to_string(), settings))
                        .collect(),
                ),
                ..Default::default()
            },
            Details::UpgradeDatabase { from, to } => DetailsView {
                upgrade_from: Some(format!("v{}.{}.{}", from.0, from.1, from.2)),
                upgrade_to: Some(format!("v{}.{}.{}", to.0, to.1, to.2)),
                ..Default::default()
            },
            Details::IndexCompaction { pre_compaction_size, post_compaction_size, .. } => {
                DetailsView {
                    pre_compaction_size: pre_compaction_size
                        .map(|size| size.get_appropriate_unit(UnitType::Both).to_string()),
                    post_compaction_size: post_compaction_size
                        .map(|size| size.get_appropriate_unit(UnitType::Both).to_string()),
                    ..Default::default()
                }
            }
            Details::NetworkTopologyChange { moved_documents, message } => DetailsView {
                moved_documents: Some(moved_documents),
                message: Some(message),
                ..Default::default()
            },
        }
    }
}

// We definitely need to factorize the code to hide the secret key
fn hide_secret(secret: &mut String) {
    match secret.len() {
        x if x < 10 => {
            secret.replace_range(.., "XXX...");
        }
        x if x < 20 => {
            secret.replace_range(2.., "XXXX...");
        }
        x if x < 30 => {
            secret.replace_range(3.., "XXXXX...");
        }
        _x => {
            secret.replace_range(5.., "XXXXXX...");
        }
    }
}

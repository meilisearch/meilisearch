use std::fmt::Display;

use meilisearch_types::batches::BatchId;
use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::tasks::{Kind, Status};
use meilisearch_types::{heed, milli};
use thiserror::Error;

use crate::TaskId;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DateField {
    BeforeEnqueuedAt,
    AfterEnqueuedAt,
    BeforeStartedAt,
    AfterStartedAt,
    BeforeFinishedAt,
    AfterFinishedAt,
}

impl Display for DateField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BeforeEnqueuedAt => write!(f, "beforeEnqueuedAt"),
            Self::AfterEnqueuedAt => write!(f, "afterEnqueuedAt"),
            Self::BeforeStartedAt => write!(f, "beforeStartedAt"),
            Self::AfterStartedAt => write!(f, "afterStartedAt"),
            Self::BeforeFinishedAt => write!(f, "beforeFinishedAt"),
            Self::AfterFinishedAt => write!(f, "afterFinishedAt"),
        }
    }
}

impl From<DateField> for Code {
    fn from(date: DateField) -> Self {
        match date {
            DateField::BeforeEnqueuedAt => Self::InvalidTaskBeforeEnqueuedAt,
            DateField::AfterEnqueuedAt => Self::InvalidTaskAfterEnqueuedAt,
            DateField::BeforeStartedAt => Self::InvalidTaskBeforeStartedAt,
            DateField::AfterStartedAt => Self::InvalidTaskAfterStartedAt,
            DateField::BeforeFinishedAt => Self::InvalidTaskBeforeFinishedAt,
            DateField::AfterFinishedAt => Self::InvalidTaskAfterFinishedAt,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
pub enum Error {
    #[error("{1}")]
    WithCustomErrorCode(Code, Box<Self>),
    #[error("Received bad task id: {received} should be >= to {expected}.")]
    BadTaskId { received: TaskId, expected: TaskId },
    #[error("Index `{0}` not found.")]
    IndexNotFound(String),
    #[error("Index `{0}` already exists.")]
    IndexAlreadyExists(String),
    #[error(
        "Indexes must be declared only once during a swap. `{0}` was specified several times."
    )]
    SwapDuplicateIndexFound(String),
    #[error(
        "Indexes must be declared only once during a swap. {} were specified several times.",
        .0.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", ")
    )]
    SwapDuplicateIndexesFound(Vec<String>),
    #[error("Index `{0}` not found.")]
    SwapIndexNotFound(String),
    #[error("Meilisearch cannot receive write operations because the limit of the task database has been reached. Please delete tasks to continue performing write operations.")]
    NoSpaceLeftInTaskQueue,
    #[error(
        "Indexes {} not found.",
        .0.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", ")
    )]
    SwapIndexesNotFound(Vec<String>),
    #[error("Corrupted dump.")]
    CorruptedDump,
    #[error(
        "Task `{field}` `{date}` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format."
    )]
    InvalidTaskDate { field: DateField, date: String },
    #[error("Task uid `{task_uid}` is invalid. It should only contain numeric characters.")]
    InvalidTaskUid { task_uid: String },
    #[error("Batch uid `{batch_uid}` is invalid. It should only contain numeric characters.")]
    InvalidBatchUid { batch_uid: String },
    #[error(
        "Task status `{status}` is invalid. Available task statuses are {}.",
            enum_iterator::all::<Status>()
                .map(|s| format!("`{s}`"))
                .collect::<Vec<String>>()
                .join(", ")
    )]
    InvalidTaskStatuses { status: String },
    #[error(
        "Task type `{type_}` is invalid. Available task types are {}",
            enum_iterator::all::<Kind>()
                .map(|s| format!("`{s}`"))
                .collect::<Vec<String>>()
                .join(", ")
    )]
    InvalidTaskTypes { type_: String },
    #[error(
        "Task canceledBy `{canceled_by}` is invalid. It should only contains numeric characters separated by `,` character."
    )]
    InvalidTaskCanceledBy { canceled_by: String },
    #[error(
        "{index_uid} is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 400 bytes."
    )]
    InvalidIndexUid { index_uid: String },
    #[error("Task `{0}` not found.")]
    TaskNotFound(TaskId),
    #[error("Task `{0}` does not contain any documents. Only `documentAdditionOrUpdate` tasks with the statuses `enqueued` or `processing` contain documents")]
    TaskFileNotFound(TaskId),
    #[error("Batch `{0}` not found.")]
    BatchNotFound(BatchId),
    #[error("Query parameters to filter the tasks to delete are missing. Available query parameters are: `uids`, `indexUids`, `statuses`, `types`, `canceledBy`, `beforeEnqueuedAt`, `afterEnqueuedAt`, `beforeStartedAt`, `afterStartedAt`, `beforeFinishedAt`, `afterFinishedAt`.")]
    TaskDeletionWithEmptyQuery,
    #[error("Query parameters to filter the tasks to cancel are missing. Available query parameters are: `uids`, `indexUids`, `statuses`, `types`, `canceledBy`, `beforeEnqueuedAt`, `afterEnqueuedAt`, `beforeStartedAt`, `afterStartedAt`, `beforeFinishedAt`, `afterFinishedAt`.")]
    TaskCancelationWithEmptyQuery,
    #[error("Aborted task")]
    AbortedTask,

    #[error(transparent)]
    Dump(#[from] dump::Error),
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error("{}", match .index_uid {
        Some(uid) if !uid.is_empty() => format!("Index `{}`: {error}", uid),
        _ => format!("{error}")
    })]
    Milli { error: milli::Error, index_uid: Option<String> },
    #[error("An unexpected crash occurred when processing the task: {0}")]
    ProcessBatchPanicked(String),
    #[error(transparent)]
    FileStore(#[from] file_store::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    Persist(#[from] tempfile::PersistError),
    #[error(transparent)]
    FeatureNotEnabled(#[from] FeatureNotEnabledError),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    // Irrecoverable errors:
    #[error(transparent)]
    CreateBatch(Box<Self>),
    #[error("Corrupted task queue.")]
    CorruptedTaskQueue,
    #[error(transparent)]
    DatabaseUpgrade(Box<Self>),
    #[error(transparent)]
    UnrecoverableError(Box<Self>),
    #[error(transparent)]
    HeedTransaction(heed::Error),

    #[cfg(test)]
    #[error("Planned failure for tests.")]
    PlannedFailure,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "{disabled_action} requires enabling the `{feature}` experimental feature. See {issue_link}"
)]
pub struct FeatureNotEnabledError {
    pub disabled_action: &'static str,
    pub feature: &'static str,
    pub issue_link: &'static str,
}

impl Error {
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::IndexNotFound(_)
            | Self::WithCustomErrorCode(_, _)
            | Self::BadTaskId { .. }
            | Self::IndexAlreadyExists(_)
            | Self::SwapDuplicateIndexFound(_)
            | Self::SwapDuplicateIndexesFound(_)
            | Self::SwapIndexNotFound(_)
            | Self::NoSpaceLeftInTaskQueue
            | Self::SwapIndexesNotFound(_)
            | Self::CorruptedDump
            | Self::InvalidTaskDate { .. }
            | Self::InvalidTaskUid { .. }
            | Self::InvalidBatchUid { .. }
            | Self::InvalidTaskStatuses { .. }
            | Self::InvalidTaskTypes { .. }
            | Self::InvalidTaskCanceledBy { .. }
            | Self::InvalidIndexUid { .. }
            | Self::TaskNotFound(_)
            | Self::TaskFileNotFound(_)
            | Self::BatchNotFound(_)
            | Self::TaskDeletionWithEmptyQuery
            | Self::TaskCancelationWithEmptyQuery
            | Self::AbortedTask
            | Self::Dump(_)
            | Self::Heed(_)
            | Self::Milli { .. }
            | Self::ProcessBatchPanicked(_)
            | Self::FileStore(_)
            | Self::IoError(_)
            | Self::Persist(_)
            | Self::FeatureNotEnabled(_)
            | Self::Anyhow(_) => true,
            Self::CreateBatch(_)
            | Self::CorruptedTaskQueue
            | Self::DatabaseUpgrade(_)
            | Self::UnrecoverableError(_)
            | Self::HeedTransaction(_) => false,
            #[cfg(test)]
            Self::PlannedFailure => false,
        }
    }

    pub fn with_custom_error_code(self, code: Code) -> Self {
        Self::WithCustomErrorCode(code, Box::new(self))
    }

    pub fn from_milli(err: milli::Error, index_uid: Option<String>) -> Self {
        match err {
            milli::Error::UserError(milli::UserError::InvalidFilter(_)) => {
                Self::Milli { error: err, index_uid }
                    .with_custom_error_code(Code::InvalidDocumentFilter)
            }
            milli::Error::UserError(milli::UserError::InvalidFilterExpression { .. }) => {
                Self::Milli { error: err, index_uid }
                    .with_custom_error_code(Code::InvalidDocumentFilter)
            }
            _ => Self::Milli { error: err, index_uid },
        }
    }
}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        match self {
            Self::WithCustomErrorCode(code, _) => *code,
            Self::BadTaskId { .. } => Code::BadRequest,
            Self::IndexNotFound(_) => Code::IndexNotFound,
            Self::IndexAlreadyExists(_) => Code::IndexAlreadyExists,
            Self::SwapDuplicateIndexesFound(_) => Code::InvalidSwapDuplicateIndexFound,
            Self::SwapDuplicateIndexFound(_) => Code::InvalidSwapDuplicateIndexFound,
            Self::SwapIndexNotFound(_) => Code::IndexNotFound,
            Self::SwapIndexesNotFound(_) => Code::IndexNotFound,
            Self::InvalidTaskDate { field, .. } => (*field).into(),
            Self::InvalidTaskUid { .. } => Code::InvalidTaskUids,
            Self::InvalidBatchUid { .. } => Code::InvalidBatchUids,
            Self::InvalidTaskStatuses { .. } => Code::InvalidTaskStatuses,
            Self::InvalidTaskTypes { .. } => Code::InvalidTaskTypes,
            Self::InvalidTaskCanceledBy { .. } => Code::InvalidTaskCanceledBy,
            Self::InvalidIndexUid { .. } => Code::InvalidIndexUid,
            Self::TaskNotFound(_) => Code::TaskNotFound,
            Self::TaskFileNotFound(_) => Code::TaskFileNotFound,
            Self::BatchNotFound(_) => Code::BatchNotFound,
            Self::TaskDeletionWithEmptyQuery => Code::MissingTaskFilters,
            Self::TaskCancelationWithEmptyQuery => Code::MissingTaskFilters,
            // TODO: not sure of the Code to use
            Self::NoSpaceLeftInTaskQueue => Code::NoSpaceLeftOnDevice,
            Self::Dump(e) => e.error_code(),
            Self::Milli { error, .. } => error.error_code(),
            Self::ProcessBatchPanicked(_) => Code::Internal,
            Self::Heed(e) => e.error_code(),
            Self::HeedTransaction(e) => e.error_code(),
            Self::FileStore(e) => e.error_code(),
            Self::IoError(e) => e.error_code(),
            Self::Persist(e) => e.error_code(),
            Self::FeatureNotEnabled(_) => Code::FeatureNotEnabled,

            // Irrecoverable errors
            Self::Anyhow(_) => Code::Internal,
            Self::CorruptedTaskQueue => Code::Internal,
            Self::CorruptedDump => Code::Internal,
            Self::DatabaseUpgrade(_) => Code::Internal,
            Self::UnrecoverableError(_) => Code::Internal,
            Self::CreateBatch(_) => Code::Internal,

            // This one should never be seen by the end user
            Self::AbortedTask => Code::Internal,

            #[cfg(test)]
            Self::PlannedFailure => Code::Internal,
        }
    }
}

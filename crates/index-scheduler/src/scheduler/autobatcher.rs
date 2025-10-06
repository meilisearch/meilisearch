/*!
The autobatcher is responsible for combining the next enqueued
tasks affecting a single index into a [batch](crate::batch::Batch).

The main function of the autobatcher is [`next_autobatch`].
*/

use std::ops::ControlFlow::{self, Break, Continue};

use meilisearch_types::tasks::{BatchStopReason, PrimaryKeyMismatchReason, TaskId};

use crate::KindWithContent;

/// Succinctly describes a task's [`Kind`](meilisearch_types::tasks::Kind)
/// for the purpose of simplifying the implementation of the autobatcher.
///
/// Only the non-prioritised tasks that can be grouped in a batch have a corresponding [`AutobatchKind`]
enum AutobatchKind {
    DocumentImport { allow_index_creation: bool, primary_key: Option<String> },
    DocumentEdition,
    DocumentDeletion { by_filter: bool },
    DocumentClear,
    Settings { allow_index_creation: bool },
    IndexCreation,
    IndexDeletion,
    IndexUpdate,
    IndexSwap,
    IndexCompaction,
}

impl AutobatchKind {
    #[rustfmt::skip]
    fn allow_index_creation(&self) -> Option<bool> {
        match self {
            AutobatchKind::DocumentImport { allow_index_creation, .. }
            | AutobatchKind::Settings { allow_index_creation, .. } => Some(*allow_index_creation),
            _ => None,
        }
    }

    fn primary_key(&self) -> Option<Option<&str>> {
        match self {
            AutobatchKind::DocumentImport { primary_key, .. } => Some(primary_key.as_deref()),
            _ => None,
        }
    }
}

impl From<KindWithContent> for AutobatchKind {
    fn from(kind: KindWithContent) -> Self {
        match kind {
            KindWithContent::DocumentAdditionOrUpdate {
                allow_index_creation, primary_key, ..
            } => AutobatchKind::DocumentImport { allow_index_creation, primary_key },
            KindWithContent::DocumentEdition { .. } => AutobatchKind::DocumentEdition,
            KindWithContent::DocumentDeletion { .. } => {
                AutobatchKind::DocumentDeletion { by_filter: false }
            }
            KindWithContent::DocumentClear { .. } => AutobatchKind::DocumentClear,
            KindWithContent::DocumentDeletionByFilter { .. } => {
                AutobatchKind::DocumentDeletion { by_filter: true }
            }
            KindWithContent::SettingsUpdate { allow_index_creation, is_deletion, .. } => {
                AutobatchKind::Settings {
                    allow_index_creation: allow_index_creation && !is_deletion,
                }
            }
            KindWithContent::IndexDeletion { .. } => AutobatchKind::IndexDeletion,
            KindWithContent::IndexCreation { .. } => AutobatchKind::IndexCreation,
            KindWithContent::IndexUpdate { .. } => AutobatchKind::IndexUpdate,
            KindWithContent::IndexSwap { .. } => AutobatchKind::IndexSwap,
            KindWithContent::IndexCompaction { .. } => AutobatchKind::IndexCompaction,
            KindWithContent::TaskCancelation { .. }
            | KindWithContent::TaskDeletion { .. }
            | KindWithContent::DumpCreation { .. }
            | KindWithContent::Export { .. }
            | KindWithContent::UpgradeDatabase { .. }
            | KindWithContent::SnapshotCreation => {
                panic!("The autobatcher should never be called with tasks that don't apply to an index.")
            }
        }
    }
}

#[derive(Debug)]
pub enum BatchKind {
    DocumentClear {
        ids: Vec<TaskId>,
    },
    DocumentOperation {
        allow_index_creation: bool,
        primary_key: Option<String>,
        operation_ids: Vec<TaskId>,
    },
    DocumentEdition {
        id: TaskId,
    },
    DocumentDeletion {
        deletion_ids: Vec<TaskId>,
        includes_by_filter: bool,
    },
    ClearAndSettings {
        other: Vec<TaskId>,
        allow_index_creation: bool,
        settings_ids: Vec<TaskId>,
    },
    Settings {
        allow_index_creation: bool,
        settings_ids: Vec<TaskId>,
    },
    IndexDeletion {
        ids: Vec<TaskId>,
    },
    IndexCreation {
        id: TaskId,
    },
    IndexUpdate {
        id: TaskId,
    },
    IndexSwap {
        id: TaskId,
    },
    IndexCompaction {
        id: TaskId,
    },
}

impl BatchKind {
    #[rustfmt::skip]
    fn allow_index_creation(&self) -> Option<bool> {
        match self {
            BatchKind::DocumentOperation { allow_index_creation, .. }
            | BatchKind::ClearAndSettings { allow_index_creation, .. }
            | BatchKind::Settings { allow_index_creation, .. } => Some(*allow_index_creation),
            _ => None,
        }
    }

    fn primary_key(&self) -> Option<Option<&str>> {
        match self {
            BatchKind::DocumentOperation { primary_key, .. } => Some(primary_key.as_deref()),
            _ => None,
        }
    }
}

impl BatchKind {
    /// Returns a `ControlFlow::Break` if you must stop right now.
    /// The boolean tell you if an index has been created by the batched task.
    /// To ease the writing of the code. `true` can be returned when you don't need to create an index
    /// but false can't be returned if you needs to create an index.
    // TODO use an AutoBatchKind as input
    pub fn new(
        task_id: TaskId,
        kind_with_content: KindWithContent,
        primary_key: Option<&str>,
    ) -> (ControlFlow<(BatchKind, BatchStopReason), BatchKind>, bool) {
        use AutobatchKind as K;

        let kind = kind_with_content.as_kind();

        match AutobatchKind::from(kind_with_content) {
            K::IndexCreation => (
                Break((
                    BatchKind::IndexCreation { id: task_id },
                    BatchStopReason::TaskCannotBeBatched { kind, id: task_id },
                )),
                true,
            ),
            K::IndexDeletion => (
                Break((
                    BatchKind::IndexDeletion { ids: vec![task_id] },
                    BatchStopReason::IndexDeletion { id: task_id },
                )),
                false,
            ),
            K::IndexUpdate => (
                Break((
                    BatchKind::IndexUpdate { id: task_id },
                    BatchStopReason::TaskCannotBeBatched { kind, id: task_id },
                )),
                false,
            ),
            K::IndexSwap => (
                Break((
                    BatchKind::IndexSwap { id: task_id },
                    BatchStopReason::TaskCannotBeBatched { kind, id: task_id },
                )),
                false,
            ),
            K::IndexCompaction => (
                Break((
                    BatchKind::IndexCompaction { id: task_id },
                    BatchStopReason::TaskCannotBeBatched { kind, id: task_id },
                )),
                false,
            ),
            K::DocumentClear => (Continue(BatchKind::DocumentClear { ids: vec![task_id] }), false),
            K::DocumentImport { allow_index_creation, primary_key: pk }
                if primary_key.is_none() || pk.is_none() || primary_key == pk.as_deref() =>
            {
                (
                    Continue(BatchKind::DocumentOperation {
                        allow_index_creation,
                        primary_key: pk,
                        operation_ids: vec![task_id],
                    }),
                    allow_index_creation,
                )
            }
            // if the primary key set in the task was different than ours we should stop and make this batch fail asap.
            K::DocumentImport { allow_index_creation, primary_key: pk } => (
                Break((
                    BatchKind::DocumentOperation {
                        allow_index_creation,
                        primary_key: pk.clone(),
                        operation_ids: vec![task_id],
                    },
                    BatchStopReason::PrimaryKeyIndexMismatch {
                        id: task_id,
                        in_index: primary_key.unwrap().to_owned(),
                        in_task: pk.unwrap(),
                    },
                )),
                allow_index_creation,
            ),
            K::DocumentEdition => (
                Break((
                    BatchKind::DocumentEdition { id: task_id },
                    BatchStopReason::TaskCannotBeBatched { kind, id: task_id },
                )),
                false,
            ),
            K::DocumentDeletion { by_filter: includes_by_filter } => (
                Continue(BatchKind::DocumentDeletion {
                    deletion_ids: vec![task_id],
                    includes_by_filter,
                }),
                false,
            ),
            K::Settings { allow_index_creation } => (
                Continue(BatchKind::Settings { allow_index_creation, settings_ids: vec![task_id] }),
                allow_index_creation,
            ),
        }
    }

    /// Returns a `ControlFlow::Break` if you must stop right now.
    /// The boolean tell you if an index has been created by the batched task.
    /// To ease the writing of the code. `true` can be returned when you don't need to create an index
    /// but false can't be returned if you needs to create an index.
    #[rustfmt::skip]
    fn accumulate(self, id: TaskId, kind_with_content: KindWithContent, index_already_exists: bool, primary_key: Option<&str>) -> ControlFlow<(BatchKind, BatchStopReason), BatchKind> {
        use AutobatchKind as K;

        let kind = kind_with_content.as_kind();
        let autobatch_kind = AutobatchKind::from(kind_with_content);

        let pk: Option<String> = match (self.primary_key(), autobatch_kind.primary_key(), primary_key) {
            // 1. If incoming task don't interact with primary key -> we can continue
            (batch_pk, None | Some(None), _) => {
                batch_pk.flatten().map(ToOwned::to_owned)
            },
            // 2.1 If we already have a primary-key ->
            // 2.1.1 If the task we're trying to accumulate have a pk it must be equal to our primary key
            (_batch_pk, Some(Some(task_pk)), Some(index_pk)) => if task_pk == index_pk {
                Some(task_pk.to_owned())
            } else {
                return Break((self, BatchStopReason::PrimaryKeyMismatch {
                    id,
                    reason: PrimaryKeyMismatchReason::TaskPrimaryKeyDifferFromIndexPrimaryKey {
                        task_pk: task_pk.to_owned(),
                        index_pk: index_pk.to_owned(),
                    },
                }))
            },
            // 2.2 If we don't have a primary-key ->
            // 2.2.2 If the batch is set to Some(None), the task should be too
            (Some(None), Some(Some(task_pk)), None) => return Break((self, BatchStopReason::PrimaryKeyMismatch {
                id,
                reason: PrimaryKeyMismatchReason::CannotInterfereWithPrimaryKeyGuessing {
                    task_pk: task_pk.to_owned(),
                },
            })),
            (Some(Some(batch_pk)), Some(Some(task_pk)), None) => if task_pk == batch_pk {
                Some(task_pk.to_owned())
            } else {
                let batch_pk = batch_pk.to_owned();
                let task_pk = task_pk.to_owned();
                return Break((self, BatchStopReason::PrimaryKeyMismatch {
                    id,
                    reason: PrimaryKeyMismatchReason::TaskPrimaryKeyDifferFromCurrentBatchPrimaryKey {
                        batch_pk,
                        task_pk
                    },
                }))
            },
            (None, Some(Some(task_pk)), None) => Some(task_pk.to_owned())
        };

        match (self, autobatch_kind) {
            // We don't batch any of these operations
            (this, K::IndexCreation | K::IndexUpdate | K::IndexSwap | K::DocumentEdition | K::IndexCompaction) => {
                Break((this, BatchStopReason::TaskCannotBeBatched { kind, id }))
            },
            // We must not batch tasks that don't have the same index creation rights if the index doesn't already exists.
            (this, kind) if !index_already_exists && this.allow_index_creation() == Some(false) && kind.allow_index_creation() == Some(true) => {
                Break((this, BatchStopReason::IndexCreationMismatch { id }))
            },
            // The index deletion can batch with everything but must stop after
            (
                BatchKind::DocumentClear { mut ids }
                | BatchKind::DocumentDeletion { deletion_ids: mut ids, includes_by_filter: _ }
                | BatchKind::DocumentOperation { allow_index_creation: _, primary_key: _, operation_ids: mut ids }
                | BatchKind::Settings { allow_index_creation: _, settings_ids: mut ids },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                Break((BatchKind::IndexDeletion { ids }, BatchStopReason::IndexDeletion { id }))
            }
            (
                BatchKind::ClearAndSettings { settings_ids: mut ids, allow_index_creation: _, mut other },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                ids.append(&mut other);
                Break((BatchKind::IndexDeletion { ids }, BatchStopReason::IndexDeletion { id }))
            }

            (
                BatchKind::DocumentClear { mut ids },
                K::DocumentClear | K::DocumentDeletion { by_filter: _ },
            ) => {
                ids.push(id);
                Continue(BatchKind::DocumentClear { ids })
            }
            (
                this @ BatchKind::DocumentClear { .. },
                K::DocumentImport { .. } | K::Settings { .. },
            ) => Break((this, BatchStopReason::DocumentOperationWithSettings { id })),
            (
                BatchKind::DocumentOperation { allow_index_creation: _, primary_key: _, mut operation_ids },
                K::DocumentClear,
            ) => {
                operation_ids.push(id);
                Continue(BatchKind::DocumentClear { ids: operation_ids })
            }

            // we can autobatch different kind of document operations and mix replacements with updates
            (
                BatchKind::DocumentOperation { allow_index_creation, primary_key: _, mut operation_ids },
                K::DocumentImport { primary_key: _, .. },
            ) => {
                operation_ids.push(id);
                Continue(BatchKind::DocumentOperation {
                    allow_index_creation,
                    operation_ids,
                    primary_key: pk,
                })
            }
            (
                BatchKind::DocumentOperation { allow_index_creation, primary_key: _, mut operation_ids },
                K::DocumentDeletion { by_filter: false },
            ) => {
                operation_ids.push(id);

                Continue(BatchKind::DocumentOperation {
                    allow_index_creation,
                    operation_ids,
                    primary_key: pk,
                })
            }
            // We can't batch a document operation with a delete by filter
            (
                this @ BatchKind::DocumentOperation { .. },
                K::DocumentDeletion { by_filter: true },
            ) => {
                Break((this, BatchStopReason::DocumentOperationWithDeletionByFilter { id }))
            }
            (
                this @ BatchKind::DocumentOperation { .. },
                K::Settings { .. },
            ) => Break((this, BatchStopReason::DocumentOperationWithSettings { id })),

            (BatchKind::DocumentDeletion { mut deletion_ids, includes_by_filter: _ }, K::DocumentClear) => {
                deletion_ids.push(id);
                Continue(BatchKind::DocumentClear { ids: deletion_ids })
            }
            // we can't autobatch the deletion and import if the document deletion contained a filter
            (
                this @ BatchKind::DocumentDeletion { deletion_ids: _, includes_by_filter: true },
                K::DocumentImport { .. }
            ) => Break((this, BatchStopReason::DeletionByFilterWithDocumentOperation { id })),
            // we can autobatch the deletion and import if the index already exists
            (
                BatchKind::DocumentDeletion { mut deletion_ids, includes_by_filter: false },
                K::DocumentImport { allow_index_creation, primary_key }
            ) if index_already_exists => {
                deletion_ids.push(id);

                Continue(BatchKind::DocumentOperation {
                    allow_index_creation,
                    primary_key,
                    operation_ids: deletion_ids,
                })
            }
            // we can autobatch the deletion and import if both can't create an index
            (
                BatchKind::DocumentDeletion { mut deletion_ids, includes_by_filter: false },
                K::DocumentImport { allow_index_creation, primary_key }
            ) if !allow_index_creation => {
                deletion_ids.push(id);

                Continue(BatchKind::DocumentOperation {
                    allow_index_creation,
                    primary_key,
                    operation_ids: deletion_ids,
                })
            }
            // we can't autobatch a deletion and an import if the index does not exist but would be created by an addition
            (
                this @ BatchKind::DocumentDeletion { .. },
                K::DocumentImport { .. }
            ) => {
                Break((this, BatchStopReason::IndexCreationMismatch { id }))
            }
            (BatchKind::DocumentDeletion { mut deletion_ids, includes_by_filter }, K::DocumentDeletion { by_filter }) => {
                deletion_ids.push(id);
                Continue(BatchKind::DocumentDeletion { deletion_ids, includes_by_filter: includes_by_filter | by_filter })
            }
            (this @ BatchKind::DocumentDeletion { .. }, K::Settings { .. }) => Break((this, BatchStopReason::DocumentOperationWithSettings { id })),

            (
                BatchKind::Settings { settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => Continue(BatchKind::ClearAndSettings {
                settings_ids,
                allow_index_creation,
                other: vec![id],
            }),
            (
                this @ BatchKind::Settings { .. },
                K::DocumentImport { .. } | K::DocumentDeletion { .. },
            ) => Break((this, BatchStopReason::SettingsWithDocumentOperation { id })),
            (
                BatchKind::Settings { mut settings_ids, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(BatchKind::Settings {
                    allow_index_creation,
                    settings_ids,
                })
            }

            (
                BatchKind::ClearAndSettings { mut other, settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => {
                other.push(id);
                Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (this @ BatchKind::ClearAndSettings { .. }, K::DocumentImport { .. }) => Break((this, BatchStopReason::SettingsWithDocumentOperation { id })),
            (
                BatchKind::ClearAndSettings {
                    mut other,
                    settings_ids,
                    allow_index_creation,
                },
                K::DocumentDeletion { .. },
            ) => {
                other.push(id);
                Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (
                BatchKind::ClearAndSettings { mut settings_ids, other, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }

            (
                BatchKind::IndexCreation { .. }
                | BatchKind::IndexDeletion { .. }
                | BatchKind::IndexUpdate { .. }
                | BatchKind::IndexSwap { .. }
                | BatchKind::IndexCompaction { .. }
                | BatchKind::DocumentEdition { .. },
                _,
            ) => {
                unreachable!()
            }
        }
    }
}

/// Create a batch from an ordered list of tasks.
///
/// ## Preconditions
/// 1. The tasks must be enqueued and given in the order in which they were enqueued
/// 2. The tasks must not be prioritised tasks (e.g. task cancellation, dump, snapshot, task deletion)
/// 3. The tasks must all be related to the same index
///
/// ## Return
/// `None` if the list of tasks is empty. Otherwise, an [`AutoBatch`] that represents
/// a subset of the given tasks.
pub fn autobatch(
    enqueued: Vec<(TaskId, KindWithContent)>,
    index_already_exists: bool,
    primary_key: Option<&str>,
) -> Option<(BatchKind, bool, Option<BatchStopReason>)> {
    let mut enqueued = enqueued.into_iter();
    let (id, kind) = enqueued.next()?;

    // index_exist will keep track of if the index should exist at this point after the tasks we batched.
    let mut index_exist = index_already_exists;

    let (mut acc, must_create_index) = match BatchKind::new(id, kind, primary_key) {
        (Continue(acc), create) => (acc, create),
        (Break((acc, batch_stop_reason)), create) => {
            return Some((acc, create, Some(batch_stop_reason)))
        }
    };

    // if an index has been created in the previous step we can consider it as existing.
    index_exist |= must_create_index;

    for (id, kind_with_content) in enqueued {
        acc = match acc.accumulate(id, kind_with_content, index_exist, primary_key) {
            Continue(acc) => acc,
            Break((acc, batch_stop_reason)) => {
                return Some((acc, must_create_index, Some(batch_stop_reason)))
            }
        };
    }

    Some((acc, must_create_index, None))
}

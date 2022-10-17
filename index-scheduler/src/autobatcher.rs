/*!
The autobatcher is responsible for combining the next enqueued
tasks affecting a single index into a [batch](crate::batch::Batch).

The main function of the autobatcher is [`next_autobatch`].
*/

use crate::TaskOperation;
use meilisearch_types::milli::update::IndexDocumentsMethod::{
    self, ReplaceDocuments, UpdateDocuments,
};
use meilisearch_types::tasks::TaskId;
use std::ops::ControlFlow::{self, Break, Continue};

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
pub fn next_autobatch(enqueued: Vec<(TaskId, TaskOperation)>) -> Option<AutoBatch> {
    let mut enqueued = enqueued.into_iter();
    let (id, kind) = enqueued.next()?;
    let mut acc = match AutoBatch::new(id, kind) {
        Continue(acc) => acc,
        Break(acc) => return Some(acc),
    };

    for (id, operation) in enqueued {
        acc = match acc.accumulate(id, operation.into()) {
            Continue(acc) => acc,
            Break(acc) => return Some(acc),
        };
    }

    Some(acc)
}

/// Succinctly describes a [`TaskOperation`](crate::TaskOperation) for the
/// purpose of simplifying the implementation of the autobatcher.
enum SimplifiedTaskOperation {
    DocumentImport {
        method: IndexDocumentsMethod,
        allow_index_creation: bool,
    },
    DocumentDeletion,
    DocumentClear,
    Settings {
        allow_index_creation: bool,
    },
    IndexCreation,
    IndexDeletion,
    IndexUpdate,
    IndexSwap,
    CancelTask,
    TaskDeletion,
    DumpExport,
    Snapshot,
}

impl SimplifiedTaskOperation {
    #[rustfmt::skip]
    fn allow_index_creation(&self) -> Option<bool> {
        match self {
            SimplifiedTaskOperation::DocumentImport { allow_index_creation, .. }
            | SimplifiedTaskOperation::Settings { allow_index_creation, .. } => Some(*allow_index_creation),
            _ => None,
        }
    }
}

impl From<TaskOperation> for SimplifiedTaskOperation {
    fn from(operation: TaskOperation) -> Self {
        match operation {
            TaskOperation::DocumentImport {
                method,
                allow_index_creation,
                ..
            } => SimplifiedTaskOperation::DocumentImport {
                method,
                allow_index_creation,
            },
            TaskOperation::DocumentDeletion { .. } => SimplifiedTaskOperation::DocumentDeletion,
            TaskOperation::DocumentClear { .. } => SimplifiedTaskOperation::DocumentClear,
            TaskOperation::Settings {
                allow_index_creation,
                ..
            } => SimplifiedTaskOperation::Settings {
                allow_index_creation,
            },
            TaskOperation::IndexDeletion { .. } => SimplifiedTaskOperation::IndexDeletion,
            TaskOperation::IndexCreation { .. } => SimplifiedTaskOperation::IndexCreation,
            TaskOperation::IndexUpdate { .. } => SimplifiedTaskOperation::IndexUpdate,
            TaskOperation::IndexSwap { .. } => SimplifiedTaskOperation::IndexSwap,
            TaskOperation::CancelTask { .. } => SimplifiedTaskOperation::CancelTask,
            TaskOperation::TaskDeletion { .. } => SimplifiedTaskOperation::TaskDeletion,
            TaskOperation::DumpExport { .. } => SimplifiedTaskOperation::DumpExport,
            TaskOperation::Snapshot => SimplifiedTaskOperation::Snapshot,
        }
    }
}

/// Describes a [batch](crate::batch::Batch) created by the autobatcher.
#[derive(Debug)]
pub enum AutoBatch {
    DocumentClear {
        ids: Vec<TaskId>,
    },
    DocumentImport {
        method: IndexDocumentsMethod,
        allow_index_creation: bool,
        import_ids: Vec<TaskId>,
    },
    DocumentDeletion {
        deletion_ids: Vec<TaskId>,
    },
    ClearAndSettings {
        other: Vec<TaskId>,
        allow_index_creation: bool,
        settings_ids: Vec<TaskId>,
    },
    SettingsAndDocumentImport {
        settings_ids: Vec<TaskId>,
        method: IndexDocumentsMethod,
        allow_index_creation: bool,
        import_ids: Vec<TaskId>,
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
}

impl AutoBatch {
    #[rustfmt::skip]
    fn allow_index_creation(&self) -> Option<bool> {
        match self {
            AutoBatch::DocumentImport { allow_index_creation, .. }
            | AutoBatch::ClearAndSettings { allow_index_creation, .. }
            | AutoBatch::SettingsAndDocumentImport { allow_index_creation, .. }
            | AutoBatch::Settings { allow_index_creation, .. } => Some(*allow_index_creation),
            _ => None,
        }
    }
}

impl AutoBatch {
    /// Returns a `ControlFlow::Break` if you must stop right now.
    pub fn new(task_id: TaskId, operation: TaskOperation) -> ControlFlow<AutoBatch, AutoBatch> {
        use SimplifiedTaskOperation as K;

        match SimplifiedTaskOperation::from(operation) {
            K::IndexCreation => Break(AutoBatch::IndexCreation { id: task_id }),
            K::IndexDeletion => Break(AutoBatch::IndexDeletion { ids: vec![task_id] }),
            K::IndexUpdate => Break(AutoBatch::IndexUpdate { id: task_id }),
            K::IndexSwap => Break(AutoBatch::IndexSwap { id: task_id }),
            K::DocumentClear => Continue(AutoBatch::DocumentClear { ids: vec![task_id] }),
            K::DocumentImport {
                method,
                allow_index_creation,
            } => Continue(AutoBatch::DocumentImport {
                method,
                allow_index_creation,
                import_ids: vec![task_id],
            }),
            K::DocumentDeletion => Continue(AutoBatch::DocumentDeletion {
                deletion_ids: vec![task_id],
            }),
            K::Settings {
                allow_index_creation,
            } => Continue(AutoBatch::Settings {
                allow_index_creation,
                settings_ids: vec![task_id],
            }),
            K::DumpExport | K::Snapshot | K::CancelTask | K::TaskDeletion => {
                unreachable!()
            }
        }
    }

    /// Returns a `ControlFlow::Break` if you must stop right now.
    #[rustfmt::skip]
    fn accumulate(self, id: TaskId, operation: SimplifiedTaskOperation) -> ControlFlow<AutoBatch, AutoBatch> {
        use SimplifiedTaskOperation as K;

        match (self, operation) {
            // We don't batch any of these operations
            (this, K::IndexCreation | K::IndexUpdate | K::IndexSwap) => Break(this),
            // We must not batch tasks that don't have the same index creation rights
            (this, kind) if this.allow_index_creation() == Some(false) && kind.allow_index_creation() == Some(true) => {
                Break(this)
            },
            // The index deletion can batch with everything but must stop after
            (
                AutoBatch::DocumentClear { mut ids }
                | AutoBatch::DocumentDeletion { deletion_ids: mut ids }
                | AutoBatch::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids }
                | AutoBatch::Settings { allow_index_creation: _, settings_ids: mut ids },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                Break(AutoBatch::IndexDeletion { ids })
            }
            (
                AutoBatch::ClearAndSettings { settings_ids: mut ids, allow_index_creation: _, mut other }
                | AutoBatch::SettingsAndDocumentImport { import_ids: mut ids, method: _, allow_index_creation: _, settings_ids: mut other },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                ids.append(&mut other);
                Break(AutoBatch::IndexDeletion { ids })
            }

            (
                AutoBatch::DocumentClear { mut ids },
                K::DocumentClear | K::DocumentDeletion,
            ) => {
                ids.push(id);
                Continue(AutoBatch::DocumentClear { ids })
            }
            (
                this @ AutoBatch::DocumentClear { .. },
                K::DocumentImport { .. } | K::Settings { .. },
            ) => Break(this),
            (
                AutoBatch::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids },
                K::DocumentClear,
            ) => {
                ids.push(id);
                Continue(AutoBatch::DocumentClear { ids })
            }

            // we can autobatch the same kind of document additions / updates
            (
                AutoBatch::DocumentImport { method: ReplaceDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: ReplaceDocuments, .. },
            ) => {
                import_ids.push(id);
                Continue(AutoBatch::DocumentImport {
                    method: ReplaceDocuments,
                    allow_index_creation,
                    import_ids,
                })
            }
            (
                AutoBatch::DocumentImport { method: UpdateDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: UpdateDocuments, .. },
            ) => {
                import_ids.push(id);
                Continue(AutoBatch::DocumentImport {
                    method: UpdateDocuments,
                    allow_index_creation,
                    import_ids,
                })
            }

            // but we can't autobatch documents if it's not the same kind
            // this match branch MUST be AFTER the previous one
            (
                this @ AutoBatch::DocumentImport { .. },
                K::DocumentDeletion | K::DocumentImport { .. },
            ) => Break(this),

            (
                AutoBatch::DocumentImport { method, allow_index_creation, import_ids },
                K::Settings { .. },
            ) => Continue(AutoBatch::SettingsAndDocumentImport {
                settings_ids: vec![id],
                method,
                allow_index_creation,
                import_ids,
            }),

            (AutoBatch::DocumentDeletion { mut deletion_ids }, K::DocumentClear) => {
                deletion_ids.push(id);
                Continue(AutoBatch::DocumentClear { ids: deletion_ids })
            }
            (this @ AutoBatch::DocumentDeletion { .. }, K::DocumentImport { .. }) => Break(this),
            (AutoBatch::DocumentDeletion { mut deletion_ids }, K::DocumentDeletion) => {
                deletion_ids.push(id);
                Continue(AutoBatch::DocumentDeletion { deletion_ids })
            }
            (this @ AutoBatch::DocumentDeletion { .. }, K::Settings { .. }) => Break(this),

            (
                AutoBatch::Settings { settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => Continue(AutoBatch::ClearAndSettings {
                settings_ids: settings_ids,
                allow_index_creation,
                other: vec![id],
            }),
            (
                this @ AutoBatch::Settings { .. },
                K::DocumentImport { .. } | K::DocumentDeletion,
            ) => Break(this),
            (
                AutoBatch::Settings { mut settings_ids, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(AutoBatch::Settings {
                    allow_index_creation,
                    settings_ids,
                })
            }

            (
                AutoBatch::ClearAndSettings { mut other, settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => {
                other.push(id);
                Continue(AutoBatch::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (this @ AutoBatch::ClearAndSettings { .. }, K::DocumentImport { .. }) => Break(this),
            (
                AutoBatch::ClearAndSettings {
                    mut other,
                    settings_ids,
                    allow_index_creation,
                },
                K::DocumentDeletion,
            ) => {
                other.push(id);
                Continue(AutoBatch::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (
                AutoBatch::ClearAndSettings { mut settings_ids, other, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(AutoBatch::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (
                AutoBatch::SettingsAndDocumentImport { settings_ids, method: _, import_ids: mut other, allow_index_creation },
                K::DocumentClear,
            ) => {
                other.push(id);
                Continue(AutoBatch::ClearAndSettings {
                    settings_ids,
                    other,
                    allow_index_creation,
                })
            }

            (
                AutoBatch::SettingsAndDocumentImport { settings_ids, method: ReplaceDocuments, mut import_ids, allow_index_creation },
                K::DocumentImport { method: ReplaceDocuments, .. },
            ) => {
                import_ids.push(id);
                Continue(AutoBatch::SettingsAndDocumentImport {
                    settings_ids,
                    method: ReplaceDocuments,
                    allow_index_creation,
                    import_ids,
                })
            }
            (
                AutoBatch::SettingsAndDocumentImport { settings_ids, method: UpdateDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: UpdateDocuments, .. },
            ) => {
                import_ids.push(id);
                Continue(AutoBatch::SettingsAndDocumentImport {
                    settings_ids,
                    method: UpdateDocuments,
                    allow_index_creation,
                    import_ids,
                })
            }
            // But we can't batch a settings and a doc op with another doc op
            // this MUST be AFTER the two previous branch
            (
                this @ AutoBatch::SettingsAndDocumentImport { .. },
                K::DocumentDeletion | K::DocumentImport { .. },
            ) => Break(this),
            (
                AutoBatch::SettingsAndDocumentImport { mut settings_ids, method, allow_index_creation, import_ids },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(AutoBatch::SettingsAndDocumentImport {
                    settings_ids,
                    method,
                    allow_index_creation,
                    import_ids,
                })
            }
            (_, K::CancelTask | K::TaskDeletion | K::DumpExport | K::Snapshot) => {
                unreachable!()
            }
            (
                AutoBatch::IndexCreation { .. }
                | AutoBatch::IndexDeletion { .. }
                | AutoBatch::IndexUpdate { .. }
                | AutoBatch::IndexSwap { .. },
                _,
            ) => {
                unreachable!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::debug_snapshot;

    use super::*;
    use uuid::Uuid;

    fn autobatch_from(input: impl IntoIterator<Item = TaskOperation>) -> Option<AutoBatch> {
        next_autobatch(
            input
                .into_iter()
                .enumerate()
                .map(|(id, kind)| (id as TaskId, kind.into()))
                .collect(),
        )
    }

    fn doc_imp(method: IndexDocumentsMethod, allow_index_creation: bool) -> TaskOperation {
        TaskOperation::DocumentImport {
            index_uid: String::from("doggo"),
            primary_key: None,
            method,
            content_file: Uuid::new_v4(),
            documents_count: 0,
            allow_index_creation,
        }
    }

    fn doc_del() -> TaskOperation {
        TaskOperation::DocumentDeletion {
            index_uid: String::from("doggo"),
            documents_ids: Vec::new(),
        }
    }

    fn doc_clr() -> TaskOperation {
        TaskOperation::DocumentClear {
            index_uid: String::from("doggo"),
        }
    }

    fn settings(allow_index_creation: bool) -> TaskOperation {
        TaskOperation::Settings {
            index_uid: String::from("doggo"),
            new_settings: Default::default(),
            is_deletion: false,
            allow_index_creation,
        }
    }

    fn idx_create() -> TaskOperation {
        TaskOperation::IndexCreation {
            index_uid: String::from("doggo"),
            primary_key: None,
        }
    }

    fn idx_update() -> TaskOperation {
        TaskOperation::IndexUpdate {
            index_uid: String::from("doggo"),
            primary_key: None,
        }
    }

    fn idx_del() -> TaskOperation {
        TaskOperation::IndexDeletion {
            index_uid: String::from("doggo"),
        }
    }

    fn idx_swap() -> TaskOperation {
        TaskOperation::IndexSwap {
            lhs: String::from("doggo"),
            rhs: String::from("catto"),
        }
    }

    #[test]
    fn autobatch_simple_operation_together() {
        // we can autobatch one or multiple DocumentAddition together
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true)]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp( ReplaceDocuments, true ), doc_imp(ReplaceDocuments, true )]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1, 2] })");
        // we can autobatch one or multiple DocumentUpdate together
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true)]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true)]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0, 1, 2] })");
        // we can autobatch one or multiple DocumentDeletion together
        debug_snapshot!(autobatch_from([doc_del()]), @"Some(DocumentDeletion { deletion_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_del(), doc_del(), doc_del()]), @"Some(DocumentDeletion { deletion_ids: [0, 1, 2] })");
        // we can autobatch one or multiple Settings together
        debug_snapshot!(autobatch_from([settings(true)]), @"Some(Settings { allow_index_creation: true, settings_ids: [0] })");
        debug_snapshot!(autobatch_from([settings(true), settings(true), settings(true)]), @"Some(Settings { allow_index_creation: true, settings_ids: [0, 1, 2] })");
    }

    #[test]
    fn simple_document_operation_dont_autobatch_with_other() {
        // addition, updates and deletion can't batch together
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(UpdateDocuments, true)]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_del()]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(ReplaceDocuments, true)]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_del()]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_del(), doc_imp(ReplaceDocuments, true)]), @"Some(DocumentDeletion { deletion_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_del(), doc_imp(UpdateDocuments, true)]), @"Some(DocumentDeletion { deletion_ids: [0] })");

        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), idx_create()]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), idx_create()]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_del(), idx_create()]), @"Some(DocumentDeletion { deletion_ids: [0] })");

        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), idx_update()]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), idx_update()]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_del(), idx_update()]), @"Some(DocumentDeletion { deletion_ids: [0] })");

        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), idx_swap()]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), idx_swap()]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_del(), idx_swap()]), @"Some(DocumentDeletion { deletion_ids: [0] })");
    }

    #[test]
    fn document_addition_batch_with_settings() {
        // simple case
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true)]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true)]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");

        // multiple settings and doc addition
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), settings(true), settings(true)]), @"Some(SettingsAndDocumentImport { settings_ids: [2, 3], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), settings(true), settings(true)]), @"Some(SettingsAndDocumentImport { settings_ids: [2, 3], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] })");

        // addition and setting unordered
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_imp(ReplaceDocuments, true), settings(true)]), @"Some(SettingsAndDocumentImport { settings_ids: [1, 3], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 2] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_imp(UpdateDocuments, true), settings(true)]), @"Some(SettingsAndDocumentImport { settings_ids: [1, 3], method: UpdateDocuments, allow_index_creation: true, import_ids: [0, 2] })");

        // We ensure this kind of batch doesn't batch with forbidden operations
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_imp(UpdateDocuments, true)]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_imp(ReplaceDocuments, true)]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_del()]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_del()]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), idx_create()]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), idx_create()]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), idx_update()]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), idx_update()]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), idx_swap()]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), idx_swap()]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
    }

    #[test]
    fn clear_and_additions() {
        // these two doesn't need to batch
        debug_snapshot!(autobatch_from([doc_clr(), doc_imp(ReplaceDocuments, true)]), @"Some(DocumentClear { ids: [0] })");
        debug_snapshot!(autobatch_from([doc_clr(), doc_imp(UpdateDocuments, true)]), @"Some(DocumentClear { ids: [0] })");

        // Basic use case
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), doc_clr()]), @"Some(DocumentClear { ids: [0, 1, 2] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_clr()]), @"Some(DocumentClear { ids: [0, 1, 2] })");

        // This batch kind doesn't mix with other document addition
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), doc_clr(), doc_imp(ReplaceDocuments, true)]), @"Some(DocumentClear { ids: [0, 1, 2] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_clr(), doc_imp(UpdateDocuments, true)]), @"Some(DocumentClear { ids: [0, 1, 2] })");

        // But you can batch multiple clear together
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), doc_clr(), doc_clr(), doc_clr()]), @"Some(DocumentClear { ids: [0, 1, 2, 3, 4] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_clr(), doc_clr(), doc_clr()]), @"Some(DocumentClear { ids: [0, 1, 2, 3, 4] })");
    }

    #[test]
    fn clear_and_additions_and_settings() {
        // A clear don't need to autobatch the settings that happens AFTER there is no documents
        debug_snapshot!(autobatch_from([doc_clr(), settings(true)]), @"Some(DocumentClear { ids: [0] })");

        debug_snapshot!(autobatch_from([settings(true), doc_clr(), settings(true)]), @"Some(ClearAndSettings { other: [1], allow_index_creation: true, settings_ids: [0, 2] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_clr()]), @"Some(ClearAndSettings { other: [0, 2], allow_index_creation: true, settings_ids: [1] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_clr()]), @"Some(ClearAndSettings { other: [0, 2], allow_index_creation: true, settings_ids: [1] })");
    }

    #[test]
    fn anything_and_index_deletion() {
        // The indexdeletion doesn't batch with anything that happens AFTER
        debug_snapshot!(autobatch_from([idx_del(), doc_imp(ReplaceDocuments, true)]), @"Some(IndexDeletion { ids: [0] })");
        debug_snapshot!(autobatch_from([idx_del(), doc_imp(UpdateDocuments, true)]), @"Some(IndexDeletion { ids: [0] })");
        debug_snapshot!(autobatch_from([idx_del(), doc_del()]), @"Some(IndexDeletion { ids: [0] })");
        debug_snapshot!(autobatch_from([idx_del(), doc_clr()]), @"Some(IndexDeletion { ids: [0] })");
        debug_snapshot!(autobatch_from([idx_del(), settings(true)]), @"Some(IndexDeletion { ids: [0] })");

        // The index deletion can accept almost any type of BatchKind and transform it to an idx_del()
        // First, the basic cases
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), idx_del()]), @"Some(IndexDeletion { ids: [0, 1] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), idx_del()]), @"Some(IndexDeletion { ids: [0, 1] })");
        debug_snapshot!(autobatch_from([doc_del(), idx_del()]), @"Some(IndexDeletion { ids: [0, 1] })");
        debug_snapshot!(autobatch_from([doc_clr(), idx_del()]), @"Some(IndexDeletion { ids: [0, 1] })");
        debug_snapshot!(autobatch_from([settings(true), idx_del()]), @"Some(IndexDeletion { ids: [0, 1] })");

        // Then the mixed cases
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), idx_del()]), @"Some(IndexDeletion { ids: [0, 2, 1] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), idx_del()]), @"Some(IndexDeletion { ids: [0, 2, 1] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some(IndexDeletion { ids: [1, 3, 0, 2] })");
        debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some(IndexDeletion { ids: [1, 3, 0, 2] })");
    }

    #[test]
    fn allowed_and_disallowed_index_creation() {
        // doc_imp(indexes canbe)ixed with those disallowed to do so
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, true)]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true)]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, false)]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0, 1] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true)]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, false), settings(true)]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] })");
    }
}

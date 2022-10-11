use meilisearch_types::milli::update::IndexDocumentsMethod::{
    self, ReplaceDocuments, UpdateDocuments,
};
use std::ops::ControlFlow::{self, Break, Continue};

use crate::{task::Kind, TaskId};

#[derive(Debug)]
pub enum BatchKind {
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

impl BatchKind {
    /// Returns a `ControlFlow::Break` if you must stop right now.
    pub fn new(task_id: TaskId, kind: Kind) -> ControlFlow<BatchKind, BatchKind> {
        match kind {
            Kind::IndexCreation => Break(BatchKind::IndexCreation { id: task_id }),
            Kind::IndexDeletion => Break(BatchKind::IndexDeletion { ids: vec![task_id] }),
            Kind::IndexUpdate => Break(BatchKind::IndexUpdate { id: task_id }),
            Kind::IndexSwap => Break(BatchKind::IndexSwap { id: task_id }),
            Kind::DocumentClear => Continue(BatchKind::DocumentClear { ids: vec![task_id] }),
            Kind::DocumentImport {
                method,
                allow_index_creation,
            } => Continue(BatchKind::DocumentImport {
                method,
                allow_index_creation,
                import_ids: vec![task_id],
            }),
            Kind::DocumentDeletion => Continue(BatchKind::DocumentDeletion {
                deletion_ids: vec![task_id],
            }),
            Kind::Settings {
                allow_index_creation,
            } => Continue(BatchKind::Settings {
                allow_index_creation,
                settings_ids: vec![task_id],
            }),
            Kind::DumpExport | Kind::Snapshot | Kind::CancelTask | Kind::DeleteTasks => {
                unreachable!()
            }
        }
    }

    /// Returns a `ControlFlow::Break` if you must stop right now.
    #[rustfmt::skip]
    fn accumulate(self, id: TaskId, kind: Kind) -> ControlFlow<BatchKind, BatchKind> {
        match (self, kind) {
            // We don't batch any of these operations
            (this, Kind::IndexCreation | Kind::IndexUpdate | Kind::IndexSwap) => Break(this),
            // The index deletion can batch with everything but must stop after
            (
                BatchKind::DocumentClear { mut ids }
                | BatchKind::DocumentDeletion { deletion_ids: mut ids }
                | BatchKind::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids }
                | BatchKind::Settings { allow_index_creation: _, settings_ids: mut ids },
                Kind::IndexDeletion,
            ) => {
                ids.push(id);
                Break(BatchKind::IndexDeletion { ids })
            }
            (
                BatchKind::ClearAndSettings { settings_ids: mut ids, allow_index_creation: _, mut other }
                | BatchKind::SettingsAndDocumentImport { import_ids: mut ids, method: _, allow_index_creation: _, settings_ids: mut other },
                Kind::IndexDeletion,
            ) => {
                ids.push(id);
                ids.append(&mut other);
                Break(BatchKind::IndexDeletion { ids })
            }

            (
                BatchKind::DocumentClear { mut ids },
                Kind::DocumentClear | Kind::DocumentDeletion,
            ) => {
                ids.push(id);
                Continue(BatchKind::DocumentClear { ids })
            }
            (
                this @ BatchKind::DocumentClear { .. },
                Kind::DocumentImport { .. } | Kind::Settings { .. },
            ) => Break(this),
            (
                BatchKind::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids },
                Kind::DocumentClear,
            ) => {
                ids.push(id);
                Continue(BatchKind::DocumentClear { ids })
            }

            // We only want to batch together document imports that are allowed to create the index
            // or document imports not allowed to create an index if the first operation can.
            (
                this @ BatchKind::DocumentImport { method: _, allow_index_creation: false, .. },
                Kind::DocumentImport { method: _, allow_index_creation: true },
            ) => Break(this),

            // we can autobatch the same kind of document additions / updates
            (
                BatchKind::DocumentImport { method: ReplaceDocuments, allow_index_creation, mut import_ids },
                Kind::DocumentImport { method: ReplaceDocuments, .. },
            ) => {
                import_ids.push(id);
                Continue(BatchKind::DocumentImport {
                    method: ReplaceDocuments,
                    allow_index_creation,
                    import_ids,
                })
            }
            (
                BatchKind::DocumentImport { method: UpdateDocuments, allow_index_creation, mut import_ids },
                Kind::DocumentImport { method: UpdateDocuments, .. },
            ) => {
                import_ids.push(id);
                Continue(BatchKind::DocumentImport {
                    method: UpdateDocuments,
                    allow_index_creation,
                    import_ids,
                })
            }

            // but we can't autobatch documents if it's not the same kind
            // this match branch MUST be AFTER the previous one
            (
                this @ BatchKind::DocumentImport { .. },
                Kind::DocumentDeletion | Kind::DocumentImport { .. },
            ) => Break(this),

            // We only want to batch together document imports that are allowed to create the index
            // or document imports not allowed to create an index if the first operation can.
            (
                this @ BatchKind::DocumentImport { allow_index_creation: false, .. },
                Kind::Settings { allow_index_creation: true },
            ) => Break(this),
            (
                BatchKind::DocumentImport { method, allow_index_creation, import_ids },
                Kind::Settings { .. },
            ) => Continue(BatchKind::SettingsAndDocumentImport {
                settings_ids: vec![id],
                method,
                allow_index_creation,
                import_ids,
            }),

            (BatchKind::DocumentDeletion { mut deletion_ids }, Kind::DocumentClear) => {
                deletion_ids.push(id);
                Continue(BatchKind::DocumentClear { ids: deletion_ids })
            }
            (this @ BatchKind::DocumentDeletion { .. }, Kind::DocumentImport { .. }) => Break(this),
            (BatchKind::DocumentDeletion { mut deletion_ids }, Kind::DocumentDeletion) => {
                deletion_ids.push(id);
                Continue(BatchKind::DocumentDeletion { deletion_ids })
            }
            (this @ BatchKind::DocumentDeletion { .. }, Kind::Settings { .. }) => Break(this),

            (
                BatchKind::Settings { settings_ids, allow_index_creation },
                Kind::DocumentClear,
            ) => Continue(BatchKind::ClearAndSettings {
                settings_ids: settings_ids,
                allow_index_creation,
                other: vec![id],
            }),
            (
                this @ BatchKind::Settings { .. },
                Kind::DocumentImport { .. } | Kind::DocumentDeletion,
            ) => Break(this),
            (
                this @ BatchKind::Settings { allow_index_creation: false, .. },
                Kind::Settings { allow_index_creation: true },
            ) => Break(this),
            (
                BatchKind::Settings { mut settings_ids, allow_index_creation },
                Kind::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(BatchKind::Settings {
                    allow_index_creation,
                    settings_ids,
                })
            }

            (
                BatchKind::ClearAndSettings { mut other, settings_ids, allow_index_creation },
                Kind::DocumentClear,
            ) => {
                other.push(id);
                Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (this @ BatchKind::ClearAndSettings { .. }, Kind::DocumentImport { .. }) => Break(this),
            (
                BatchKind::ClearAndSettings {
                    mut other,
                    settings_ids,
                    allow_index_creation,
                },
                Kind::DocumentDeletion,
            ) => {
                other.push(id);
                Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (
                this @ BatchKind::ClearAndSettings { allow_index_creation: false, .. },
                Kind::Settings {
                    allow_index_creation: true,
                },
            ) => Break(this),
            (
                BatchKind::ClearAndSettings { mut settings_ids, other, allow_index_creation },
                Kind::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (
                BatchKind::SettingsAndDocumentImport { settings_ids, method: _, import_ids: mut other, allow_index_creation },
                Kind::DocumentClear,
            ) => {
                other.push(id);
                Continue(BatchKind::ClearAndSettings {
                    settings_ids,
                    other,
                    allow_index_creation,
                })
            }

            // we can batch the settings with a kind of document operation with the same kind of document operation
            (
                this @ BatchKind::SettingsAndDocumentImport { allow_index_creation: false, .. },
                Kind::DocumentImport { allow_index_creation: true, .. },
            ) => Break(this),
            (
                BatchKind::SettingsAndDocumentImport { settings_ids, method: ReplaceDocuments, mut import_ids, allow_index_creation },
                Kind::DocumentImport { method: ReplaceDocuments, .. },
            ) => {
                import_ids.push(id);
                Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method: ReplaceDocuments,
                    allow_index_creation,
                    import_ids,
                })
            }
            (
                BatchKind::SettingsAndDocumentImport { settings_ids, method: UpdateDocuments, allow_index_creation, mut import_ids },
                Kind::DocumentImport { method: UpdateDocuments, .. },
            ) => {
                import_ids.push(id);
                Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method: UpdateDocuments,
                    allow_index_creation,
                    import_ids,
                })
            }
            // But we can't batch a settings and a doc op with another doc op
            // this MUST be AFTER the two previous branch
            (
                this @ BatchKind::SettingsAndDocumentImport { .. },
                Kind::DocumentDeletion | Kind::DocumentImport { .. },
            ) => Break(this),
            (
                this @ BatchKind::SettingsAndDocumentImport { allow_index_creation: false, .. },
                Kind::Settings { allow_index_creation: true },
            ) => Break(this),
            (
                BatchKind::SettingsAndDocumentImport { mut settings_ids, method, allow_index_creation, import_ids },
                Kind::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method,
                    allow_index_creation,
                    import_ids,
                })
            }
            (_, Kind::CancelTask | Kind::DeleteTasks | Kind::DumpExport | Kind::Snapshot) => {
                unreachable!()
            }
            (
                BatchKind::IndexCreation { .. }
                | BatchKind::IndexDeletion { .. }
                | BatchKind::IndexUpdate { .. }
                | BatchKind::IndexSwap { .. },
                _,
            ) => {
                unreachable!()
            }
        }
    }
}

pub fn autobatch(enqueued: Vec<(TaskId, Kind)>) -> Option<BatchKind> {
    let mut enqueued = enqueued.into_iter();
    let (id, kind) = enqueued.next()?;
    let mut acc = match BatchKind::new(id, kind) {
        Continue(acc) => acc,
        Break(acc) => return Some(acc),
    };

    for (id, kind) in enqueued {
        acc = match acc.accumulate(id, kind) {
            Continue(acc) => acc,
            Break(acc) => return Some(acc),
        };
    }

    Some(acc)
}

#[cfg(test)]
mod tests {
    use crate::assert_smol_debug_snapshot;

    use super::*;
    use Kind::*;

    fn autobatch_from(input: impl IntoIterator<Item = Kind>) -> Option<BatchKind> {
        autobatch(
            input
                .into_iter()
                .enumerate()
                .map(|(id, kind)| (id as TaskId, kind))
                .collect(),
        )
    }

    #[test]
    fn autobatch_simple_operation_together() {
        // we can autobatch one or multiple DocumentAddition together
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1, 2] })");
        // we can autobatch one or multiple DocumentUpdate together
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentImport { method: UpdateDocuments, allow_index_creation: true }]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0, 1, 2] })");
        // we can autobatch one or multiple DocumentDeletion together
        assert_smol_debug_snapshot!(autobatch_from([DocumentDeletion]), @"Some(DocumentDeletion { deletion_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentDeletion, DocumentDeletion, DocumentDeletion]), @"Some(DocumentDeletion { deletion_ids: [0, 1, 2] })");
        // we can autobatch one or multiple Settings together
        assert_smol_debug_snapshot!(autobatch_from([Settings { allow_index_creation: true }]), @"Some(Settings { allow_index_creation: true, settings_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([Settings { allow_index_creation: true }, Settings { allow_index_creation: true }, Settings { allow_index_creation: true }]), @"Some(Settings { allow_index_creation: true, settings_ids: [0, 1, 2] })");
    }

    #[test]
    fn simple_document_operation_dont_autobatch_with_other() {
        // addition, updates and deletion can't batch together
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentImport { method: UpdateDocuments, allow_index_creation: true }]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentDeletion]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentDeletion]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentDeletion, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(DocumentDeletion { deletion_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentDeletion, DocumentImport { method: UpdateDocuments, allow_index_creation: true }]), @"Some(DocumentDeletion { deletion_ids: [0] })");

        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, IndexCreation]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, IndexCreation]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentDeletion, IndexCreation]), @"Some(DocumentDeletion { deletion_ids: [0] })");

        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, IndexUpdate]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, IndexUpdate]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentDeletion, IndexUpdate]), @"Some(DocumentDeletion { deletion_ids: [0] })");

        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, IndexSwap]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, IndexSwap]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentDeletion, IndexSwap]), @"Some(DocumentDeletion { deletion_ids: [0] })");
    }

    #[test]
    fn document_addition_batch_with_settings() {
        // simple case
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");

        // multiple settings and doc addition
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, Settings { allow_index_creation: true }]), @"Some(SettingsAndDocumentImport { settings_ids: [2, 3], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, Settings { allow_index_creation: true }]), @"Some(SettingsAndDocumentImport { settings_ids: [2, 3], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] })");

        // addition and setting unordered
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }]), @"Some(SettingsAndDocumentImport { settings_ids: [1, 3], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }]), @"Some(SettingsAndDocumentImport { settings_ids: [1, 3], method: UpdateDocuments, allow_index_creation: true, import_ids: [0, 2] })");

        // We ensure this kind of batch doesn't batch with forbidden operations
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentImport { method: UpdateDocuments, allow_index_creation: true }]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentDeletion]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentDeletion]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, IndexCreation]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, IndexCreation]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, IndexUpdate]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, IndexUpdate]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, IndexSwap]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, IndexSwap]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
    }

    #[test]
    fn clear_and_additions() {
        // these two doesn't need to batch
        assert_smol_debug_snapshot!(autobatch_from([DocumentClear, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(DocumentClear { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentClear, DocumentImport { method: UpdateDocuments, allow_index_creation: true }]), @"Some(DocumentClear { ids: [0] })");

        // Basic use case
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentClear]), @"Some(DocumentClear { ids: [0, 1, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentClear]), @"Some(DocumentClear { ids: [0, 1, 2] })");

        // This batch kind doesn't mix with other document addition
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentClear, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(DocumentClear { ids: [0, 1, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentClear, DocumentImport { method: UpdateDocuments, allow_index_creation: true }]), @"Some(DocumentClear { ids: [0, 1, 2] })");

        // But you can batch multiple clear together
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentClear, DocumentClear, DocumentClear]), @"Some(DocumentClear { ids: [0, 1, 2, 3, 4] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentImport { method: UpdateDocuments, allow_index_creation: true }, DocumentClear, DocumentClear, DocumentClear]), @"Some(DocumentClear { ids: [0, 1, 2, 3, 4] })");
    }

    #[test]
    fn clear_and_additions_and_settings() {
        // A clear don't need to autobatch the settings that happens AFTER there is no documents
        assert_smol_debug_snapshot!(autobatch_from([DocumentClear, Settings { allow_index_creation: true }]), @"Some(DocumentClear { ids: [0] })");

        assert_smol_debug_snapshot!(autobatch_from([Settings { allow_index_creation: true }, DocumentClear, Settings { allow_index_creation: true }]), @"Some(ClearAndSettings { other: [1], allow_index_creation: true, settings_ids: [0, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentClear]), @"Some(ClearAndSettings { other: [0, 2], allow_index_creation: true, settings_ids: [1] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentClear]), @"Some(ClearAndSettings { other: [0, 2], allow_index_creation: true, settings_ids: [1] })");
    }

    #[test]
    fn anything_and_index_deletion() {
        // The indexdeletion doesn't batch with anything that happens AFTER
        assert_smol_debug_snapshot!(autobatch_from([IndexDeletion, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(IndexDeletion { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([IndexDeletion, DocumentImport { method: UpdateDocuments, allow_index_creation: true }]), @"Some(IndexDeletion { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([IndexDeletion, DocumentDeletion]), @"Some(IndexDeletion { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([IndexDeletion, DocumentClear]), @"Some(IndexDeletion { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([IndexDeletion, Settings { allow_index_creation: true }]), @"Some(IndexDeletion { ids: [0] })");

        // The index deletion can accept almost any type of BatchKind and transform it to an IndexDeletion
        // First, the basic cases
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, IndexDeletion]), @"Some(IndexDeletion { ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, IndexDeletion]), @"Some(IndexDeletion { ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentDeletion, IndexDeletion]), @"Some(IndexDeletion { ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentClear, IndexDeletion]), @"Some(IndexDeletion { ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([Settings { allow_index_creation: true }, IndexDeletion]), @"Some(IndexDeletion { ids: [0, 1] })");

        // Then the mixed cases
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, IndexDeletion]), @"Some(IndexDeletion { ids: [0, 2, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, IndexDeletion]), @"Some(IndexDeletion { ids: [0, 2, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentClear, IndexDeletion]), @"Some(IndexDeletion { ids: [1, 3, 0, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: UpdateDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }, DocumentClear, IndexDeletion]), @"Some(IndexDeletion { ids: [1, 3, 0, 2] })");
    }

    #[test]
    fn allowed_and_disallowed_index_creation() {
        // DocumentImport that can create indexes can't be mixed with those disallowed to do so
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: false }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, DocumentImport { method: ReplaceDocuments, allow_index_creation: true }]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: false }, DocumentImport { method: ReplaceDocuments, allow_index_creation: false }]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: true }, Settings { allow_index_creation: true }]), @"Some(SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([DocumentImport { method: ReplaceDocuments, allow_index_creation: false }, Settings { allow_index_creation: true }]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] })");
    }
}

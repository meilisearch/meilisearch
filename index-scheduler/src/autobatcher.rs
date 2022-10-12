use meilisearch_types::milli::update::IndexDocumentsMethod::{
    self, ReplaceDocuments, UpdateDocuments,
};
use meilisearch_types::tasks::{Kind, TaskId};
use std::ops::ControlFlow::{self, Break, Continue};

use crate::KindWithContent;

/// This enum contain the minimal necessary informations
/// to make the autobatcher works.
enum AutobatchKind {
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
    DeleteTasks,
    DumpExport,
    Snapshot,
}

impl From<KindWithContent> for AutobatchKind {
    fn from(kind: KindWithContent) -> Self {
        match kind {
            KindWithContent::DocumentImport {
                method,
                allow_index_creation,
                ..
            } => AutobatchKind::DocumentImport {
                method,
                allow_index_creation,
            },
            KindWithContent::DocumentDeletion { .. } => AutobatchKind::DocumentDeletion,
            KindWithContent::DocumentClear { .. } => AutobatchKind::DocumentClear,
            KindWithContent::Settings {
                allow_index_creation,
                ..
            } => AutobatchKind::Settings {
                allow_index_creation,
            },
            KindWithContent::IndexDeletion { .. } => AutobatchKind::IndexDeletion,
            KindWithContent::IndexCreation { .. } => AutobatchKind::IndexCreation,
            KindWithContent::IndexUpdate { .. } => AutobatchKind::IndexUpdate,
            KindWithContent::IndexSwap { lhs, rhs } => AutobatchKind::IndexSwap,
            KindWithContent::CancelTask { .. } => AutobatchKind::CancelTask,
            KindWithContent::DeleteTasks { .. } => AutobatchKind::DeleteTasks,
            KindWithContent::DumpExport { .. } => AutobatchKind::DumpExport,
            KindWithContent::Snapshot => AutobatchKind::Snapshot,
        }
    }
}

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
    pub fn new(task_id: TaskId, kind: KindWithContent) -> ControlFlow<BatchKind, BatchKind> {
        use AutobatchKind as K;

        match AutobatchKind::from(kind) {
            K::IndexCreation => Break(BatchKind::IndexCreation { id: task_id }),
            K::IndexDeletion => Break(BatchKind::IndexDeletion { ids: vec![task_id] }),
            K::IndexUpdate => Break(BatchKind::IndexUpdate { id: task_id }),
            K::IndexSwap => Break(BatchKind::IndexSwap { id: task_id }),
            K::DocumentClear => Continue(BatchKind::DocumentClear { ids: vec![task_id] }),
            K::DocumentImport {
                method,
                allow_index_creation,
            } => Continue(BatchKind::DocumentImport {
                method,
                allow_index_creation,
                import_ids: vec![task_id],
            }),
            K::DocumentDeletion => Continue(BatchKind::DocumentDeletion {
                deletion_ids: vec![task_id],
            }),
            K::Settings {
                allow_index_creation,
            } => Continue(BatchKind::Settings {
                allow_index_creation,
                settings_ids: vec![task_id],
            }),
            K::DumpExport | K::Snapshot | K::CancelTask | K::DeleteTasks => {
                unreachable!()
            }
        }
    }

    /// Returns a `ControlFlow::Break` if you must stop right now.
    #[rustfmt::skip]
    fn accumulate(self, id: TaskId, kind: AutobatchKind) -> ControlFlow<BatchKind, BatchKind> {
        use AutobatchKind as K;

        match (self, kind) {
            // We don't batch any of these operations
            (this, K::IndexCreation | K::IndexUpdate | K::IndexSwap) => Break(this),
            // The index deletion can batch with everything but must stop after
            (
                BatchKind::DocumentClear { mut ids }
                | BatchKind::DocumentDeletion { deletion_ids: mut ids }
                | BatchKind::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids }
                | BatchKind::Settings { allow_index_creation: _, settings_ids: mut ids },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                Break(BatchKind::IndexDeletion { ids })
            }
            (
                BatchKind::ClearAndSettings { settings_ids: mut ids, allow_index_creation: _, mut other }
                | BatchKind::SettingsAndDocumentImport { import_ids: mut ids, method: _, allow_index_creation: _, settings_ids: mut other },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                ids.append(&mut other);
                Break(BatchKind::IndexDeletion { ids })
            }

            (
                BatchKind::DocumentClear { mut ids },
                K::DocumentClear | K::DocumentDeletion,
            ) => {
                ids.push(id);
                Continue(BatchKind::DocumentClear { ids })
            }
            (
                this @ BatchKind::DocumentClear { .. },
                K::DocumentImport { .. } | K::Settings { .. },
            ) => Break(this),
            (
                BatchKind::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids },
                K::DocumentClear,
            ) => {
                ids.push(id);
                Continue(BatchKind::DocumentClear { ids })
            }

            // We only want to batch together document imports that are allowed to create the index
            // or document imports not allowed to create an index if the first operation can.
            (
                this @ BatchKind::DocumentImport { method: _, allow_index_creation: false, .. },
                K::DocumentImport { method: _, allow_index_creation: true },
            ) => Break(this),

            // we can autobatch the same kind of document additions / updates
            (
                BatchKind::DocumentImport { method: ReplaceDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: ReplaceDocuments, .. },
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
                K::DocumentImport { method: UpdateDocuments, .. },
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
                K::DocumentDeletion | K::DocumentImport { .. },
            ) => Break(this),

            // We only want to batch together document imports that are allowed to create the index
            // or document imports not allowed to create an index if the first operation can.
            (
                this @ BatchKind::DocumentImport { allow_index_creation: false, .. },
                K::Settings { allow_index_creation: true },
            ) => Break(this),
            (
                BatchKind::DocumentImport { method, allow_index_creation, import_ids },
                K::Settings { .. },
            ) => Continue(BatchKind::SettingsAndDocumentImport {
                settings_ids: vec![id],
                method,
                allow_index_creation,
                import_ids,
            }),

            (BatchKind::DocumentDeletion { mut deletion_ids }, K::DocumentClear) => {
                deletion_ids.push(id);
                Continue(BatchKind::DocumentClear { ids: deletion_ids })
            }
            (this @ BatchKind::DocumentDeletion { .. }, K::DocumentImport { .. }) => Break(this),
            (BatchKind::DocumentDeletion { mut deletion_ids }, K::DocumentDeletion) => {
                deletion_ids.push(id);
                Continue(BatchKind::DocumentDeletion { deletion_ids })
            }
            (this @ BatchKind::DocumentDeletion { .. }, K::Settings { .. }) => Break(this),

            (
                BatchKind::Settings { settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => Continue(BatchKind::ClearAndSettings {
                settings_ids: settings_ids,
                allow_index_creation,
                other: vec![id],
            }),
            (
                this @ BatchKind::Settings { .. },
                K::DocumentImport { .. } | K::DocumentDeletion,
            ) => Break(this),
            (
                this @ BatchKind::Settings { allow_index_creation: false, .. },
                K::Settings { allow_index_creation: true },
            ) => Break(this),
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
            (this @ BatchKind::ClearAndSettings { .. }, K::DocumentImport { .. }) => Break(this),
            (
                BatchKind::ClearAndSettings {
                    mut other,
                    settings_ids,
                    allow_index_creation,
                },
                K::DocumentDeletion,
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
                K::Settings {
                    allow_index_creation: true,
                },
            ) => Break(this),
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
                BatchKind::SettingsAndDocumentImport { settings_ids, method: _, import_ids: mut other, allow_index_creation },
                K::DocumentClear,
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
                K::DocumentImport { allow_index_creation: true, .. },
            ) => Break(this),
            (
                BatchKind::SettingsAndDocumentImport { settings_ids, method: ReplaceDocuments, mut import_ids, allow_index_creation },
                K::DocumentImport { method: ReplaceDocuments, .. },
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
                K::DocumentImport { method: UpdateDocuments, .. },
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
                K::DocumentDeletion | K::DocumentImport { .. },
            ) => Break(this),
            (
                this @ BatchKind::SettingsAndDocumentImport { allow_index_creation: false, .. },
                K::Settings { allow_index_creation: true },
            ) => Break(this),
            (
                BatchKind::SettingsAndDocumentImport { mut settings_ids, method, allow_index_creation, import_ids },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method,
                    allow_index_creation,
                    import_ids,
                })
            }
            (_, K::CancelTask | K::DeleteTasks | K::DumpExport | K::Snapshot) => {
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

pub fn autobatch(enqueued: Vec<(TaskId, KindWithContent)>) -> Option<BatchKind> {
    let mut enqueued = enqueued.into_iter();
    let (id, kind) = enqueued.next()?;
    let mut acc = match BatchKind::new(id, kind) {
        Continue(acc) => acc,
        Break(acc) => return Some(acc),
    };

    for (id, kind) in enqueued {
        acc = match acc.accumulate(id, kind.into()) {
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
    use uuid::Uuid;

    fn autobatch_from(input: impl IntoIterator<Item = KindWithContent>) -> Option<BatchKind> {
        autobatch(
            input
                .into_iter()
                .enumerate()
                .map(|(id, kind)| (id as TaskId, kind.into()))
                .collect(),
        )
    }

    fn doc_imp(method: IndexDocumentsMethod, allow_index_creation: bool) -> KindWithContent {
        KindWithContent::DocumentImport {
            index_uid: String::from("doggo"),
            primary_key: None,
            method,
            content_file: Uuid::new_v4(),
            documents_count: 0,
            allow_index_creation,
        }
    }

    fn doc_del() -> KindWithContent {
        KindWithContent::DocumentDeletion {
            index_uid: String::from("doggo"),
            documents_ids: Vec::new(),
        }
    }

    fn doc_clr() -> KindWithContent {
        KindWithContent::DocumentClear {
            index_uid: String::from("doggo"),
        }
    }

    fn settings(allow_index_creation: bool) -> KindWithContent {
        KindWithContent::Settings {
            index_uid: String::from("doggo"),
            new_settings: Default::default(),
            is_deletion: false,
            allow_index_creation,
        }
    }

    fn idx_create() -> KindWithContent {
        KindWithContent::IndexCreation {
            index_uid: String::from("doggo"),
            primary_key: None,
        }
    }

    fn idx_update() -> KindWithContent {
        KindWithContent::IndexUpdate {
            index_uid: String::from("doggo"),
            primary_key: None,
        }
    }

    fn idx_del() -> KindWithContent {
        KindWithContent::IndexDeletion {
            index_uid: String::from("doggo"),
        }
    }

    fn idx_swap() -> KindWithContent {
        KindWithContent::IndexSwap {
            lhs: String::from("doggo"),
            rhs: String::from("catto"),
        }
    }

    #[test]
    fn autobatch_simple_operation_together() {
        // we can autobatch one or multiple DocumentAddition together
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true)]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp( ReplaceDocuments, true ), doc_imp(ReplaceDocuments, true )]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1, 2] })");
        // we can autobatch one or multiple DocumentUpdate together
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true)]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true)]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0, 1, 2] })");
        // we can autobatch one or multiple DocumentDeletion together
        assert_smol_debug_snapshot!(autobatch_from([doc_del()]), @"Some(DocumentDeletion { deletion_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_del(), doc_del(), doc_del()]), @"Some(DocumentDeletion { deletion_ids: [0, 1, 2] })");
        // we can autobatch one or multiple Settings together
        assert_smol_debug_snapshot!(autobatch_from([settings(true)]), @"Some(Settings { allow_index_creation: true, settings_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([settings(true), settings(true), settings(true)]), @"Some(Settings { allow_index_creation: true, settings_ids: [0, 1, 2] })");
    }

    #[test]
    fn simple_document_operation_dont_autobatch_with_other() {
        // addition, updates and deletion can't batch together
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(UpdateDocuments, true)]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_del()]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(ReplaceDocuments, true)]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_del()]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_del(), doc_imp(ReplaceDocuments, true)]), @"Some(DocumentDeletion { deletion_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_del(), doc_imp(UpdateDocuments, true)]), @"Some(DocumentDeletion { deletion_ids: [0] })");

        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), idx_create()]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), idx_create()]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_del(), idx_create()]), @"Some(DocumentDeletion { deletion_ids: [0] })");

        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), idx_update()]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), idx_update()]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_del(), idx_update()]), @"Some(DocumentDeletion { deletion_ids: [0] })");

        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), idx_swap()]), @"Some(DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), idx_swap()]), @"Some(DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_del(), idx_swap()]), @"Some(DocumentDeletion { deletion_ids: [0] })");
    }

    #[test]
    fn document_addition_batch_with_settings() {
        // simple case
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true)]), @"Some(settingsAnddoc_im(Repl)eDocuments)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true)]), @"Some(settingsAnddoc_im(Upda)Documents)allow_index_creation: true, import_ids: [0] })");

        // multiple settings and doc addition
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), settings(true), settings(true)]), @"Some(settingsAnddoc_im(Repl)eDocuments)allow_index_creation: true, import_ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), settings(true), settings(true)]), @"Some(settingsAnddoc_im(Repl)eDocuments)allow_index_creation: true, import_ids: [0, 1] })");

        // addition and setting unordered
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_imp(ReplaceDocuments, true), settings(true)]), @"Some(settingsAnddoc_im(Repl)eDocuments)allow_index_creation: true, import_ids: [0, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_imp(UpdateDocuments, true), settings(true)]), @"Some(settingsAnddoc_im(Upda)Documents)allow_index_creation: true, import_ids: [0, 2] })");

        // We ensure this kind of batch doesn't batch with forbidden operations
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_imp(UpdateDocuments, true)]), @"Some(settingsAnddoc_im(Repl)eDocuments)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_imp(ReplaceDocuments, true)]), @"Some(settingsAnddoc_im(Upda)Documents)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_del()]), @"Some(settingsAnddoc_im(Repl)eDocuments)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_del()]), @"Some(settingsAnddoc_im(Upda)Documents)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), idx_create()]), @"Some(settingsAnddoc_im(Repl)eDocuments)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), idx_create()]), @"Some(settingsAnddoc_im(Upda)Documents)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), idx_update()]), @"Some(settingsAnddoc_im(Repl)eDocuments)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), idx_update()]), @"Some(settingsAnddoc_im(Upda)Documents)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), idx_swap()]), @"Some(settingsAnddoc_im(Repl)eDocuments)allow_index_creation: true, import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), idx_swap()]), @"Some(settingsAnddoc_im(Upda)Documents)allow_index_creation: true, import_ids: [0] })");
    }

    #[test]
    fn clear_and_additions() {
        // these two doesn't need to batch
        assert_smol_debug_snapshot!(autobatch_from([doc_clr(), doc_imp(ReplaceDocuments, true)]), @"Some(doc_clr() { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_clr(), doc_imp(UpdateDocuments, true)]), @"Some(doc_clr() { ids: [0] })");

        // Basic use case
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), doc_clr()]), @"Some(doc_clr() { ids: [0, 1, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_clr()]), @"Some(doc_clr() { ids: [0, 1, 2] })");

        // This batch kind doesn't mix with other document addition
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), doc_clr(), doc_imp(ReplaceDocuments, true)]), @"Some(doc_clr() { ids: [0, 1, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_clr(), doc_imp(UpdateDocuments, true)]), @"Some(doc_clr() { ids: [0, 1, 2] })");

        // But you can batch multiple clear together
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), doc_clr(), doc_clr(), doc_clr()]), @"Some(doc_clr() { ids: [0, 1, 2, 3, 4] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_clr(), doc_clr(), doc_clr()]), @"Some(doc_clr() { ids: [0, 1, 2, 3, 4] })");
    }

    #[test]
    fn clear_and_additions_and_settings() {
        // A clear don't need to autobatch the settings that happens AFTER there is no documents
        assert_smol_debug_snapshot!(autobatch_from([doc_clr(), settings(true)]), @"Some(doc_clr() { ids: [0] })");

        assert_smol_debug_snapshot!(autobatch_from([settings(true), doc_clr(), settings(true)]), @"Some(clearAndSettings([1) allow_index_creation: true, settings_ids: [0, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_clr()]), @"Some(clearAndSettings([0)2], allow_index_creation: true, settings_ids: [1] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_clr()]), @"Some(clearAndSettings([0)2], allow_index_creation: true, settings_ids: [1] })");
    }

    #[test]
    fn anything_and_index_deletion() {
        // The indexdeletion doesn't batch with anything that happens AFTER
        assert_smol_debug_snapshot!(autobatch_from([idx_del(), doc_imp(ReplaceDocuments, true)]), @"Some(idx_del() { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([idx_del(), doc_imp(UpdateDocuments, true)]), @"Some(idx_del() { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([idx_del(), doc_del()]), @"Some(idx_del() { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([idx_del(), doc_clr()]), @"Some(idx_del() { ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([idx_del(), settings(true)]), @"Some(idx_del() { ids: [0] })");

        // The index deletion can accept almost any type of BatchKind and transform it to an idx_del()
        // First, the basic cases
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), idx_del()]), @"Some(idx_del() { ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), idx_del()]), @"Some(idx_del() { ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_del(), idx_del()]), @"Some(idx_del() { ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_clr(), idx_del()]), @"Some(idx_del() { ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([settings(true), idx_del()]), @"Some(idx_del() { ids: [0, 1] })");

        // Then the mixed cases
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), idx_del()]), @"Some(idx_del() { ids: [0, 2, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), idx_del()]), @"Some(idx_del() { ids: [0, 2, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some(idx_del() { ids: [1, 3, 0, 2] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(UpdateDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some(idx_del() { ids: [1, 3, 0, 2] })");
    }

    #[test]
    fn allowed_and_disallowed_index_creation() {
        // doc_imp(indexes canbe)ixed with those disallowed to do so
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, true)]), @"Some(doc_imp(ReplaceDocuments, false)import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true)]), @"Some(doc_imp(ReplaceDocuments, true)import_ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, false)]), @"Some(doc_imp(ReplaceDocuments, false)import_ids: [0, 1] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, true), settings(true)]), @"Some(settingsAnddoc_imp(: ReplaceDocuments: true)import_ids: [0] })");
        assert_smol_debug_snapshot!(autobatch_from([doc_imp(ReplaceDocuments, false), settings(true)]), @"Some(doc_imp(ReplaceDocuments, false)import_ids: [0] })");
    }
}

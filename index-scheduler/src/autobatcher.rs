/*!
The autobatcher is responsible for combining the next enqueued
tasks affecting a single index into a [batch](crate::batch::Batch).

The main function of the autobatcher is [`next_autobatch`].
*/

use std::ops::ControlFlow::{self, Break, Continue};

use meilisearch_types::milli::update::IndexDocumentsMethod::{
    self, ReplaceDocuments, UpdateDocuments,
};
use meilisearch_types::tasks::TaskId;

use crate::KindWithContent;

/// Succinctly describes a task's [`Kind`](meilisearch_types::tasks::Kind)
/// for the purpose of simplifying the implementation of the autobatcher.
///
/// Only the non-prioritised tasks that can be grouped in a batch have a corresponding [`AutobatchKind`]
enum AutobatchKind {
    DocumentImport { method: IndexDocumentsMethod, allow_index_creation: bool },
    DocumentDeletion,
    DocumentClear,
    Settings { allow_index_creation: bool },
    IndexCreation,
    IndexDeletion,
    IndexUpdate,
    IndexSwap,
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
}

impl From<KindWithContent> for AutobatchKind {
    fn from(kind: KindWithContent) -> Self {
        match kind {
            KindWithContent::DocumentAdditionOrUpdate { method, allow_index_creation, .. } => {
                AutobatchKind::DocumentImport { method, allow_index_creation }
            }
            KindWithContent::DocumentDeletion { .. } => AutobatchKind::DocumentDeletion,
            KindWithContent::DocumentClear { .. } => AutobatchKind::DocumentClear,
            KindWithContent::SettingsUpdate { allow_index_creation, is_deletion, .. } => {
                AutobatchKind::Settings {
                    allow_index_creation: allow_index_creation && !is_deletion,
                }
            }
            KindWithContent::IndexDeletion { .. } => AutobatchKind::IndexDeletion,
            KindWithContent::IndexCreation { .. } => AutobatchKind::IndexCreation,
            KindWithContent::IndexUpdate { .. } => AutobatchKind::IndexUpdate,
            KindWithContent::IndexSwap { .. } => AutobatchKind::IndexSwap,
            KindWithContent::TaskCancelation { .. }
            | KindWithContent::TaskDeletion { .. }
            | KindWithContent::DumpCreation { .. }
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
    #[rustfmt::skip]
    fn allow_index_creation(&self) -> Option<bool> {
        match self {
            BatchKind::DocumentImport { allow_index_creation, .. }
            | BatchKind::ClearAndSettings { allow_index_creation, .. }
            | BatchKind::SettingsAndDocumentImport { allow_index_creation, .. }
            | BatchKind::Settings { allow_index_creation, .. } => Some(*allow_index_creation),
            _ => None,
        }
    }
}

impl BatchKind {
    /// Returns a `ControlFlow::Break` if you must stop right now.
    /// The boolean tell you if an index has been created by the batched task.
    /// To ease the writting of the code. `true` can be returned when you don't need to create an index
    /// but false can't be returned if you needs to create an index.
    // TODO use an AutoBatchKind as input
    pub fn new(
        task_id: TaskId,
        kind: KindWithContent,
    ) -> (ControlFlow<BatchKind, BatchKind>, bool) {
        use AutobatchKind as K;

        match AutobatchKind::from(kind) {
            K::IndexCreation => (Break(BatchKind::IndexCreation { id: task_id }), true),
            K::IndexDeletion => (Break(BatchKind::IndexDeletion { ids: vec![task_id] }), false),
            K::IndexUpdate => (Break(BatchKind::IndexUpdate { id: task_id }), false),
            K::IndexSwap => (Break(BatchKind::IndexSwap { id: task_id }), false),
            K::DocumentClear => (Continue(BatchKind::DocumentClear { ids: vec![task_id] }), false),
            K::DocumentImport { method, allow_index_creation } => (
                Continue(BatchKind::DocumentImport {
                    method,
                    allow_index_creation,
                    import_ids: vec![task_id],
                }),
                allow_index_creation,
            ),
            K::DocumentDeletion => {
                (Continue(BatchKind::DocumentDeletion { deletion_ids: vec![task_id] }), false)
            }
            K::Settings { allow_index_creation } => (
                Continue(BatchKind::Settings { allow_index_creation, settings_ids: vec![task_id] }),
                allow_index_creation,
            ),
        }
    }

    /// Returns a `ControlFlow::Break` if you must stop right now.
    /// The boolean tell you if an index has been created by the batched task.
    /// To ease the writting of the code. `true` can be returned when you don't need to create an index
    /// but false can't be returned if you needs to create an index.
    #[rustfmt::skip]
    fn accumulate(self, id: TaskId, kind: AutobatchKind, index_already_exists: bool) -> ControlFlow<BatchKind, BatchKind> {
        use AutobatchKind as K;

        match (self, kind) {
            // We don't batch any of these operations
            (this, K::IndexCreation | K::IndexUpdate | K::IndexSwap) => Break(this),
            // We must not batch tasks that don't have the same index creation rights if the index doesn't already exists.
            (this, kind) if !index_already_exists && this.allow_index_creation() == Some(false) && kind.allow_index_creation() == Some(true) => {
                Break(this)
            },
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
                settings_ids,
                allow_index_creation,
                other: vec![id],
            }),
            (
                this @ BatchKind::Settings { .. },
                K::DocumentImport { .. } | K::DocumentDeletion,
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
) -> Option<(BatchKind, bool)> {
    let mut enqueued = enqueued.into_iter();
    let (id, kind) = enqueued.next()?;

    // index_exist will keep track of if the index should exist at this point after the tasks we batched.
    let mut index_exist = index_already_exists;

    let (mut acc, must_create_index) = match BatchKind::new(id, kind) {
        (Continue(acc), create) => (acc, create),
        (Break(acc), create) => return Some((acc, create)),
    };

    // if an index has been created in the previous step we can consider it as existing.
    index_exist |= must_create_index;

    for (id, kind) in enqueued {
        acc = match acc.accumulate(id, kind.into(), index_exist) {
            Continue(acc) => acc,
            Break(acc) => return Some((acc, must_create_index)),
        };
    }

    Some((acc, must_create_index))
}

#[cfg(test)]
mod tests {
    use meilisearch_types::tasks::IndexSwap;
    use uuid::Uuid;

    use super::*;
    use crate::debug_snapshot;

    fn autobatch_from(
        index_already_exists: bool,
        input: impl IntoIterator<Item = KindWithContent>,
    ) -> Option<(BatchKind, bool)> {
        autobatch(
            input.into_iter().enumerate().map(|(id, kind)| (id as TaskId, kind)).collect(),
            index_already_exists,
        )
    }

    fn doc_imp(method: IndexDocumentsMethod, allow_index_creation: bool) -> KindWithContent {
        KindWithContent::DocumentAdditionOrUpdate {
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
        KindWithContent::DocumentClear { index_uid: String::from("doggo") }
    }

    fn settings(allow_index_creation: bool) -> KindWithContent {
        KindWithContent::SettingsUpdate {
            index_uid: String::from("doggo"),
            new_settings: Default::default(),
            is_deletion: false,
            allow_index_creation,
        }
    }

    fn idx_create() -> KindWithContent {
        KindWithContent::IndexCreation { index_uid: String::from("doggo"), primary_key: None }
    }

    fn idx_update() -> KindWithContent {
        KindWithContent::IndexUpdate { index_uid: String::from("doggo"), primary_key: None }
    }

    fn idx_del() -> KindWithContent {
        KindWithContent::IndexDeletion { index_uid: String::from("doggo") }
    }

    fn idx_swap() -> KindWithContent {
        KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: (String::from("doggo"), String::from("catto")) }],
        }
    }

    #[test]
    fn autobatch_simple_operation_together() {
        // we can autobatch one or multiple `ReplaceDocuments` together.
        // if the index exists.
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, false)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp( ReplaceDocuments, true ), doc_imp(ReplaceDocuments, true )]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, false), doc_imp( ReplaceDocuments, false ), doc_imp(ReplaceDocuments, false )]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0, 1, 2] }, false))");

        // if it doesn't exists.
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, false)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, true), doc_imp( ReplaceDocuments, true ), doc_imp(ReplaceDocuments, true )]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1, 2] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, false), doc_imp( ReplaceDocuments, true ), doc_imp(ReplaceDocuments, true )]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, false))");

        // we can autobatch one or multiple `UpdateDocuments` together.
        // if the index exists.
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0, 1, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, false)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: false, import_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, false), doc_imp(UpdateDocuments, false), doc_imp(UpdateDocuments, false)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: false, import_ids: [0, 1, 2] }, false))");

        // if it doesn't exists.
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, true)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0, 1, 2] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, false)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: false, import_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, false), doc_imp(UpdateDocuments, false), doc_imp(UpdateDocuments, false)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: false, import_ids: [0, 1, 2] }, false))");

        // we can autobatch one or multiple DocumentDeletion together
        debug_snapshot!(autobatch_from(true, [doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_del(), doc_del(), doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0, 1, 2] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_del(), doc_del(), doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0, 1, 2] }, false))");

        // we can autobatch one or multiple Settings together
        debug_snapshot!(autobatch_from(true, [settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [settings(true), settings(true), settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0, 1, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [settings(false)]), @"Some((Settings { allow_index_creation: false, settings_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [settings(false), settings(false), settings(false)]), @"Some((Settings { allow_index_creation: false, settings_ids: [0, 1, 2] }, false))");

        debug_snapshot!(autobatch_from(false, [settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(false, [settings(true), settings(true), settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0, 1, 2] }, true))");
        debug_snapshot!(autobatch_from(false, [settings(false)]), @"Some((Settings { allow_index_creation: false, settings_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [settings(false), settings(false), settings(false)]), @"Some((Settings { allow_index_creation: false, settings_ids: [0, 1, 2] }, false))");
    }

    #[test]
    fn simple_document_operation_dont_autobatch_with_other() {
        // addition, updates and deletion can't batch together
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp(UpdateDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_del()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), doc_del()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentDeletion { deletion_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_del(), doc_imp(UpdateDocuments, true)]), @"Some((DocumentDeletion { deletion_ids: [0] }, false))");

        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), idx_create()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), idx_create()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), idx_create()]), @"Some((DocumentDeletion { deletion_ids: [0] }, false))");

        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), idx_update()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), idx_update()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), idx_update()]), @"Some((DocumentDeletion { deletion_ids: [0] }, false))");

        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), idx_swap()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), idx_swap()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), idx_swap()]), @"Some((DocumentDeletion { deletion_ids: [0] }, false))");
    }

    #[test]
    fn document_addition_batch_with_settings() {
        // simple case
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");

        // multiple settings and doc addition
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), settings(true), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [2, 3], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), settings(true), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [2, 3], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] }, true))");

        // addition and setting unordered
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), doc_imp(ReplaceDocuments, true), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1, 3], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), doc_imp(UpdateDocuments, true), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1, 3], method: UpdateDocuments, allow_index_creation: true, import_ids: [0, 2] }, true))");

        // We ensure this kind of batch doesn't batch with forbidden operations
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), doc_imp(UpdateDocuments, true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), doc_imp(ReplaceDocuments, true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), doc_del()]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), doc_del()]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), idx_create()]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), idx_create()]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), idx_update()]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), idx_update()]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), idx_swap()]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), idx_swap()]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
    }

    #[test]
    fn clear_and_additions() {
        // these two doesn't need to batch
        debug_snapshot!(autobatch_from(true, [doc_clr(), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentClear { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_clr(), doc_imp(UpdateDocuments, true)]), @"Some((DocumentClear { ids: [0] }, false))");

        // Basic use case
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), doc_clr()]), @"Some((DocumentClear { ids: [0, 1, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_clr()]), @"Some((DocumentClear { ids: [0, 1, 2] }, true))");

        // This batch kind doesn't mix with other document addition
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), doc_clr(), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentClear { ids: [0, 1, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_clr(), doc_imp(UpdateDocuments, true)]), @"Some((DocumentClear { ids: [0, 1, 2] }, true))");

        // But you can batch multiple clear together
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true), doc_clr(), doc_clr(), doc_clr()]), @"Some((DocumentClear { ids: [0, 1, 2, 3, 4] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_clr(), doc_clr(), doc_clr()]), @"Some((DocumentClear { ids: [0, 1, 2, 3, 4] }, true))");
    }

    #[test]
    fn clear_and_additions_and_settings() {
        // A clear don't need to autobatch the settings that happens AFTER there is no documents
        debug_snapshot!(autobatch_from(true, [doc_clr(), settings(true)]), @"Some((DocumentClear { ids: [0] }, false))");

        debug_snapshot!(autobatch_from(true, [settings(true), doc_clr(), settings(true)]), @"Some((ClearAndSettings { other: [1], allow_index_creation: true, settings_ids: [0, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), doc_clr()]), @"Some((ClearAndSettings { other: [0, 2], allow_index_creation: true, settings_ids: [1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), doc_clr()]), @"Some((ClearAndSettings { other: [0, 2], allow_index_creation: true, settings_ids: [1] }, true))");
    }

    #[test]
    fn anything_and_index_deletion() {
        // The `IndexDeletion` doesn't batch with anything that happens AFTER.
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_imp(ReplaceDocuments, true)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_imp(UpdateDocuments, true)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_imp(ReplaceDocuments, false)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_imp(UpdateDocuments, false)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_del()]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_clr()]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), settings(true)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), settings(false)]), @"Some((IndexDeletion { ids: [0] }, false))");

        debug_snapshot!(autobatch_from(false, [idx_del(), doc_imp(ReplaceDocuments, true)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [idx_del(), doc_imp(UpdateDocuments, true)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [idx_del(), doc_imp(ReplaceDocuments, false)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [idx_del(), doc_imp(UpdateDocuments, false)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [idx_del(), doc_del()]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [idx_del(), doc_clr()]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [idx_del(), settings(true)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [idx_del(), settings(false)]), @"Some((IndexDeletion { ids: [0] }, false))");

        // The index deletion can accept almost any type of `BatchKind` and transform it to an `IndexDeletion`.
        // First, the basic cases
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, false), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, false), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_del(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");

        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, false), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, false), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_del(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(false, [settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(false, [settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");

        // Then the mixed cases.
        // The index already exists, whatever is the right of the tasks it shouldn't change the result.
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments,false), settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, false), settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments,false), settings(false), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, false), settings(false), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments,false), settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, false), settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments,false), settings(true), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, false), settings(true), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments,true), settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments,true), settings(false), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(false), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");

        // When the index doesn't exists yet it's more complicated.
        // Either the first task we encounter create it, in which case we can create a big batch with everything.
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, true), settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, true), settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");
        // The right of the tasks following isn't really important.
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments,true), settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, true), settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments,true), settings(false), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, true), settings(false), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");
        // Or, the second case; the first task doesn't create the index and thus we wants to batch it with only tasks that can't create an index.
        // that can be a second task that don't have the right to create an index. Or anything that can't create an index like an index deletion, document deletion, document clear, etc.
        // All theses tasks are going to throw an error `Index doesn't exist` once the batch is processed.
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments,false), settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, false), settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments,false), settings(false), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, false), settings(false), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, false))");
        // The third and final case is when the first task doesn't create an index but is directly followed by a task creating an index. In this case we can't batch whith what
        // follows because we first need to process the erronous batch.
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments,false), settings(true), idx_del()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, false), settings(true), idx_del()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: false, import_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments,false), settings(true), doc_clr(), idx_del()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(UpdateDocuments, false), settings(true), doc_clr(), idx_del()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: false, import_ids: [0] }, false))");
    }

    #[test]
    fn allowed_and_disallowed_index_creation() {
        // `DocumentImport` can't be mixed with those disallowed to do so except if the index already exists.
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, false)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, false), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, false))");

        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, false)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0, 1] }, false))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, true), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(false, [doc_imp(ReplaceDocuments, false), settings(true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, false))");
    }
}

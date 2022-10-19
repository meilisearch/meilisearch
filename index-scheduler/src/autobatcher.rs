/*!
The autobatcher is responsible for combining the next enqueued
tasks affecting a single index into a [batch](crate::batch::Batch).

The main function of the autobatcher is [`next_autobatch`].
*/

use meilisearch_types::milli::update::IndexDocumentsMethod::{
    self, ReplaceDocuments, UpdateDocuments,
};
use meilisearch_types::tasks::TaskId;
use std::ops::ControlFlow::{self, Break, Continue};

use crate::KindWithContent;

/// Succinctly describes a task's [`Kind`](meilisearch_types::tasks::Kind)
/// for the purpose of simplifying the implementation of the autobatcher.
///
/// Only the non-prioritised tasks that can be grouped in a batch have a corresponding [`AutobatchKind`]
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
                is_deletion,
                ..
            } => AutobatchKind::Settings {
                allow_index_creation: allow_index_creation && !is_deletion,
            },
            KindWithContent::IndexDeletion { .. } => AutobatchKind::IndexDeletion,
            KindWithContent::IndexCreation { .. } => AutobatchKind::IndexCreation,
            KindWithContent::IndexUpdate { .. } => AutobatchKind::IndexUpdate,
            KindWithContent::IndexSwap { .. } => AutobatchKind::IndexSwap,
            KindWithContent::TaskCancelation { .. }
            | KindWithContent::TaskDeletion { .. }
            | KindWithContent::DumpExport { .. }
            | KindWithContent::Snapshot => {
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
            K::IndexDeletion => (
                Break(BatchKind::IndexDeletion { ids: vec![task_id] }),
                false,
            ),
            K::IndexUpdate => (Break(BatchKind::IndexUpdate { id: task_id }), false),
            K::IndexSwap => (Break(BatchKind::IndexSwap { id: task_id }), false),
            K::DocumentClear => (
                Continue(BatchKind::DocumentClear { ids: vec![task_id] }),
                false,
            ),
            K::DocumentImport {
                method,
                allow_index_creation,
            } => (
                Continue(BatchKind::DocumentImport {
                    method,
                    allow_index_creation,
                    import_ids: vec![task_id],
                }),
                allow_index_creation,
            ),
            K::DocumentDeletion => (
                Continue(BatchKind::DocumentDeletion {
                    deletion_ids: vec![task_id],
                }),
                false,
            ),
            K::Settings {
                allow_index_creation,
            } => (
                Continue(BatchKind::Settings {
                    allow_index_creation,
                    settings_ids: vec![task_id],
                }),
                allow_index_creation,
            ),
        }
    }

    /// Returns a `ControlFlow::Break` if you must stop right now.
    /// The boolean tell you if an index has been created by the batched task.
    /// To ease the writting of the code. `true` can be returned when you don't need to create an index
    /// but false can't be returned if you needs to create an index.
    #[rustfmt::skip]
    fn accumulate(self, id: TaskId, kind: AutobatchKind, index_already_exists: bool) -> (ControlFlow<BatchKind, BatchKind>, bool) {
        use AutobatchKind as K;

        match (index_already_exists, self, kind) {
            // We don't batch any of these operations.
            (true, this, K::IndexCreation | K::IndexUpdate | K::IndexSwap) => (Break(this), true),
            // We must not batch tasks that don't have the same index creation rights.
            (true, this, kind) if this.allow_index_creation() == Some(false) && kind.allow_index_creation() == Some(true) => {
                (Break(this), true)
            },
            // The index deletion can batch with everything but must stop after
            (true,
                BatchKind::DocumentClear { mut ids }
                | BatchKind::DocumentDeletion { deletion_ids: mut ids }
                | BatchKind::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids }
                | BatchKind::Settings { allow_index_creation: _, settings_ids: mut ids },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                (Break(BatchKind::IndexDeletion { ids }), true)
            }
            (true,
                BatchKind::ClearAndSettings { settings_ids: mut ids, allow_index_creation: _, mut other }
                | BatchKind::SettingsAndDocumentImport { import_ids: mut ids, method: _, allow_index_creation: _, settings_ids: mut other },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                ids.append(&mut other);
                (Break(BatchKind::IndexDeletion { ids }), true)
            }

            (true,
                BatchKind::DocumentClear { mut ids },
                K::DocumentClear | K::DocumentDeletion,
            ) => {
                ids.push(id);
                (Continue(BatchKind::DocumentClear { ids }), true)
            }
            (true,
                this @ BatchKind::DocumentClear { .. },
                K::DocumentImport { .. } | K::Settings { .. },
            ) => (Break(this), true),
            (true,
                BatchKind::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids },
                K::DocumentClear,
            ) => {
                ids.push(id);
                (Continue(BatchKind::DocumentClear { ids }), true)
            }

            // we can autobatch the same kind of document additions / updates
            ( true,
                BatchKind::DocumentImport { method: ReplaceDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: ReplaceDocuments, .. },
            ) => {
                import_ids.push(id);
                (Continue(BatchKind::DocumentImport {
                    method: ReplaceDocuments,
                    allow_index_creation,
                    import_ids,
                }), true)
            }
            (true,
                BatchKind::DocumentImport { method: UpdateDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: UpdateDocuments, .. },
            ) => {
                import_ids.push(id);
                (Continue(BatchKind::DocumentImport {
                    method: UpdateDocuments,
                    allow_index_creation,
                    import_ids,
                }), true)
            }

            // but we can't autobatch documents if it's not the same kind
            // this match branch MUST be AFTER the previous one
            (true, 
                this @ BatchKind::DocumentImport { .. },
                K::DocumentDeletion | K::DocumentImport { .. },
            ) => (Break(this), true),

            (true,
                BatchKind::DocumentImport { method, allow_index_creation, import_ids },
                K::Settings { .. },
            ) => (Continue(BatchKind::SettingsAndDocumentImport {
                settings_ids: vec![id],
                method,
                allow_index_creation,
                import_ids,
            }), true),

            (true, BatchKind::DocumentDeletion { mut deletion_ids }, K::DocumentClear) => {
                deletion_ids.push(id);
                (Continue(BatchKind::DocumentClear { ids: deletion_ids }), true)
            }
            (true, this @ BatchKind::DocumentDeletion { .. }, K::DocumentImport { .. }) => (Break(this), true),
            (true, BatchKind::DocumentDeletion { mut deletion_ids }, K::DocumentDeletion) => {
                deletion_ids.push(id);
                (Continue(BatchKind::DocumentDeletion { deletion_ids }), true)
            }
            (true, this @ BatchKind::DocumentDeletion { .. }, K::Settings { .. }) => (Break(this), true),

            (true,
                BatchKind::Settings { settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => (Continue(BatchKind::ClearAndSettings {
                settings_ids,
                allow_index_creation,
                other: vec![id],
            }), true),
            (true,
                this @ BatchKind::Settings { .. },
                K::DocumentImport { .. } | K::DocumentDeletion,
            ) => (Break(this), true),
            (true,
                BatchKind::Settings { mut settings_ids, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                (Continue(BatchKind::Settings {
                    allow_index_creation,
                    settings_ids,
                }), true)
            }

            (true,
                BatchKind::ClearAndSettings { mut other, settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => {
                other.push(id);
                (Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                }), true)
            }
            (true, this @ BatchKind::ClearAndSettings { .. }, K::DocumentImport { .. }) => (Break(this), true),
            (true, 
                BatchKind::ClearAndSettings {
                    mut other,
                    settings_ids,
                    allow_index_creation,
                },
                K::DocumentDeletion,
            ) => {
                other.push(id);
                (Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                }), true)
            }
            (true,
                BatchKind::ClearAndSettings { mut settings_ids, other, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                (Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                }), true)
            }
            (true,
                BatchKind::SettingsAndDocumentImport { settings_ids, method: _, import_ids: mut other, allow_index_creation },
                K::DocumentClear,
            ) => {
                other.push(id);
                (Continue(BatchKind::ClearAndSettings {
                    settings_ids,
                    other,
                    allow_index_creation,
                }), true)
            }

            (true,
                BatchKind::SettingsAndDocumentImport { settings_ids, method: ReplaceDocuments, mut import_ids, allow_index_creation },
                K::DocumentImport { method: ReplaceDocuments, .. },
            ) => {
                import_ids.push(id);
                (Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method: ReplaceDocuments,
                    allow_index_creation,
                    import_ids,
                }), true)
            }
            (true,
                BatchKind::SettingsAndDocumentImport { settings_ids, method: UpdateDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: UpdateDocuments, .. },
            ) => {
                import_ids.push(id);
                (Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method: UpdateDocuments,
                    allow_index_creation,
                    import_ids,
                }), true)
            }
            // But we can't batch a settings and a doc op with another doc op
            // this MUST be AFTER the two previous branch
            (true,
                this @ BatchKind::SettingsAndDocumentImport { .. },
                K::DocumentDeletion | K::DocumentImport { .. },
            ) => (Break(this), true),
            (true,
                BatchKind::SettingsAndDocumentImport { mut settings_ids, method, allow_index_creation, import_ids },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                (Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method,
                    allow_index_creation,
                    import_ids,
                }), true)
            }
            
            // TODO
            
            
            // We don't batch any of these operations.
            (false, this, K::IndexCreation) => (Break(this), true),
            (false, this, K::IndexUpdate | K::IndexSwap) => (Break(this), false),

            // We must not batch tasks that don't have the same index creation rights.
            (false, this, kind) if this.allow_index_creation() == Some(false) && kind.allow_index_creation() == Some(true) => {
                (Break(this), false)
            },
            // The index deletion can batch with everything but must stop after
            (false,
                BatchKind::DocumentClear { mut ids }
                | BatchKind::DocumentDeletion { deletion_ids: mut ids }
                | BatchKind::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids }
                | BatchKind::Settings { allow_index_creation: _, settings_ids: mut ids },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                (Break(BatchKind::IndexDeletion { ids }), false)
            }
            (false,
                BatchKind::ClearAndSettings { settings_ids: mut ids, allow_index_creation: _, mut other }
                | BatchKind::SettingsAndDocumentImport { import_ids: mut ids, method: _, allow_index_creation: _, settings_ids: mut other },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                ids.append(&mut other);
                (Break(BatchKind::IndexDeletion { ids }), false)
            }

            (false,
                BatchKind::DocumentClear { mut ids },
                K::DocumentClear | K::DocumentDeletion,
            ) => {
                ids.push(id);
                (Continue(BatchKind::DocumentClear { ids }), false)
            }
            (false,
                this @ BatchKind::DocumentClear { .. },
                K::DocumentImport { .. } | K::Settings { .. },
            ) => (Break(this), false),
            (false,
                BatchKind::DocumentImport { method: _, allow_index_creation: _, import_ids: mut ids },
                K::DocumentClear,
            ) => {
                ids.push(id);
                (Continue(BatchKind::DocumentClear { ids }), false)
            }

            // we can autobatch the same kind of document additions / updates
            (false,
                BatchKind::DocumentImport { method: ReplaceDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: ReplaceDocuments, .. },
            ) => {
                import_ids.push(id);
                (Continue(BatchKind::DocumentImport {
                    method: ReplaceDocuments,
                    allow_index_creation,
                    import_ids,
                }), false)
            }
            (false,
                BatchKind::DocumentImport { method: UpdateDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: UpdateDocuments, .. },
            ) => {
                import_ids.push(id);
                (Continue(BatchKind::DocumentImport {
                    method: UpdateDocuments,
                    allow_index_creation,
                    import_ids,
                }), false)
            }

            // but we can't autobatch documents if it's not the same kind
            // this match branch MUST be AFTER the previous one
            (false,
                this @ BatchKind::DocumentImport { .. },
                K::DocumentDeletion | K::DocumentImport { .. },
            ) => (Break(this), false),

            (false,
                BatchKind::DocumentImport { method, allow_index_creation, import_ids },
                K::Settings { .. },
            ) => (Continue(BatchKind::SettingsAndDocumentImport {
                settings_ids: vec![id],
                method,
                allow_index_creation,
                import_ids,
            }), false),

            (false, BatchKind::DocumentDeletion { mut deletion_ids }, K::DocumentClear) => {
                deletion_ids.push(id);
                (Continue(BatchKind::DocumentClear { ids: deletion_ids }), false)
            }
            (false, this @ BatchKind::DocumentDeletion { .. }, K::DocumentImport { .. }) => (Break(this), false),
            (false, BatchKind::DocumentDeletion { mut deletion_ids }, K::DocumentDeletion) => {
                deletion_ids.push(id);
                (Continue(BatchKind::DocumentDeletion { deletion_ids }), false)
            }
            (false, this @ BatchKind::DocumentDeletion { .. }, K::Settings { .. }) => (Break(this), false),

            (false,
                BatchKind::Settings { settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => (Continue(BatchKind::ClearAndSettings {
                settings_ids,
                allow_index_creation,
                other: vec![id],
            }), false),
            (false,
                this @ BatchKind::Settings { .. },
                K::DocumentImport { .. } | K::DocumentDeletion,
            ) => (Break(this), false),
            (false,
                BatchKind::Settings { mut settings_ids, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                (Continue(BatchKind::Settings {
                    allow_index_creation,
                    settings_ids,
                }), false)
            }

            (false,
                BatchKind::ClearAndSettings { mut other, settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => {
                other.push(id);
                (Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                }), false)
            }
            (false, this @ BatchKind::ClearAndSettings { .. }, K::DocumentImport { .. }) => (Break(this), false),
            (false,
                BatchKind::ClearAndSettings {
                    mut other,
                    settings_ids,
                    allow_index_creation,
                },
                K::DocumentDeletion,
            ) => {
                other.push(id);
                (Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                }), false)
            }
            (false,
                BatchKind::ClearAndSettings { mut settings_ids, other, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                (Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                }), false)
            }
            (false,
                BatchKind::SettingsAndDocumentImport { settings_ids, method: _, import_ids: mut other, allow_index_creation },
                K::DocumentClear,
            ) => {
                other.push(id);
                (Continue(BatchKind::ClearAndSettings {
                    settings_ids,
                    other,
                    allow_index_creation,
                }), false)
            }

            (false,
                BatchKind::SettingsAndDocumentImport { settings_ids, method: ReplaceDocuments, mut import_ids, allow_index_creation },
                K::DocumentImport { method: ReplaceDocuments, .. },
            ) => {
                import_ids.push(id);
                (Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method: ReplaceDocuments,
                    allow_index_creation,
                    import_ids,
                }), false)
            }
            (false,
                BatchKind::SettingsAndDocumentImport { settings_ids, method: UpdateDocuments, allow_index_creation, mut import_ids },
                K::DocumentImport { method: UpdateDocuments, .. },
            ) => {
                import_ids.push(id);
                (Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method: UpdateDocuments,
                    allow_index_creation,
                    import_ids,
                }), false)
            }
            // But we can't batch a settings and a doc op with another doc op
            // this MUST be AFTER the two previous branch
            (false,
                this @ BatchKind::SettingsAndDocumentImport { .. },
                K::DocumentDeletion | K::DocumentImport { .. },
            ) => (Break(this), false),
            (false,
                BatchKind::SettingsAndDocumentImport { mut settings_ids, method, allow_index_creation, import_ids },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                (Continue(BatchKind::SettingsAndDocumentImport {
                    settings_ids,
                    method,
                    allow_index_creation,
                    import_ids,
                }), false)
            }
            (_,
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

    let (mut acc, mut must_create_index) = match BatchKind::new(id, kind) {
        (Continue(acc), create) => (acc, create),
        (Break(acc), create) => return Some((acc, create)),
    };

    for (id, kind) in enqueued {
        // if an index has been created in the previous step we can consider it exists.
        index_exist |= must_create_index;

        match acc.accumulate(id, kind.into(), index_exist) {
            (Continue(a), create) => {
                acc = a;
                must_create_index |= create;
            }
            (Break(acc), create) => return Some((acc, must_create_index | create)),
        };
    }

    Some((acc, must_create_index))
}

#[cfg(test)]
mod tests {
    use crate::debug_snapshot;

    use super::*;
    use uuid::Uuid;

    fn autobatch_from(index_already_exists: bool, input: impl IntoIterator<Item = KindWithContent>) -> Option<(BatchKind, bool)> {
        autobatch(
            input
                .into_iter()
                .enumerate()
                .map(|(id, kind)| (id as TaskId, kind.into()))
                .collect(),
            index_already_exists,
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
            swaps: vec![(String::from("doggo"), String::from("catto"))],
        }
    }

    #[test]
    fn autobatch_simple_operation_together() {
        // we can autobatch one or multiple DocumentAddition together
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp( ReplaceDocuments, true ), doc_imp(ReplaceDocuments, true )]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1, 2] }, true))");
        // we can autobatch one or multiple DocumentUpdate together
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true), doc_imp(UpdateDocuments, true)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0, 1, 2] }, true))");
        // we can autobatch one or multiple DocumentDeletion together
        debug_snapshot!(autobatch_from(true, [doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [doc_del(), doc_del(), doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0, 1, 2] }, true))");
        // we can autobatch one or multiple Settings together
        debug_snapshot!(autobatch_from(true, [settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [settings(true), settings(true), settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0, 1, 2] }, true))");
    }

    #[test]
    fn simple_document_operation_dont_autobatch_with_other() {
        // addition, updates and deletion can't batch together
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp(UpdateDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_del()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), doc_del()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentDeletion { deletion_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), doc_imp(UpdateDocuments, true)]), @"Some((DocumentDeletion { deletion_ids: [0] }, true))");

        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), idx_create()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), idx_create()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), idx_create()]), @"Some((DocumentDeletion { deletion_ids: [0] }, true))");

        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), idx_update()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), idx_update()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), idx_update()]), @"Some((DocumentDeletion { deletion_ids: [0] }, true))");

        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), idx_swap()]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), idx_swap()]), @"Some((DocumentImport { method: UpdateDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), idx_swap()]), @"Some((DocumentDeletion { deletion_ids: [0] }, true))");
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
        debug_snapshot!(autobatch_from(true, [doc_clr(), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentClear { ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_clr(), doc_imp(UpdateDocuments, true)]), @"Some((DocumentClear { ids: [0] }, true))");

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
        debug_snapshot!(autobatch_from(true, [doc_clr(), settings(true)]), @"Some((DocumentClear { ids: [0] }, true))");

        debug_snapshot!(autobatch_from(true, [settings(true), doc_clr(), settings(true)]), @"Some((ClearAndSettings { other: [1], allow_index_creation: true, settings_ids: [0, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), doc_clr()]), @"Some((ClearAndSettings { other: [0, 2], allow_index_creation: true, settings_ids: [1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), doc_clr()]), @"Some((ClearAndSettings { other: [0, 2], allow_index_creation: true, settings_ids: [1] }, true))");
    }

    #[test]
    fn anything_and_index_deletion() {
        // The indexdeletion doesn't batch with anything that happens AFTER
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_imp(ReplaceDocuments, true)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_imp(UpdateDocuments, true)]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_del()]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), doc_clr()]), @"Some((IndexDeletion { ids: [0] }, false))");
        debug_snapshot!(autobatch_from(true, [idx_del(), settings(true)]), @"Some((IndexDeletion { ids: [0] }, false))");

        // The index deletion can accept almost any type of BatchKind and transform it to an idx_del()
        // First, the basic cases
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_del(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");

        // Then the mixed cases
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 2, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(UpdateDocuments, true), settings(true), doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [1, 3, 0, 2] }, true))");
    }

    #[test]
    fn allowed_and_disallowed_index_creation() {
        // doc_imp(indexes canbe)ixed with those disallowed to do so
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), doc_imp(ReplaceDocuments, true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: true, import_ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, false), doc_imp(ReplaceDocuments, false)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0, 1] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, true), settings(true)]), @"Some((SettingsAndDocumentImport { settings_ids: [1], method: ReplaceDocuments, allow_index_creation: true, import_ids: [0] }, true))");
        debug_snapshot!(autobatch_from(true, [doc_imp(ReplaceDocuments, false), settings(true)]), @"Some((DocumentImport { method: ReplaceDocuments, allow_index_creation: false, import_ids: [0] }, true))");
    }
}

use std::ops::ControlFlow;

use crate::{task::Kind, TaskId};

pub enum BatchKind {
    DocumentClear {
        ids: Vec<TaskId>,
    },
    DocumentAddition {
        addition_ids: Vec<TaskId>,
    },
    DocumentUpdate {
        update_ids: Vec<TaskId>,
    },
    DocumentDeletion {
        deletion_ids: Vec<TaskId>,
    },
    ClearAndSettings {
        other: Vec<TaskId>,
        settings_ids: Vec<TaskId>,
    },
    SettingsAndDocumentAddition {
        settings_ids: Vec<TaskId>,
        addition_ids: Vec<TaskId>,
    },
    SettingsAndDocumentUpdate {
        settings_ids: Vec<TaskId>,
        update_ids: Vec<TaskId>,
    },
    Settings {
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
    IndexRename {
        id: TaskId,
    },
    IndexSwap {
        id: TaskId,
    },
}

impl BatchKind {
    /// return true if you must stop right there.
    pub fn new(task_id: TaskId, kind: Kind) -> (Self, bool) {
        match kind {
            Kind::IndexCreation => (BatchKind::IndexCreation { id: task_id }, true),
            Kind::IndexDeletion => (BatchKind::IndexDeletion { ids: vec![task_id] }, true),
            Kind::IndexUpdate => (BatchKind::IndexUpdate { id: task_id }, true),
            Kind::IndexRename => (BatchKind::IndexRename { id: task_id }, true),
            Kind::IndexSwap => (BatchKind::IndexSwap { id: task_id }, true),
            Kind::DocumentClear => (BatchKind::DocumentClear { ids: vec![task_id] }, false),
            Kind::DocumentAddition => (
                BatchKind::DocumentAddition {
                    addition_ids: vec![task_id],
                },
                false,
            ),
            Kind::DocumentUpdate => (
                BatchKind::DocumentUpdate {
                    update_ids: vec![task_id],
                },
                false,
            ),
            Kind::DocumentDeletion => (
                BatchKind::DocumentDeletion {
                    deletion_ids: vec![task_id],
                },
                false,
            ),
            Kind::Settings => (
                BatchKind::Settings {
                    settings_ids: vec![task_id],
                },
                false,
            ),

            Kind::DumpExport | Kind::Snapshot | Kind::CancelTask => unreachable!(),
        }
    }

    /// Return true if you must stop.
    fn accumulate(mut self, id: TaskId, kind: Kind) -> ControlFlow<Self, Self> {
        match (self, kind) {
            // We don't batch any of these operations
            (
                this,
                Kind::IndexCreation | Kind::IndexRename | Kind::IndexUpdate | Kind::IndexSwap,
            ) => ControlFlow::Break(this),
            // The index deletion can batch with everything but must stop after
            (
                BatchKind::DocumentClear { mut ids }
                | BatchKind::DocumentAddition {
                    addition_ids: mut ids,
                }
                | BatchKind::DocumentUpdate {
                    update_ids: mut ids,
                }
                | BatchKind::DocumentDeletion {
                    deletion_ids: mut ids,
                }
                | BatchKind::Settings {
                    settings_ids: mut ids,
                },
                Kind::IndexDeletion,
            ) => {
                ids.push(id);
                ControlFlow::Break(BatchKind::IndexDeletion { ids })
            }
            (
                BatchKind::ClearAndSettings {
                    settings_ids: mut ids,
                    mut other,
                }
                | BatchKind::SettingsAndDocumentAddition {
                    addition_ids: mut ids,
                    settings_ids: mut other,
                }
                | BatchKind::SettingsAndDocumentUpdate {
                    update_ids: mut ids,
                    settings_ids: mut other,
                },
                Kind::IndexDeletion,
            ) => {
                ids.push(id);
                ids.append(&mut other);
                ControlFlow::Break(BatchKind::IndexDeletion { ids })
            }

            (
                BatchKind::DocumentClear { mut ids },
                Kind::DocumentClear | Kind::DocumentDeletion,
            ) => {
                ids.push(id);
                ControlFlow::Continue(BatchKind::DocumentClear { ids })
            }
            (
                this @ BatchKind::DocumentClear { .. },
                Kind::DocumentAddition | Kind::DocumentUpdate | Kind::Settings,
            ) => ControlFlow::Break(this),
            (
                BatchKind::DocumentAddition {
                    addition_ids: mut ids,
                }
                | BatchKind::DocumentUpdate {
                    update_ids: mut ids,
                },
                Kind::DocumentClear,
            ) => {
                ids.push(id);
                ControlFlow::Continue(BatchKind::DocumentClear { ids })
            }

            // we can autobatch the same kind of document additions / updates
            (BatchKind::DocumentAddition { mut addition_ids }, Kind::DocumentAddition) => {
                addition_ids.push(id);
                ControlFlow::Continue(BatchKind::DocumentAddition { addition_ids })
            }
            (BatchKind::DocumentUpdate { mut update_ids }, Kind::DocumentUpdate) => {
                update_ids.push(id);
                ControlFlow::Continue(BatchKind::DocumentUpdate { update_ids })
            }
            // but we can't autobatch documents if it's not the same kind
            // this match branch MUST be AFTER the previous one
            (
                this @ BatchKind::DocumentAddition { .. } | this @ BatchKind::DocumentUpdate { .. },
                Kind::DocumentDeletion | Kind::DocumentAddition | Kind::DocumentUpdate,
            ) => ControlFlow::Break(this),
            (BatchKind::DocumentAddition { addition_ids }, Kind::Settings) => {
                ControlFlow::Continue(BatchKind::SettingsAndDocumentAddition {
                    settings_ids: vec![id],
                    addition_ids,
                })
            }
            (BatchKind::DocumentUpdate { update_ids }, Kind::Settings) => {
                ControlFlow::Continue(BatchKind::SettingsAndDocumentUpdate {
                    settings_ids: vec![id],
                    update_ids,
                })
            }

            (BatchKind::DocumentDeletion { mut deletion_ids }, Kind::DocumentClear) => {
                deletion_ids.push(id);
                ControlFlow::Continue(BatchKind::DocumentClear { ids: deletion_ids })
            }
            (
                this @ BatchKind::DocumentDeletion { .. },
                Kind::DocumentAddition | Kind::DocumentUpdate,
            ) => ControlFlow::Break(this),
            (BatchKind::DocumentDeletion { mut deletion_ids }, Kind::DocumentDeletion) => {
                deletion_ids.push(id);
                ControlFlow::Continue(BatchKind::DocumentDeletion { deletion_ids })
            }
            (this @ BatchKind::DocumentDeletion { .. }, Kind::Settings) => ControlFlow::Break(this),

            (BatchKind::Settings { settings_ids }, Kind::DocumentClear) => {
                ControlFlow::Continue(BatchKind::ClearAndSettings {
                    settings_ids: settings_ids.clone(),
                    other: vec![id],
                })
            }
            (
                this @ BatchKind::Settings { .. },
                Kind::DocumentAddition | Kind::DocumentUpdate | Kind::DocumentDeletion,
            ) => ControlFlow::Break(this),
            (BatchKind::Settings { mut settings_ids }, Kind::Settings) => {
                settings_ids.push(id);
                ControlFlow::Continue(BatchKind::Settings { settings_ids })
            }

            (
                BatchKind::ClearAndSettings {
                    mut other,
                    settings_ids,
                },
                Kind::DocumentClear,
            ) => {
                other.push(id);
                ControlFlow::Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                })
            }
            (
                this @ BatchKind::ClearAndSettings { .. },
                Kind::DocumentAddition | Kind::DocumentUpdate,
            ) => ControlFlow::Break(this),
            (
                BatchKind::ClearAndSettings {
                    mut other,
                    settings_ids,
                },
                Kind::DocumentDeletion,
            ) => {
                other.push(id);
                ControlFlow::Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                })
            }
            (
                BatchKind::ClearAndSettings {
                    mut settings_ids,
                    other,
                },
                Kind::Settings,
            ) => {
                settings_ids.push(id);
                ControlFlow::Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                })
            }
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids: mut other,
                }
                | BatchKind::SettingsAndDocumentUpdate {
                    settings_ids,
                    update_ids: mut other,
                },
                Kind::DocumentClear,
            ) => {
                other.push(id);

                ControlFlow::Continue(BatchKind::ClearAndSettings {
                    settings_ids,
                    other,
                })
            }

            // we can batch the settings with a kind of document operation with the same kind of document operation
            (
                BatchKind::SettingsAndDocumentAddition {
                    mut addition_ids,
                    settings_ids,
                },
                Kind::DocumentAddition,
            ) => {
                addition_ids.push(id);
                ControlFlow::Continue(BatchKind::SettingsAndDocumentAddition {
                    addition_ids,
                    settings_ids,
                })
            }
            (
                BatchKind::SettingsAndDocumentUpdate {
                    mut update_ids,
                    settings_ids,
                },
                Kind::DocumentUpdate,
            ) => {
                update_ids.push(id);
                ControlFlow::Continue(BatchKind::SettingsAndDocumentUpdate {
                    update_ids,
                    settings_ids,
                })
            }
            // But we can't batch a settings and a doc op with another doc op
            // this MUST be AFTER the two previous branch
            (
                this @ BatchKind::SettingsAndDocumentAddition { .. }
                | this @ BatchKind::SettingsAndDocumentUpdate { .. },
                Kind::DocumentDeletion | Kind::DocumentAddition | Kind::DocumentUpdate,
            ) => ControlFlow::Break(this),
            (
                BatchKind::SettingsAndDocumentAddition {
                    mut settings_ids,
                    addition_ids,
                },
                Kind::Settings,
            ) => {
                settings_ids.push(id);
                ControlFlow::Continue(BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids,
                })
            }
            (
                BatchKind::SettingsAndDocumentUpdate {
                    mut settings_ids,
                    update_ids,
                },
                Kind::Settings,
            ) => {
                settings_ids.push(id);
                ControlFlow::Continue(BatchKind::SettingsAndDocumentUpdate {
                    settings_ids,
                    update_ids,
                })
            }
            (_, Kind::CancelTask | Kind::DumpExport | Kind::Snapshot) => unreachable!(),
            (
                BatchKind::IndexCreation { .. }
                | BatchKind::IndexDeletion { .. }
                | BatchKind::IndexUpdate { .. }
                | BatchKind::IndexRename { .. }
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
    let (mut acc, is_finished) = BatchKind::new(id, kind);
    if is_finished {
        return Some(acc);
    }

    for (id, kind) in enqueued {
        acc = match acc.accumulate(id, kind) {
            ControlFlow::Continue(acc) => acc,
            ControlFlow::Break(acc) => return Some(acc),
        };
    }

    None
}

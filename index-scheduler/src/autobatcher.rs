use std::ops::ControlFlow;

use crate::{task::Kind, TaskId};

pub enum BatchKind {
    ClearAll {
        ids: Vec<TaskId>,
    },
    DocumentAddition {
        addition_ids: Vec<TaskId>,
    },
    DocumentDeletion {
        deletion_ids: Vec<TaskId>,
    },
    ClearAllAndSettings {
        other: Vec<TaskId>,
        settings_ids: Vec<TaskId>,
    },
    SettingsAndDocumentAddition {
        settings_ids: Vec<TaskId>,
        addition_ids: Vec<TaskId>,
    },
    Settings {
        settings_ids: Vec<TaskId>,
    },
    DeleteIndex {
        ids: Vec<TaskId>,
    },
    CreateIndex {
        id: TaskId,
    },
    SwapIndex {
        id: TaskId,
    },
    RenameIndex {
        id: TaskId,
    },
}

impl BatchKind {
    /// return true if you must stop right there.
    pub fn new(task_id: TaskId, kind: Kind) -> (Self, bool) {
        match kind {
            Kind::CreateIndex => (BatchKind::CreateIndex { id: task_id }, true),
            Kind::DeleteIndex => (BatchKind::DeleteIndex { ids: vec![task_id] }, true),
            Kind::RenameIndex => (BatchKind::RenameIndex { id: task_id }, true),
            Kind::SwapIndex => (BatchKind::SwapIndex { id: task_id }, true),
            Kind::ClearAllDocuments => (BatchKind::ClearAll { ids: vec![task_id] }, false),
            Kind::DocumentAddition => (
                BatchKind::DocumentAddition {
                    addition_ids: vec![task_id],
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
            (this, Kind::CreateIndex | Kind::RenameIndex | Kind::SwapIndex) => {
                ControlFlow::Break(this)
            }
            // The index deletion can batch with everything but must stop after
            (
                BatchKind::ClearAll { mut ids }
                | BatchKind::DocumentAddition {
                    addition_ids: mut ids,
                }
                | BatchKind::DocumentDeletion {
                    deletion_ids: mut ids,
                }
                | BatchKind::Settings {
                    settings_ids: mut ids,
                },
                Kind::DeleteIndex,
            ) => {
                ids.push(id);
                ControlFlow::Break(BatchKind::DeleteIndex { ids })
            }
            (
                BatchKind::ClearAllAndSettings {
                    settings_ids: mut ids,
                    mut other,
                }
                | BatchKind::SettingsAndDocumentAddition {
                    addition_ids: mut ids,
                    settings_ids: mut other,
                },
                Kind::DeleteIndex,
            ) => {
                ids.push(id);
                ids.append(&mut other);
                ControlFlow::Break(BatchKind::DeleteIndex { ids })
            }

            (BatchKind::ClearAll { mut ids }, Kind::ClearAllDocuments | Kind::DocumentDeletion) => {
                ids.push(id);
                ControlFlow::Continue(BatchKind::ClearAll { ids })
            }
            (this @ BatchKind::ClearAll { .. }, Kind::DocumentAddition | Kind::Settings) => {
                ControlFlow::Break(this)
            }
            (BatchKind::DocumentAddition { mut addition_ids }, Kind::ClearAllDocuments) => {
                addition_ids.push(id);
                ControlFlow::Continue(BatchKind::ClearAll { ids: addition_ids })
            }

            (BatchKind::DocumentAddition { mut addition_ids }, Kind::DocumentAddition) => {
                addition_ids.push(id);
                ControlFlow::Continue(BatchKind::DocumentAddition { addition_ids })
            }
            (this @ BatchKind::DocumentAddition { .. }, Kind::DocumentDeletion) => {
                ControlFlow::Break(this)
            }
            (BatchKind::DocumentAddition { addition_ids }, Kind::Settings) => {
                ControlFlow::Continue(BatchKind::SettingsAndDocumentAddition {
                    settings_ids: vec![id],
                    addition_ids,
                })
            }

            (BatchKind::DocumentDeletion { mut deletion_ids }, Kind::ClearAllDocuments) => {
                deletion_ids.push(id);
                ControlFlow::Continue(BatchKind::ClearAll { ids: deletion_ids })
            }
            (this @ BatchKind::DocumentDeletion { .. }, Kind::DocumentAddition) => {
                ControlFlow::Break(this)
            }
            (BatchKind::DocumentDeletion { mut deletion_ids }, Kind::DocumentDeletion) => {
                deletion_ids.push(id);
                ControlFlow::Continue(BatchKind::DocumentDeletion { deletion_ids })
            }
            (this @ BatchKind::DocumentDeletion { .. }, Kind::Settings) => ControlFlow::Break(this),

            (BatchKind::Settings { settings_ids }, Kind::ClearAllDocuments) => {
                ControlFlow::Continue(BatchKind::ClearAllAndSettings {
                    settings_ids: settings_ids.clone(),
                    other: vec![id],
                })
            }
            (this @ BatchKind::Settings { .. }, Kind::DocumentAddition) => ControlFlow::Break(this),
            (this @ BatchKind::Settings { .. }, Kind::DocumentDeletion) => ControlFlow::Break(this),
            (BatchKind::Settings { mut settings_ids }, Kind::Settings) => {
                settings_ids.push(id);
                ControlFlow::Continue(BatchKind::Settings { settings_ids })
            }

            (
                BatchKind::ClearAllAndSettings {
                    mut other,
                    settings_ids,
                },
                Kind::ClearAllDocuments,
            ) => {
                other.push(id);
                ControlFlow::Continue(BatchKind::ClearAllAndSettings {
                    other,
                    settings_ids,
                })
            }
            (this @ BatchKind::ClearAllAndSettings { .. }, Kind::DocumentAddition) => {
                ControlFlow::Break(this)
            }
            (
                BatchKind::ClearAllAndSettings {
                    mut other,
                    settings_ids,
                },
                Kind::DocumentDeletion,
            ) => {
                other.push(id);
                ControlFlow::Continue(BatchKind::ClearAllAndSettings {
                    other,
                    settings_ids,
                })
            }
            (
                BatchKind::ClearAllAndSettings {
                    mut settings_ids,
                    other,
                },
                Kind::Settings,
            ) => {
                settings_ids.push(id);
                ControlFlow::Continue(BatchKind::ClearAllAndSettings {
                    other,
                    settings_ids,
                })
            }
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    mut addition_ids,
                },
                Kind::ClearAllDocuments,
            ) => {
                addition_ids.push(id);

                ControlFlow::Continue(BatchKind::ClearAllAndSettings {
                    settings_ids,
                    other: addition_ids,
                })
            }
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
            (this @ BatchKind::SettingsAndDocumentAddition { .. }, Kind::DocumentDeletion) => {
                ControlFlow::Break(this)
            }
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
            (_, Kind::CancelTask | Kind::DumpExport | Kind::Snapshot) => unreachable!(),
            (
                BatchKind::CreateIndex { .. }
                | BatchKind::DeleteIndex { .. }
                | BatchKind::SwapIndex { .. }
                | BatchKind::RenameIndex { .. },
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
